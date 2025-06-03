import requests
import psycopg2
from psycopg2.extras import RealDictCursor
import json
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import schedule
import os
from dataclasses import dataclass
from urllib.parse import urlparse
import hashlib
import gzip
import io
import pandas as pd
from bs4 import BeautifulSoup

# Configuration
@dataclass
class Config:
    db_host: str = os.getenv('DB_HOST', 'localhost')
    db_port: int = int(os.getenv('DB_PORT', '5432'))
    db_name: str = os.getenv('DB_NAME', 'kickstarter')
    db_user: str = os.getenv('DB_USER', 'postgres')
    db_password: str = os.getenv('DB_PASSWORD', 'password')
    base_url: str = "https://webrobots.io/kickstarter-datasets"
    request_timeout: int = 30
    cache_ttl_minutes: int = 60
    batch_size: int = 1000
    max_retries: int = 3
    cleanup_days: int = int(os.getenv('CLEANUP_DAYS', '180'))  # Configurable retention period
    retry_delay_base: int = 2
    rate_limit_delay: float = 1.0
    pipeline_schedule_days: int = int(os.getenv('PIPELINE_SCHEDULE_DAYS', '7'))

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('kickstarter_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class KickstarterPipeline:
    def __init__(self, config: Config = None):
        self.config = config or Config()
        self.cache = {}
        self.cache_ttl = {}
        self.init_database()
    
    def get_db_connection(self):
        """Create database connection with proper error handling"""
        try:
            conn = psycopg2.connect(
                host=self.config.db_host,
                port=self.config.db_port,
                database=self.config.db_name,
                user=self.config.db_user,
                password=self.config.db_password,
                cursor_factory=RealDictCursor
            )
            return conn
        except psycopg2.Error as e:
            logger.error(f"Database connection failed: {e}")
            raise
    
    def init_database(self):
        """Initialize PostgreSQL database with proper schema"""
        try:
            with self.get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS projects (
                            id BIGINT PRIMARY KEY,
                            name TEXT NOT NULL,
                            blurb TEXT,
                            goal DECIMAL(12,2),
                            pledged DECIMAL(12,2),
                            state VARCHAR(50),
                            country VARCHAR(10),
                            currency VARCHAR(10),
                            deadline TIMESTAMP,
                            created_at TIMESTAMP,
                            launched_at TIMESTAMP,
                            backers_count INTEGER DEFAULT 0,
                            category_id INTEGER,
                            category_name VARCHAR(100),
                            category_slug VARCHAR(100),
                            creator_id BIGINT,
                            creator_name TEXT,
                            staff_pick BOOLEAN DEFAULT FALSE,
                            spotlight BOOLEAN DEFAULT FALSE,
                            success_rate DECIMAL(5,2),
                            data_hash VARCHAR(64),
                            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            created_at_local TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    """)
                    indexes = [
                        "CREATE INDEX IF NOT EXISTS idx_projects_state ON projects(state)",
                        "CREATE INDEX IF NOT EXISTS idx_projects_category ON projects(category_name)",
                        "CREATE INDEX IF NOT EXISTS idx_projects_created ON projects(created_at)",
                        "CREATE INDEX IF NOT EXISTS idx_projects_updated ON projects(updated_at)",
                        "CREATE INDEX IF NOT EXISTS idx_projects_country ON projects(country)",
                        "CREATE INDEX IF NOT EXISTS idx_projects_hash ON projects(data_hash)"
                    ]
                    for index_sql in indexes:
                        cur.execute(index_sql)
                    
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS analytics_summary (
                            id SERIAL PRIMARY KEY,
                            summary_date DATE UNIQUE,
                            total_projects INTEGER,
                            successful_projects INTEGER,
                            success_rate DECIMAL(5,2),
                            total_pledged DECIMAL(15,2),
                            avg_goal DECIMAL(12,2),
                            avg_pledged DECIMAL(12,2),
                            top_categories JSONB,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    """)
                    
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS processed_datasets (
                            dataset_url VARCHAR(255) PRIMARY KEY,
                            dataset_date DATE,
                            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    """)
                    cur.execute("CREATE INDEX IF NOT EXISTS idx_processed_datasets_date ON processed_datasets(dataset_date)")
                    
                    conn.commit()
                    logger.info("Database initialized successfully")
                    
        except Exception as e:
            logger.error(f"Database initialization failed: {e}")
            raise
    
    def get_cached_data(self, key: str) -> Optional[Dict]:
        """Simple cache with TTL check"""
        if key in self.cache:
            if datetime.now() < self.cache_ttl.get(key, datetime.min):
                return self.cache[key]
            else:
                self.cache.pop(key, None)
                self.cache_ttl.pop(key, None)
        return None
    
    def set_cache(self, key: str, data: Dict, ttl_minutes: int = None):
        """Set cache with TTL"""
        ttl_minutes = ttl_minutes or self.config.cache_ttl_minutes
        self.cache[key] = data
        self.cache_ttl[key] = datetime.now() + timedelta(minutes=ttl_minutes)
    
    def fetch_dataset_info(self) -> Optional[List[Dict]]:
        """Fetch available datasets from webrobots.io by scraping the datasets page"""
        cache_key = "dataset_info"
        cached_data = self.get_cached_data(cache_key)
        if cached_data:
            logger.info("Using cached dataset info")
            return cached_data
        
        try:
            logger.info(f"Scraping dataset info from {self.config.base_url}")
            response = requests.get(
                self.config.base_url,
                timeout=self.config.request_timeout,
                headers={'User-Agent': 'KickstarterPipeline/1.0'}
            )
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            datasets = []
            
            for link in soup.find_all('a', href=True):
                href = link['href']
                if href.endswith('.json') or href.endswith('.csv'):
                    if not href.startswith('http'):
                        href = f"{self.config.base_url.rstrip('/')}/{href.lstrip('/')}"
                    dataset_date = None
                    if link.text.strip():
                        try:
                            dataset_date = datetime.strptime(link.text.strip(), '%Y-%m-%d').date()
                        except ValueError:
                            import re
                            date_match = re.search(r'(\d{4}-\d{2}-\d{2})', href)
                            if date_match:
                                dataset_date = datetime.strptime(date_match.group(1), '%Y-%m-%d').date()
                    datasets.append({
                        'url': href,
                        'type': 'json' if href.endswith('.json') else 'csv',
                        'date': dataset_date
                    })
            
            if not datasets:
                logger.warning("No dataset links found on the page")
                return None
            
            logger.info(f"Found {len(datasets)} datasets")
            self.set_cache(cache_key, datasets)
            return datasets
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch dataset info: {e}, status code: {getattr(e.response, 'status_code', 'N/A')}")
            return None
        except Exception as e:
            logger.error(f"Error parsing dataset info: {e}")
            return None
    
    def check_new_datasets(self, datasets: List[Dict]) -> List[Dict]:
        """Check which datasets are new or unprocessed"""
        try:
            with self.get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT dataset_url FROM processed_datasets")
                    processed_urls = {row['dataset_url'] for row in cur.fetchall()}
                    new_datasets = [ds for ds in datasets if ds['url'] not in processed_urls]
                    logger.info(f"Found {len(new_datasets)} new datasets out of {len(datasets)}")
                    return new_datasets
                    
        except Exception as e:
            logger.error(f"Failed to check new datasets: {e}")
            return datasets
    
    def mark_dataset_processed(self, dataset: Dict):
        """Mark a dataset as processed in the database"""
        try:
            with self.get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO processed_datasets (dataset_url, dataset_date, processed_at)
                        VALUES (%s, %s, CURRENT_TIMESTAMP)
                        ON CONFLICT (dataset_url) DO UPDATE SET
                            processed_at = CURRENT_TIMESTAMP
                    """, (dataset['url'], dataset['date']))
                    conn.commit()
                    logger.info(f"Marked dataset {dataset['url']} as processed")
        except Exception as e:
            logger.error(f"Failed to mark dataset {dataset['url']} as processed: {e}")
    
    def fetch_project_data(self, url: str) -> Optional[List[Dict]]:
        """Fetch project data with retry logic, handling JSON streaming and compression"""
        cache_key = f"projects_{hashlib.md5(url.encode()).hexdigest()}"
        cached_data = self.get_cached_data(cache_key)
        if cached_data:
            logger.info(f"Using cached data for {url}")
            return cached_data
        
        for attempt in range(self.config.max_retries):
            try:
                logger.info(f"Fetching data from {url} (attempt {attempt + 1})")
                response = requests.get(
                    url,
                    timeout=self.config.request_timeout,
                    headers={'User-Agent': 'KickstarterPipeline/1.0'}
                )
                response.raise_for_status()
                
                is_compressed = response.headers.get('content-encoding', '') == 'gzip'
                data = gzip.decompress(response.content) if is_compressed else response.content
                
                content_type = response.headers.get('content-type', '')
                if 'json' in content_type or url.endswith('.json'):
                    projects = []
                    for line in io.StringIO(data.decode('utf-8')).readlines():
                        try:
                            project = json.loads(line.strip())
                            if project:
                                projects.append(project)
                        except json.JSONDecodeError as e:
                            logger.warning(f"Skipping invalid JSON line: {e}")
                            continue
                else:
                    df = pd.read_csv(io.StringIO(data.decode('utf-8')))
                    projects = df.to_dict('records')
                
                seen_ids = set()
                unique_projects = []
                for project in projects:
                    project_id = project.get('id') or project.get('project_id')
                    if project_id and project_id not in seen_ids:
                        seen_ids.add(project_id)
                        unique_projects.append(project)
                
                self.set_cache(cache_key, unique_projects)
                logger.info(f"Successfully fetched {len(unique_projects)} unique projects from {url}")
                return unique_projects
                
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt + 1} failed for {url}: {e}, status code: {getattr(e.response, 'status_code', 'N/A')}")
                if attempt < self.config.max_retries - 1:
                    time.sleep(self.config.retry_delay_base ** attempt)
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
                if attempt < self.config.max_retries - 1:
                    time.sleep(self.config.retry_delay_base ** attempt)
        
        logger.error(f"All attempts failed for {url}")
        return None
    
    def clean_project_data(self, raw_data: Dict) -> Optional[Dict]:
        """Clean and validate project data with comprehensive field mapping"""
        try:
            project_id = raw_data.get('id') or raw_data.get('project_id')
            if not project_id:
                return None
            
            def parse_date(date_val):
                if not date_val:
                    return None
                try:
                    if isinstance(date_val, (int, float)):
                        return datetime.fromtimestamp(date_val)
                    return datetime.fromisoformat(str(date_val).replace('Z', '+00:00'))
                except:
                    return None
            
            def clean_numeric(val, default=0):
                try:
                    return float(val) if val is not None else default
                except (ValueError, TypeError):
                    return default
            
            data_hash = hashlib.sha256(
                json.dumps(raw_data, sort_keys=True, default=str).encode()
            ).hexdigest()
            
            cleaned = {
                'id': int(project_id),
                'name': str(raw_data.get('name', '')).strip(),
                'blurb': str(raw_data.get('blurb', '')).strip(),
                'goal': clean_numeric(raw_data.get('goal')),
                'pledged': clean_numeric(raw_data.get('pledged')),
                'state': str(raw_data.get('state', '')).lower().strip(),
                'country': str(raw_data.get('country', '')).upper().strip(),
                'currency': str(raw_data.get('currency', '')).upper().strip(),
                'deadline': parse_date(raw_data.get('deadline')),
                'created_at': parse_date(raw_data.get('created_at')),
                'launched_at': parse_date(raw_data.get('launched_at')),
                'backers_count': int(clean_numeric(raw_data.get('backers_count', 0))),
                'category_id': raw_data.get('category_id'),
                'category_name': str(raw_data.get('category_name', '')).strip(),
                'category_slug': str(raw_data.get('category_slug', '')).strip(),
                'creator_id': raw_data.get('creator_id'),
                'creator_name': str(raw_data.get('creator_name', '')).strip(),
                'staff_pick': bool(raw_data.get('staff_pick', False)),
                'spotlight': bool(raw_data.get('spotlight', False)),
                'data_hash': data_hash
            }
            
            if cleaned['goal'] > 0:
                cleaned['success_rate'] = round((cleaned['pledged'] / cleaned['goal']) * 100, 2)
            else:
                cleaned['success_rate'] = 0
            
            if cleaned['goal'] < 0 or cleaned['pledged'] < 0 or cleaned['backers_count'] < 0:
                logger.warning(f"Invalid numeric data for project {cleaned['id']}")
                return None
            
            valid_countries = {'US', 'GB', 'CA', 'AU', 'DE', 'FR', 'IT', 'ES', 'NL', 'SE', 'DK', 'NO', 'CH', 'BE', 'AT', 'IE', 'LU', 'HK', 'SG', 'MX', 'NZ'}
            valid_currencies = {'USD', 'GBP', 'CAD', 'AUD', 'EUR', 'SEK', 'DKK', 'NOK', 'CHF', 'HKD', 'SGD', 'MXN', 'NZD'}
            if cleaned['country'] and cleaned['country'] not in valid_countries:
                logger.warning(f"Invalid country code {cleaned['country']} for project {cleaned['id']}")
                cleaned['country'] = None
            if cleaned['currency'] and cleaned['currency'] not in valid_currencies:
                logger.warning(f"Invalid currency code {cleaned['currency']} for project {cleaned['id']}")
                cleaned['currency'] = None
            
            return cleaned
            
        except Exception as e:
            logger.error(f"Data cleaning failed for project {project_id}: {e}")
            return None
    
    def save_projects_batch(self, projects: List[Dict]):
        """Save projects to database in batches with conflict handling"""
        if not projects:
            return 0
        
        try:
            with self.get_db_connection() as conn:
                with conn.cursor() as cur:
                    upsert_sql = """
                        INSERT INTO projects (
                            id, name, blurb, goal, pledged, state, country, currency,
                            deadline, created_at, launched_at, backers_count, category_id,
                            category_name, category_slug, creator_id, creator_name,
                            staff_pick, spotlight, success_rate, data_hash, updated_at
                        ) VALUES (
                            %(id)s, %(name)s, %(blurb)s, %(goal)s, %(pledged)s, %(state)s,
                            %(country)s, %(currency)s, %(deadline)s, %(created_at)s,
                            %(launched_at)s, %(backers_count)s, %(category_id)s,
                            %(category_name)s, %(category_slug)s, %(creator_id)s,
                            %(creator_name)s, %(staff_pick)s, %(spotlight)s,
                            %(success_rate)s, %(data_hash)s, CURRENT_TIMESTAMP
                        )
                        ON CONFLICT (id) DO UPDATE SET
                            name = EXCLUDED.name,
                            blurb = EXCLUDED.blurb,
                            goal = EXCLUDED.goal,
                            pledged = EXCLUDED.pledged,
                            state = EXCLUDED.state,
                            backers_count = EXCLUDED.backers_count,
                            success_rate = EXCLUDED.success_rate,
                            data_hash = EXCLUDED.data_hash,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE projects.data_hash != EXCLUDED.data_hash
                    """
                    
                    cur.executemany(upsert_sql, projects)
                    affected_rows = cur.rowcount
                    conn.commit()
                    
                    logger.info(f"Processed {len(projects)} projects, {affected_rows} rows affected")
                    return affected_rows
                    
        except Exception as e:
            logger.error(f"Failed to save projects batch: {e}")
            raise
    
    def generate_analytics(self) -> Dict:
        """Generate comprehensive analytics"""
        try:
            with self.get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT 
                            COUNT(*) as total_projects,
                            COUNT(CASE WHEN pledged >= goal AND goal > 0 THEN 1 END) as successful_projects,
                            ROUND(AVG(goal), 2) as avg_goal,
                            ROUND(AVG(pledged), 2) as avg_pledged,
                            ROUND(SUM(pledged), 2) as total_pledged,
                            COUNT(DISTINCT country) as unique_countries,
                            COUNT(DISTINCT category_name) as unique_categories
                        FROM projects 
                        WHERE goal > 0
                    """)
                    
                    stats = cur.fetchone()
                    
                    success_rate = 0
                    if stats['total_projects'] > 0:
                        success_rate = round((stats['successful_projects'] / stats['total_projects']) * 100, 2)
                    
                    cur.execute("""
                        SELECT category_name, COUNT(*) as project_count,
                               ROUND(AVG(CASE WHEN pledged >= goal THEN 100.0 ELSE 0 END), 2) as success_rate
                        FROM projects 
                        WHERE category_name != '' AND goal > 0
                        GROUP BY category_name 
                        ORDER BY project_count DESC 
                        LIMIT 10
                    """)
                    
                    top_categories = {row['category_name']: {
                        'count': row['project_count'],
                        'success_rate': row['success_rate']
                    } for row in cur.fetchall()}
                    
                    cur.execute("""
                        SELECT country, COUNT(*) as project_count,
                               ROUND(SUM(pledged), 2) as total_pledged
                        FROM projects 
                        WHERE country != ''
                        GROUP BY country 
                        ORDER BY project_count DESC 
                        LIMIT 10
                    """)
                    
                    country_stats = {row['country']: {
                        'count': row['project_count'],
                        'total_pledged': float(row['total_pledged'])
                    } for row in cur.fetchall()}
                    
                    analytics = {
                        'total_projects': stats['total_projects'],
                        'successful_projects': stats['successful_projects'],
                        'success_rate': success_rate,
                        'avg_goal': float(stats['avg_goal'] or 0),
                        'avg_pledged': float(stats['avg_pledged'] or 0),
                        'total_pledged': float(stats['total_pledged'] or 0),
                        'unique_countries': stats['unique_countries'],
                        'unique_categories': stats['unique_categories'],
                        'top_categories': top_categories,
                        'country_stats': country_stats,
                        'generated_at': datetime.now().isoformat()
                    }
                    
                    self.set_cache('analytics', analytics, ttl_minutes=30)
                    self.save_daily_summary(analytics)
                    
                    return analytics
                    
        except Exception as e:
            logger.error(f"Analytics generation failed: {e}")
            return {}
    
    def save_daily_summary(self, analytics: Dict):
        """Save daily analytics summary"""
        try:
            with self.get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO analytics_summary (
                            summary_date, total_projects, successful_projects, success_rate,
                            total_pledged, avg_goal, avg_pledged, top_categories
                        ) VALUES (
                            CURRENT_DATE, %(total_projects)s, %(successful_projects)s,
                            %(success_rate)s, %(total_pledged)s, %(avg_goal)s,
                            %(avg_pledged)s, %(top_categories)s
                        )
                        ON CONFLICT (summary_date) DO UPDATE SET
                            total_projects = EXCLUDED.total_projects,
                            successful_projects = EXCLUDED.successful_projects,
                            success_rate = EXCLUDED.success_rate,
                            total_pledged = EXCLUDED.total_pledged,
                            avg_goal = EXCLUDED.avg_goal,
                            avg_pledged = EXCLUDED.avg_pledged,
                            top_categories = EXCLUDED.top_categories
                    """, {
                        **analytics,
                        'top_categories': json.dumps(analytics.get('top_categories', {}))
                    })
                    conn.commit()
        except Exception as e:
            logger.warning(f"Failed to save daily summary: {e}")
    
    def cleanup_old_data(self, cleanup_days: int = None, confirm: bool = False):
        """Manually remove old data from projects and processed_datasets tables.
        
        Args:
            cleanup_days (int, optional): Number of days to retain data. Defaults to config.cleanup_days.
            confirm (bool): If True, requires user confirmation before deletion.
        """
        cleanup_days = cleanup_days or self.config.cleanup_days
        cutoff_date = datetime.now() - timedelta(days=cleanup_days)
        
        if confirm:
            response = input(
                f"WARNING: This will delete records from projects and processed_datasets "
                f"older than {cleanup_days} days (before {cutoff_date}). "
                f"Type 'yes' to proceed: "
            )
            if response.lower() != 'yes':
                logger.info("Manual cleanup cancelled by user")
                return
        
        try:
            with self.get_db_connection() as conn:
                with conn.cursor() as cur:
                    # Delete old projects
                    cur.execute("""
                        DELETE FROM projects 
                        WHERE updated_at < %s
                    """, (cutoff_date,))
                    deleted_count = cur.rowcount
                    if deleted_count > 0:
                        logger.info(f"Manually cleaned up {deleted_count} old project records")
                    
                    # Delete old processed_datasets entries
                    cur.execute("""
                        DELETE FROM processed_datasets 
                        WHERE processed_at < %s
                    """, (cutoff_date,))
                    deleted_datasets = cur.rowcount
                    if deleted_datasets > 0:
                        logger.info(f"Manually cleaned up {deleted_datasets} old dataset records")
                    
                    conn.commit()
                    logger.info(f"Manual cleanup completed for records older than {cleanup_days} days")
                    
        except Exception as e:
            logger.error(f"Manual cleanup failed: {e}")
            raise
    
    def run_pipeline(self):
        """Main pipeline execution"""
        logger.info("Starting Kickstarter data pipeline")
        start_time = time.time()
        
        try:
            datasets = self.fetch_dataset_info()
            if not datasets:
                logger.error("No datasets available")
                return
            
            new_datasets = self.check_new_datasets(datasets)
            if not new_datasets:
                logger.info("No new datasets to process")
                return
            
            total_processed = 0
            for dataset in new_datasets:
                logger.info(f"Processing dataset: {dataset['url']}")
                
                raw_data = self.fetch_project_data(dataset['url'])
                if not raw_data:
                    logger.warning(f"No data from {dataset['url']}")
                    continue
                
                cleaned_batch = []
                for i, project_data in enumerate(raw_data):
                    cleaned = self.clean_project_data(project_data)
                    if cleaned:
                        cleaned_batch.append(cleaned)
                    
                    if len(cleaned_batch) >= self.config.batch_size:
                        self.save_projects_batch(cleaned_batch)
                        total_processed += len(cleaned_batch)
                        cleaned_batch = []
                    
                    time.sleep(self.config.rate_limit_delay)
                
                if cleaned_batch:
                    self.save_projects_batch(cleaned_batch)
                    total_processed += len(cleaned_batch)
                
                self.mark_dataset_processed(dataset)
            
            analytics = self.generate_analytics()
            
            processing_time = time.time() - start_time
            logger.info(f"Pipeline completed in {processing_time:.2f}s")
            logger.info(f"Total processed: {total_processed} projects")
            logger.info(f"Analytics: Success rate: {analytics.get('success_rate', 0)}%")
            
            return analytics
            
        except Exception as e:
            logger.error(f"Pipeline execution failed: {e}")
            raise

def main():
    """Main function with proper configuration"""
    try:
        config = Config()
        pipeline = KickstarterPipeline(config)
        
        logger.info("Running initial pipeline execution")
        pipeline.run_pipeline()
        
        schedule.every(config.pipeline_schedule_days).days.do(pipeline.run_pipeline)
        logger.info(f"Pipeline scheduled - running every {config.pipeline_schedule_days} days")
        
        while True:
            schedule.run_pending()
            time.sleep(300)
            
    except KeyboardInterrupt:
        logger.info("Pipeline stopped by user")
        schedule.clear()
    except Exception as e:
        logger.error(f"Pipeline failed to start: {e}")
        raise

if __name__ == "__main__":
    main()
