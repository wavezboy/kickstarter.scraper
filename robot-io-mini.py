import requests
import sqlite3
import json
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import schedule

# Simple logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SimpleKickstarterPipeline:
    def __init__(self, db_path: str = "kickstarter.db"):
        self.db_path = db_path
        self.cache = {}  # Simple in-memory cache instead of Redis
        self.cache_ttl = {}  # Track cache expiry times
        self.init_database()
    
    def init_database(self):
        """Initialize SQLite database"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS projects (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    category_name TEXT,
                    goal REAL,
                    pledged REAL,
                    state TEXT,
                    backers_count INTEGER,
                    created_at TEXT,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.commit()
    
    def get_cached_data(self, key: str) -> Optional[Dict]:
        """Simple cache with TTL check"""
        if key in self.cache:
            if datetime.now() < self.cache_ttl.get(key, datetime.min):
                return self.cache[key]
            else:
                # Cache expired, remove it
                self.cache.pop(key, None)
                self.cache_ttl.pop(key, None)
        return None
    
    def set_cache(self, key: str, data: Dict, ttl_minutes: int = 60):
        """Set cache with TTL"""
        self.cache[key] = data
        self.cache_ttl[key] = datetime.now() + timedelta(minutes=ttl_minutes)
    
    def fetch_project_data(self, url: str) -> Optional[List[Dict]]:
        """Fetch project data with simple caching"""
        cache_key = f"projects_{url}"
        
        # Check cache first
        cached_data = self.get_cached_data(cache_key)
        if cached_data:
            logger.info("Using cached data")
            return cached_data
        
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            # Cache the result
            self.set_cache(cache_key, data)
            logger.info(f"Fetched {len(data) if isinstance(data, list) else 1} projects")
            return data
            
        except Exception as e:
            logger.error(f"Failed to fetch data: {e}")
            return None
    
    def clean_project_data(self, raw_data: Dict) -> Optional[Dict]:
        """Clean and validate project data"""
        try:
            # Basic validation and cleaning
            if not raw_data.get('id') or not raw_data.get('name'):
                return None
            
            cleaned = {
                'id': int(raw_data['id']),
                'name': raw_data['name'].strip(),
                'category_name': raw_data.get('category_name', '').strip(),
                'goal': float(raw_data.get('goal', 0)),
                'pledged': float(raw_data.get('pledged', 0)),
                'state': raw_data.get('state', '').lower(),
                'backers_count': int(raw_data.get('backers_count', 0)),
                'created_at': raw_data.get('created_at'),
                'updated_at': datetime.now().isoformat()
            }
            
            # Basic validation
            if cleaned['goal'] < 0 or cleaned['pledged'] < 0 or cleaned['backers_count'] < 0:
                logger.warning(f"Invalid data for project {cleaned['id']}")
                return None
                
            return cleaned
            
        except (ValueError, TypeError) as e:
            logger.error(f"Data cleaning failed: {e}")
            return None
    
    def save_projects(self, projects: List[Dict]):
        """Save projects to database"""
        if not projects:
            return
        
        with sqlite3.connect(self.db_path) as conn:
            for project in projects:
                conn.execute("""
                    INSERT OR REPLACE INTO projects 
                    (id, name, category_name, goal, pledged, state, backers_count, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    project['id'], project['name'], project['category_name'],
                    project['goal'], project['pledged'], project['state'],
                    project['backers_count'], project['created_at'], project['updated_at']
                ))
            conn.commit()
        
        logger.info(f"Saved {len(projects)} projects to database")
    
    def analyze_data(self) -> Dict:
        """Generate basic analytics"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            
            # Top categories
            top_categories = dict(conn.execute("""
                SELECT category_name, COUNT(*) as count
                FROM projects 
                WHERE category_name != ''
                GROUP BY category_name 
                ORDER BY count DESC 
                LIMIT 5
            """).fetchall())
            
            # Success rate
            success_stats = conn.execute("""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN pledged >= goal THEN 1 ELSE 0 END) as successful,
                    AVG(goal) as avg_goal,
                    AVG(pledged) as avg_pledged
                FROM projects
                WHERE goal > 0
            """).fetchone()
            
            success_rate = success_stats['successful'] / success_stats['total'] if success_stats['total'] > 0 else 0
            
            insights = {
                'top_categories': top_categories,
                'success_rate': round(success_rate * 100, 2),
                'avg_goal': round(success_stats['avg_goal'], 2),
                'avg_pledged': round(success_stats['avg_pledged'], 2),
                'total_projects': success_stats['total']
            }
            
            # Cache insights
            self.set_cache('insights', insights, ttl_minutes=30)
            
            return insights
    
    def cleanup_old_data(self):
        """Remove old data to keep database size manageable"""
        with sqlite3.connect(self.db_path) as conn:
            # Keep only last 6 months of data
            cutoff_date = (datetime.now() - timedelta(days=180)).isoformat()
            result = conn.execute("""
                DELETE FROM projects 
                WHERE updated_at < ?
            """, (cutoff_date,))
            
            if result.rowcount > 0:
                logger.info(f"Cleaned up {result.rowcount} old records")
            conn.commit()
    
    def run_pipeline(self, api_url: str):
        """Main pipeline execution"""
        logger.info("Starting pipeline run")
        start_time = time.time()
        
        try:
            # Fetch data
            raw_data = self.fetch_project_data(api_url)
            if not raw_data:
                logger.error("No data fetched")
                return
            
            # Ensure raw_data is a list
            if not isinstance(raw_data, list):
                raw_data = [raw_data]
            
            # Clean and process data
            cleaned_projects = []
            for project_data in raw_data:
                cleaned = self.clean_project_data(project_data)
                if cleaned:
                    cleaned_projects.append(cleaned)
            
            # Save to database
            self.save_projects(cleaned_projects)
            
            # Generate insights
            insights = self.analyze_data()
            
            # Log results
            processing_time = time.time() - start_time
            logger.info(f"Pipeline completed in {processing_time:.2f}s")
            logger.info(f"Processed {len(cleaned_projects)} projects")
            logger.info(f"Insights: {insights}")
            
            # Cleanup old data periodically
            if datetime.now().hour == 2:  # Run cleanup at 2 AM
                self.cleanup_old_data()
                
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")

def main():
    """Main function to run the pipeline"""
    # Initialize pipeline
    pipeline = SimpleKickstarterPipeline()
    
    # Example API URL (replace with actual Kickstarter API endpoint)
    api_url = "https://api.kickstarter.com/v1/projects"
    
    # Run once immediately
    pipeline.run_pipeline(api_url)
    
    # Schedule to run every hour
    schedule.every(1).hour.do(lambda: pipeline.run_pipeline(api_url))
    
    logger.info("Pipeline scheduled to run every hour")
    
    # Keep the script running
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    main()