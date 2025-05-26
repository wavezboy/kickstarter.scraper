import asyncio
import aiohttp
import requests
from bs4 import BeautifulSoup
import json
import psycopg2
from psycopg2 import sql, pool
import schedule
import time
import logging
import os
import argparse
from datetime import datetime
from tqdm import tqdm
import hashlib
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dataclasses import dataclass, asdict
from typing import Optional, List, Dict, Any, Tuple
import prometheus_client
from prometheus_client import Counter, Histogram, Gauge
import structlog
from tenacity import retry, stop_after_attempt, wait_exponential
import redis
from jsonschema import validate
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import backoff
from datetime import timedelta

# Metrics
SCRAPE_COUNTER = Counter('kickstarter_scrapes_total', 'Total number of scraping operations')
PROCESSING_TIME = Histogram('kickstarter_processing_seconds', 'Time spent processing data')
ACTIVE_CONNECTIONS = Gauge('kickstarter_db_connections_active', 'Number of active database connections')
DATA_QUALITY_SCORE = Gauge('kickstarter_data_quality_score', 'Data quality score')
PROCESSING_ERRORS = Counter('kickstarter_processing_errors', 'Number of processing errors')

# Schema validation
PROJECT_SCHEMA = {
    "type": "object",
    "properties": {
        "id": {"type": "integer"},
        "name": {"type": "string"},
        "category_name": {"type": "string"},
        "goal": {"type": "number"},
        "pledged": {"type": "number"},
        "state": {"type": "string"},
        "backers_count": {"type": "integer"},
        "created_at": {"type": "string", "format": "date-time"},
        "updated_at": {"type": "string", "format": "date-time"},
    },
    "required": ["id", "name"]
}

@dataclass
class Project:
    """Project data model with validation"""
    id: int
    name: str
    category_name: Optional[str] = None
    goal: Optional[float] = None
    pledged: Optional[float] = None
    state: Optional[str] = None
    backers_count: Optional[int] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    data_version: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {k: v for k, v in asdict(self).items() if v is not None}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Project':
        return cls(**{k: v for k, v in data.items() if k in cls.__annotations__})

    def validate(self) -> Tuple[bool, List[str]]:
        """Validate project data quality"""
        errors = []
        
        if not self.name or len(self.name.strip()) < 3:
            errors.append("Invalid name")
            
        if self.goal is not None and self.goal < 0:
            errors.append("Negative goal amount")
            
        if self.pledged is not None and self.pledged < 0:
            errors.append("Negative pledged amount")
            
        if self.backers_count is not None and self.backers_count < 0:
            errors.append("Negative backers count")
            
        return len(errors) == 0, errors

class DataPipeline:
    def __init__(self):
        # Initialize structured logging
        structlog.configure(
            processors=[
                structlog.processors.TimeStamper(fmt="iso"),
                structlog.processors.JSONRenderer()
            ]
        )
        self.logger = structlog.get_logger()
        
        # Initialize Redis cache with connection pool
        self.redis_pool = redis.ConnectionPool(
            host=os.getenv('REDIS_HOST', 'localhost'),
            port=int(os.getenv('REDIS_PORT', 6379)),
            max_connections=20
        )
        self.redis_client = redis.Redis(connection_pool=self.redis_pool)
        
        # Initialize database pool
        self.db_pool = self.init_db_pool()
        
        # Initialize metrics server
        prometheus_client.start_http_server(8000)
        
        # Initialize thread pool for parallel processing
        self.thread_pool = ThreadPoolExecutor(max_workers=10)

    def init_db_pool(self):
        """Initialize database connection pool with monitoring"""
        try:
            pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=5,
                maxconn=20,
                **self.get_db_config()
            )
            ACTIVE_CONNECTIONS.set(0)
            return pool
        except Exception as e:
            self.logger.error("database_init_failed", error=str(e))
            raise

    @staticmethod
    def get_db_config():
        """Get database configuration from environment"""
        return {
            'dbname': os.getenv('DB_NAME', 'kickstarter_db'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': os.getenv('DB_PORT', '5432')
        }

    @backoff.on_exception(
        backoff.expo,
        (aiohttp.ClientError, redis.RedisError),
        max_tries=5
    )
    async def fetch_project_data(self, url: str) -> Optional[Dict]:
        """Fetch project data with caching and retry logic"""
        cache_key = f"project:{hashlib.md5(url.encode()).hexdigest()}"
        
        try:
            # Check cache first
            cached_data = self.redis_client.get(cache_key)
            if cached_data:
                return json.loads(cached_data)

            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    response.raise_for_status()
                    data = await response.json()
                    
                    # Cache the result with versioning
                    data['data_version'] = datetime.now().isoformat()
                    self.redis_client.setex(
                        cache_key,
                        timedelta(hours=1),
                        json.dumps(data)
                    )
                    return data
                    
        except Exception as e:
            self.logger.error("fetch_failed", url=url, error=str(e))
            PROCESSING_ERRORS.inc()
            raise

    def calculate_data_quality_score(self, project: Project) -> float:
        """Calculate data quality score"""
        score = 1.0
        valid, errors = project.validate()
        
        if not valid:
            score *= 0.5
            
        # Check data completeness
        fields = project.to_dict()
        total_fields = len(PROJECT_SCHEMA['properties'])
        filled_fields = sum(1 for v in fields.values() if v is not None)
        completeness = filled_fields / total_fields
        
        score *= completeness
        
        return score

    async def clean_project_data(self, raw_data: Dict) -> Project:
        """Clean and validate project data"""
        try:
            # Validate against schema
            validate(instance=raw_data, schema=PROJECT_SCHEMA)
            
            # Clean and transform data
            cleaned_data = {
                'id': int(raw_data['id']),
                'name': raw_data['name'].strip(),
                'category_name': raw_data.get('category_name', '').strip(),
                'goal': float(raw_data.get('goal', 0)),
                'pledged': float(raw_data.get('pledged', 0)),
                'state': raw_data.get('state', '').lower(),
                'backers_count': int(raw_data.get('backers_count', 0)),
                'created_at': raw_data.get('created_at'),
                'updated_at': raw_data.get('updated_at'),
                'data_version': raw_data.get('data_version')
            }
            
            project = Project.from_dict(cleaned_data)
            
            # Calculate and record data quality
            quality_score = self.calculate_data_quality_score(project)
            DATA_QUALITY_SCORE.set(quality_score)
            
            return project
            
        except Exception as e:
            self.logger.error("data_cleaning_failed", error=str(e), data=raw_data)
            PROCESSING_ERRORS.inc()
            raise

    async def process_projects(self, projects: List[Dict]):
        """Process multiple projects in parallel with batching"""
        BATCH_SIZE = 100
        
        async def process_batch(batch: List[Dict]):
            try:
                cleaned_projects = []
                for project_data in batch:
                    project = await self.clean_project_data(project_data)
                    if project:
                        cleaned_projects.append(project)
                
                if cleaned_projects:
                    await self.save_projects_batch(cleaned_projects)
                return cleaned_projects
            except Exception as e:
                self.logger.error("batch_processing_failed", error=str(e))
                PROCESSING_ERRORS.inc()
                return []

        # Process in batches
        results = []
        for i in range(0, len(projects), BATCH_SIZE):
            batch = projects[i:i + BATCH_SIZE]
            batch_results = await process_batch(batch)
            results.extend(batch_results)
        
        return results

    async def save_projects_batch(self, projects: List[Project]):
        """Save multiple projects in a single transaction"""
        ACTIVE_CONNECTIONS.inc()
        try:
            conn = self.db_pool.getconn()
            cur = conn.cursor()
            
            # Create temporary table for batch insert
            cur.execute("""
                CREATE TEMP TABLE tmp_projects (LIKE projects INCLUDING ALL)
                ON COMMIT DROP;
            """)
            
            # Prepare batch data
            batch_data = [project.to_dict() for project in projects]
            
            # Bulk insert into temp table
            cur.executemany("""
                INSERT INTO tmp_projects (
                    id, name, category_name, goal, pledged, state, 
                    backers_count, created_at, updated_at, data_version
                ) VALUES (
                    %(id)s, %(name)s, %(category_name)s, %(goal)s, 
                    %(pledged)s, %(state)s, %(backers_count)s,
                    %(created_at)s, %(updated_at)s, %(data_version)s
                )
            """, batch_data)
            
            # Merge temp table with main table
            cur.execute("""
                INSERT INTO projects
                SELECT * FROM tmp_projects
                ON CONFLICT (id) DO UPDATE
                SET name = EXCLUDED.name,
                    category_name = EXCLUDED.category_name,
                    goal = EXCLUDED.goal,
                    pledged = EXCLUDED.pledged,
                    state = EXCLUDED.state,
                    backers_count = EXCLUDED.backers_count,
                    updated_at = NOW(),
                    data_version = EXCLUDED.data_version;
            """)
            
            conn.commit()
            
        except Exception as e:
            self.logger.error("batch_save_failed", error=str(e))
            PROCESSING_ERRORS.inc()
            conn.rollback()
            raise
        finally:
            cur.close()
            self.db_pool.putconn(conn)
            ACTIVE_CONNECTIONS.dec()

    def analyze_data(self):
        """Analyze collected data with advanced metrics"""
        try:
            conn = self.db_pool.getconn()
            
            # Load data into pandas with partitioning
            df = pd.read_sql_query("""
                SELECT 
                    category_name,
                    DATE_TRUNC('month', created_at) as month,
                    COUNT(*) as project_count,
                    AVG(goal) as avg_goal,
                    AVG(pledged) as avg_pledged,
                    AVG(backers_count) as avg_backers,
                    SUM(CASE WHEN pledged >= goal THEN 1 ELSE 0 END) as successful_projects
                FROM projects 
                WHERE created_at >= NOW() - INTERVAL '1 year'
                GROUP BY category_name, DATE_TRUNC('month', created_at)
                ORDER BY month DESC, category_name
            """, conn)
            
            # Generate comprehensive insights
            insights = {
                'top_categories': df.groupby('category_name')['project_count'].sum()
                    .nlargest(5).to_dict(),
                'monthly_trends': df.groupby('month')[
                    ['project_count', 'avg_goal', 'avg_pledged']
                ].mean().to_dict(),
                'success_rate': (df['successful_projects'] / df['project_count']).mean(),
                'funding_ratio': (df['avg_pledged'] / df['avg_goal']).mean(),
                'backer_distribution': df['avg_backers'].describe().to_dict()
            }
            
            # Cache insights with versioning
            cache_key = f"insights:{datetime.now().strftime('%Y-%m-%d')}"
            self.redis_client.setex(
                cache_key,
                timedelta(days=1),
                json.dumps(insights)
            )
            
            return insights
            
        finally:
            self.db_pool.putconn(conn)

    async def run_pipeline(self):
        """Main pipeline execution with monitoring"""
        start_time = time.time()
        SCRAPE_COUNTER.inc()
        
        try:
            # Fetch and process data
            raw_data = await self.fetch_project_data(os.getenv('KICKSTARTER_API_URL'))
            if not raw_data:
                return
                
            processed_projects = await self.process_projects(raw_data)
            
            # Analyze results
            insights = self.analyze_data()
            
            # Log metrics
            processing_time = time.time() - start_time
            PROCESSING_TIME.observe(processing_time)
            
            self.logger.info("pipeline_completed",
                           projects_processed=len(processed_projects),
                           processing_time=processing_time,
                           insights=insights)
            
        except Exception as e:
            self.logger.error("pipeline_failed", error=str(e))
            PROCESSING_ERRORS.inc()
            raise
        finally:
            # Cleanup
            await self.cleanup_old_data()

    async def cleanup_old_data(self):
        """Clean up old data and maintain storage"""
        try:
            # Remove old cache entries
            for key in self.redis_client.scan_iter("project:*"):
                if self.redis_client.ttl(key) < 0:
                    self.redis_client.delete(key)
            
            # Archive old data
            conn = self.db_pool.getconn()
            cur = conn.cursor()
            
            cur.execute("""
                INSERT INTO projects_archive
                SELECT *
                FROM projects
                WHERE updated_at < NOW() - INTERVAL '1 year';
                
                DELETE FROM projects
                WHERE updated_at < NOW() - INTERVAL '1 year';
            """)
            
            conn.commit()
            
        except Exception as e:
            self.logger.error("cleanup_failed", error=str(e))
        finally:
            if 'conn' in locals():
                cur.close()
                self.db_pool.putconn(conn)

def main():
    # Load environment variables
    load_dotenv()
    
    # Initialize and run pipeline
    pipeline = DataPipeline()
    
    # Run pipeline on schedule
    schedule.every(1).hour.do(lambda: asyncio.run(pipeline.run_pipeline()))
    
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    main()