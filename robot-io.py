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
from typing import Optional, List, Dict, Any
import prometheus_client
from prometheus_client import Counter, Histogram, Gauge
import structlog
from tenacity import retry, stop_after_attempt, wait_exponential
import redis
from jsonschema import validate
import pandas as pd

# Metrics
SCRAPE_COUNTER = Counter('kickstarter_scrapes_total', 'Total number of scraping operations')
PROCESSING_TIME = Histogram('kickstarter_processing_seconds', 'Time spent processing data')
ACTIVE_CONNECTIONS = Gauge('kickstarter_db_connections_active', 'Number of active database connections')

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
    },
    "required": ["id", "name"]
}

@dataclass
class Project:
    """Project data model"""
    id: int
    name: str
    category_name: Optional[str] = None
    goal: Optional[float] = None
    pledged: Optional[float] = None
    state: Optional[str] = None
    backers_count: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Project':
        return cls(**{k: v for k, v in data.items() if k in cls.__annotations__})

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
        
        # Initialize Redis cache
        self.redis_client = redis.Redis(
            host=os.getenv('REDIS_HOST', 'localhost'),
            port=int(os.getenv('REDIS_PORT', 6379)),
            decode_responses=True
        )
        
        # Initialize database pool
        self.db_pool = self.init_db_pool()
        
        # Initialize metrics server
        prometheus_client.start_http_server(8000)

    def init_db_pool(self):
        """Initialize database connection pool with monitoring"""
        try:
            pool = psycopg2.pool.SimpleConnectionPool(
                minconn=1,
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

    async def fetch_project_data(self, url: str) -> Optional[Dict]:
        """Fetch project data with caching and retry logic"""
        cache_key = f"project:{hashlib.md5(url.encode()).hexdigest()}"
        
        # Check cache first
        cached_data = self.redis_client.get(cache_key)
        if cached_data:
            return json.loads(cached_data)

        @retry(
            stop=stop_after_attempt(3),
            wait=wait_exponential(multiplier=1, min=4, max=10)
        )
        async def _fetch():
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    response.raise_for_status()
                    return await response.json()

        try:
            data = await _fetch()
            # Cache the result
            self.redis_client.setex(cache_key, 3600, json.dumps(data))
            return data
        except Exception as e:
            self.logger.error("fetch_failed", url=url, error=str(e))
            return None

    def clean_project_data(self, raw_data: Dict) -> Project:
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
                'backers_count': int(raw_data.get('backers_count', 0))
            }
            
            return Project.from_dict(cleaned_data)
        except Exception as e:
            self.logger.error("data_cleaning_failed", error=str(e), data=raw_data)
            raise

    async def process_projects(self, projects: List[Dict]):
        """Process multiple projects in parallel"""
        async def process_single_project(project_data: Dict):
            try:
                project = self.clean_project_data(project_data)
                await self.save_project(project)
                return project
            except Exception as e:
                self.logger.error("project_processing_failed", 
                                project_id=project_data.get('id'),
                                error=str(e))
                return None

        tasks = [process_single_project(p) for p in projects]
        return await asyncio.gather(*tasks)

    async def save_project(self, project: Project):
        """Save project to database with connection pooling"""
        ACTIVE_CONNECTIONS.inc()
        try:
            conn = self.db_pool.getconn()
            cur = conn.cursor()
            
            query = """
            INSERT INTO projects (id, name, category_name, goal, pledged, state, backers_count)
            VALUES (%(id)s, %(name)s, %(category_name)s, %(goal)s, %(pledged)s, %(state)s, %(backers_count)s)
            ON CONFLICT (id) DO UPDATE
            SET name = EXCLUDED.name,
                category_name = EXCLUDED.category_name,
                goal = EXCLUDED.goal,
                pledged = EXCLUDED.pledged,
                state = EXCLUDED.state,
                backers_count = EXCLUDED.backers_count,
                updated_at = NOW()
            """
            
            cur.execute(query, project.to_dict())
            conn.commit()
            
        except Exception as e:
            self.logger.error("save_failed", project_id=project.id, error=str(e))
            conn.rollback()
            raise
        finally:
            cur.close()
            self.db_pool.putconn(conn)
            ACTIVE_CONNECTIONS.dec()

    def analyze_data(self):
        """Analyze collected data"""
        try:
            conn = self.db_pool.getconn()
            
            # Load data into pandas
            df = pd.read_sql_query("""
                SELECT category_name, 
                       COUNT(*) as project_count,
                       AVG(goal) as avg_goal,
                       AVG(pledged) as avg_pledged,
                       AVG(backers_count) as avg_backers
                FROM projects 
                GROUP BY category_name
            """, conn)
            
            # Generate insights
            insights = {
                'top_categories': df.nlargest(5, 'project_count')[['category_name', 'project_count']].to_dict(),
                'funding_success': df['avg_pledged'] / df['avg_goal'],
                'backer_distribution': df['avg_backers'].describe().to_dict()
            }
            
            # Cache insights
            self.redis_client.setex('project_insights', 3600, json.dumps(insights))
            
            return insights
            
        finally:
            self.db_pool.putconn(conn)

    async def run_pipeline(self):
        """Main pipeline execution"""
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
            raise

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