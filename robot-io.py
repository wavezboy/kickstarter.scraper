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

# Load environment variables from .env file if available
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('kickstarter_downloader.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Database configuration from environment variables
DB_CONFIG = {
    'dbname': os.getenv('DB_NAME', 'kickstarter_db'),
    'user': os.getenv('DB_USER', 'your_username'),
    'password': os.getenv('DB_PASSWORD', 'your_password'),
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432')
}

# URL of the Kickstarter datasets page
DATASET_URL = 'https://webrobots.io/kickstarter-datasets/'

# Connection pool
connection_pool = None

def init_db_pool(min_conn=1, max_conn=5):
    """Initialize database connection pool"""
    global connection_pool
    try:
        connection_pool = pool.SimpleConnectionPool(min_conn, max_conn, **DB_CONFIG)
        logger.info(f"Database connection pool initialized with {min_conn}-{max_conn} connections")
        return True
    except Exception as e:
        logger.error(f"Failed to initialize connection pool: {e}")
        return False

def get_connection():
    """Get a connection from the pool"""
    if connection_pool:
        return connection_pool.getconn()
    else:
        # Fallback to direct connection if pool not initialized
        return psycopg2.connect(**DB_CONFIG)

def release_connection(conn):
    """Release connection back to the pool"""
    if connection_pool and conn:
        connection_pool.putconn(conn)

def create_database_table():
    """Create the kickstarter_projects table if it doesn't exist."""
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        # Create table to track processed files
        processed_files_table = """
        CREATE TABLE IF NOT EXISTS processed_files (
            file_hash TEXT PRIMARY KEY,
            file_url TEXT NOT NULL,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            record_count INTEGER DEFAULT 0
        );
        """
        cur.execute(processed_files_table)
        
        create_table_query = """
        CREATE TABLE IF NOT EXISTS kickstarter_projects (
            id BIGINT PRIMARY KEY,
            name TEXT,
            category_name TEXT,
            category_slug TEXT,
            goal DOUBLE PRECISION,
            pledged DOUBLE PRECISION,
            state TEXT,
            backers_count INTEGER,
            created_at TIMESTAMP,
            launched_at TIMESTAMP,
            deadline TIMESTAMP,
            state_changed_at TIMESTAMP,
            creator_name TEXT,
            location_name TEXT,
            country TEXT,
            currency TEXT,
            scraped_at DATE
        );
        """
        cur.execute(create_table_query)
        
        # Add useful indexes
        cur.execute("CREATE INDEX IF NOT EXISTS idx_projects_state ON kickstarter_projects(state);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_projects_category ON kickstarter_projects(category_slug);")
        
        conn.commit()
        logger.info("Database tables and indexes are ready.")
    except Exception as e:
        logger.error(f"Error creating table: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cur.close()
            release_connection(conn)

def is_file_processed(file_url):
    """Check if a file has already been processed"""
    conn = None
    try:
        file_hash = hashlib.md5(file_url.encode()).hexdigest()
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM processed_files WHERE file_hash = %s", (file_hash,))
        count = cur.fetchone()[0]
        return count > 0
    except Exception as e:
        logger.error(f"Error checking processed file: {e}")
        return False
    finally:
        if conn:
            cur.close()
            release_connection(conn)

def mark_file_processed(file_url, record_count=0):
    """Mark a file as processed"""
    conn = None
    try:
        file_hash = hashlib.md5(file_url.encode()).hexdigest()
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO processed_files (file_hash, file_url, record_count) VALUES (%s, %s, %s)",
            (file_hash, file_url, record_count)
        )
        conn.commit()
        logger.info(f"Marked {file_url} as processed with {record_count} records")
        return True
    except Exception as e:
        logger.error(f"Error marking file as processed: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            cur.close()
            release_connection(conn)

def validate_project(project):
    """Validate project data before insertion"""
    if not project or not isinstance(project, dict):
        return False
        
    # Check required fields
    if 'id' not in project or not project['id']:
        return False
        
    # Validate numeric fields
    try:
        int(project.get('id', 0))
        float(project.get('goal', 0) or 0)
        float(project.get('pledged', 0) or 0)
        int(project.get('backers_count', 0) or 0)
    except (ValueError, TypeError):
        return False
        
    return True

def insert_project(project, scrape_date):
    """Insert a project into the PostgreSQL database, skipping duplicates."""
    if not validate_project(project):
        return False
        
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        insert_query = """
        INSERT INTO kickstarter_projects (
            id, name, category_name, category_slug, goal, pledged, state,
            backers_count, created_at, launched_at, deadline, state_changed_at,
            creator_name, location_name, country, currency, scraped_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING;
        """
        
        # Extract and convert data safely
        category = project.get('category', {}) or {}
        creator = project.get('creator', {}) or {}
        location = project.get('location', {}) or {}
        
        # Convert Unix timestamps to datetime
        def safe_timestamp(ts):
            if not ts:
                return None
            try:
                return datetime.fromtimestamp(ts)
            except (ValueError, TypeError, OverflowError):
                return None
        
        created_at = safe_timestamp(project.get('created_at'))
        launched_at = safe_timestamp(project.get('launched_at'))
        deadline = safe_timestamp(project.get('deadline'))
        state_changed_at = safe_timestamp(project.get('state_changed_at'))
        
        data = (
            project.get('id'),
            project.get('name', ''),
            category.get('name', ''),
            category.get('slug', ''),
            float(project.get('goal', 0) or 0),
            float(project.get('pledged', 0) or 0),
            project.get('state', ''),
            int(project.get('backers_count', 0) or 0),
            created_at,
            launched_at,
            deadline,
            state_changed_at,
            creator.get('name', ''),
            location.get('name', ''),
            project.get('country', ''),
            project.get('currency', ''),
            scrape_date
        )
        
        cur.execute(insert_query, data)
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"Error inserting project {project.get('id', 'unknown')}: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            cur.close()
            release_connection(conn)

def create_session_with_retries():
    """Create a requests session with retry logic"""
    session = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD"]
    )
    session.mount('http://', HTTPAdapter(max_retries=retries))
    session.mount('https://', HTTPAdapter(max_retries=retries))
    return session

def download_and_process_file(url, scrape_date, force_reprocess=False):
    """Download a JSON file and process its contents into the database."""
    # Skip if already processed and not forced to reprocess
    if not force_reprocess and is_file_processed(url):
        logger.info(f"Skipping already processed file: {url}")
        return True
        
    try:
        logger.info(f"Downloading dataset from {url}")
        session = create_session_with_retries()
        
        # Get file size for progress bar if possible
        try:
            response_head = session.head(url, timeout=10)
            total_size = int(response_head.headers.get('content-length', 0))
        except Exception:
            total_size = 0
            
        response = session.get(url, stream=True)
        response.raise_for_status()
        
        # Process JSON streaming format (line-delimited JSON)
        record_count = 0
        processed_count = 0
        
        # Setup progress bar if we have file size
        progress_bar = None
        if total_size > 0:
            progress_bar = tqdm(total=total_size, unit='B', unit_scale=True, 
                               desc=f"Processing {os.path.basename(url)}")
        
        for line in response.iter_lines():
            if line:
                if progress_bar:
                    progress_bar.update(len(line) + 1)  # +1 for newline
                    
                try:
                    record_count += 1
                    project = json.loads(line.decode('utf-8'))
                    if insert_project(project, scrape_date):
                        processed_count += 1
                        
                    # Log progress periodically
                    if record_count % 1000 == 0:
                        logger.info(f"Processed {record_count} records from {url}")
                        
                except json.JSONDecodeError as e:
                    logger.warning(f"Skipping invalid JSON line: {e}")
                except Exception as e:
                    logger.error(f"Error processing line: {e}")
        
        if progress_bar:
            progress_bar.close()
            
        # Mark file as processed
        mark_file_processed(url, processed_count)
        logger.info(f"Completed processing dataset from {url}. Processed {processed_count} of {record_count} records.")
        return True
    except requests.RequestException as e:
        logger.error(f"Error downloading {url}: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error processing {url}: {e}")
        return False

def scrape_dataset_links():
    """Scrape the dataset page for JSON download links."""
    try:
        logger.info(f"Scraping dataset links from {DATASET_URL}")
        session = create_session_with_retries()
        response = session.get(DATASET_URL)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        links = []
        
        # Find all links containing 'JSON' and a date in the format YYYY-MM-DD
        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href']
            if 'JSON' in a_tag.text and 'http' in href:
                # Extract date from the link text (e.g., 2025-05-12)
                date_str = a_tag.text.split('[')[0].strip()
                try:
                    scrape_date = datetime.strptime(date_str, '%Y-%m-%d').date()
                    links.append((href, scrape_date))
                except ValueError:
                    logger.warning(f"Could not parse date from {a_tag.text}")
        
        # Sort links by date (newest first)
        links.sort(key=lambda x: x[1], reverse=True)
        logger.info(f"Found {len(links)} dataset links")
        return links
    except requests.RequestException as e:
        logger.error(f"Error scraping dataset links: {e}")
        return []

def run_scraper(max_datasets=None, force_reprocess=False):
    """Main function to run the scraper and process datasets."""
    logger.info("Starting Kickstarter dataset scraper")
    
    # Initialize connection pool
    init_db_pool()
    
    # Create database table
    create_database_table()
    
    # Scrape dataset links
    dataset_links = scrape_dataset_links()
    
    if not dataset_links:
        logger.error("No dataset links found. Exiting.")
        return False
    
    # Process each dataset
    processed_count = 0
    for url, scrape_date in dataset_links:
        if max_datasets is not None and processed_count >= max_datasets:
            logger.info(f"Reached maximum dataset limit ({max_datasets})")
            break
            
        success = download_and_process_file(url, scrape_date, force_reprocess)
        if success:
            processed_count += 1
    
    logger.info(f"Scraper run completed. Processed {processed_count} datasets.")
    return True

def schedule_scraper():
    """Schedule the scraper to run on the 1st and 15th of each month."""
    # Run on the 1st of each month at midnight
    schedule.every().month.at("00:00").do(run_scraper).tag('scraper')
    
    # Run on the 15th of each month at midnight
    schedule.every().day.at("00:00").do(
        lambda: run_scraper() if datetime.now().day == 15 else None
    ).tag('scraper')
    
    logger.info("Scheduler started. Waiting for next run...")
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
    except KeyboardInterrupt:
        logger.info("Scheduler stopped by user")

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Kickstarter Dataset Scraper')
    parser.add_argument('--run-once', action='store_true', help='Run the scraper once and exit')
    parser.add_argument('--schedule', action='store_true', help='Run the scraper on a schedule')
    parser.add_argument('--max-datasets', type=int, help='Maximum number of datasets to process')
    parser.add_argument('--force-reprocess', action='store_true', help='Force reprocessing of already processed files')
    parser.add_argument('--list-only', action='store_true', help='Only list available datasets without downloading')
    return parser.parse_args()

def list_available_datasets():
    """List available datasets without downloading them."""
    dataset_links = scrape_dataset_links()
    
    if not dataset_links:
        print("No datasets found.")
        return
        
    print("\nAvailable Kickstarter Datasets:")
    print("-" * 80)
    print(f"{'Date':<12} {'URL':<68}")
    print("-" * 80)
    
    for url, scrape_date in dataset_links:
        print(f"{scrape_date.strftime('%Y-%m-%d'):<12} {url:<68}")

if __name__ == "__main__":
    args = parse_arguments()
    
    if args.list_only:
        list_available_datasets()
    elif args.schedule:
        schedule_scraper()
    elif args.run_once:
        run_scraper(max_datasets=args.max_datasets, force_reprocess=args.force_reprocess)
    else:
        # Default behavior: run once
        print("Running scraper once. Use --schedule to run scheduler or --help for more options.")
        run_scraper(max_datasets=args.max_datasets, force_reprocess=args.force_reprocess)