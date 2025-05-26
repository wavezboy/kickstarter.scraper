import time
import random
import os
import json
import csv
import logging
import subprocess
import argparse
from typing import List, Dict, Any, Tuple, Optional, Union
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
from dataclasses import dataclass, field, asdict

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup, Tag
import requests
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("kickstarter_scraper.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("KickstarterScraper")

@dataclass
class Project:
    """Class for storing project data"""
    url: str
    title: str
    creator: str = ""
    funding_info: str = ""
    full_description: str = ""
    funding_goal: str = ""
    backers_count: str = ""
    end_date: str = ""
    creator_bio: str = ""
    location: str = ""
    updates_count: str = ""
    comments_count: str = ""
    campaign_status: str = ""
    categories: List[str] = field(default_factory=list)
    last_updated: str = ""
    created_at: str = ""
    detailed_fetched: bool = False
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary with categories as string"""
        data = asdict(self)
        data['categories'] = ", ".join(self.categories)
        return data

class ChromeManager:
    """Class to manage Chrome browser instances"""
    
    @staticmethod
    def start_chrome_with_debugging(port: int = 9222, profile_dir: str = None) -> Tuple[bool, str]:
        """Start Chrome with remote debugging enabled"""
        try:
            # Get Chrome path
            chrome_paths = [
                r"C:\Program Files\Google\Chrome\Application\chrome.exe",  # Windows default
                r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",  # Windows x86
                "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",  # macOS
                "/usr/bin/google-chrome",  # Linux
                "/usr/bin/google-chrome-stable"  # Linux alternative
            ]
            
            chrome_path = None
            for path in chrome_paths:
                if os.path.exists(path):
                    chrome_path = path
                    break
            
            if not chrome_path:
                return False, "Chrome executable not found in default locations"
            
            # Create profile directory if not provided
            if not profile_dir:
                profile_dir = os.path.join(os.path.expanduser("~"), "chrome_debug_profile")
                os.makedirs(profile_dir, exist_ok=True)
            
            # Start Chrome with debugging
            cmd = [
                chrome_path,
                f"--remote-debugging-port={port}",
                f"--user-data-dir={profile_dir}"
            ]
            
            subprocess.Popen(cmd)
            logger.info(f"Chrome started with debugging enabled on port {port}")
            
            # Give Chrome time to start
            time.sleep(3)
            return True, "Chrome started successfully"
        except Exception as e:
            return False, f"Failed to start Chrome: {str(e)}"
    
    @staticmethod
    def create_driver(use_existing_browser: bool = False, 
                      headless: bool = False,
                      port: int = 9222,
                      user_agent: str = None) -> webdriver.Chrome:
        """Create and configure a Chrome WebDriver"""
        chrome_options = Options()
        
        if use_existing_browser:
            logger.info(f"Connecting to existing Chrome browser on port {port}...")
            chrome_options.add_experimental_option("debuggerAddress", f"127.0.0.1:{port}")
        else:
            logger.info("Setting up new Chrome browser...")
            chrome_options.add_argument("--window-size=1920,1080")
            
            if headless:
                chrome_options.add_argument("--headless=new")
                
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            
            # Set user agent
            default_ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
            chrome_options.add_argument(f"--user-agent={user_agent or default_ua}")
        
        try:
            # Setup the Chrome WebDriver
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=chrome_options)
            return driver
        except Exception as e:
            logger.error(f"Failed to create Chrome driver: {str(e)}")
            raise

class KickstarterScraper:
    """Class for scraping Kickstarter projects"""
    
    def __init__(self, 
                 use_existing_browser: bool = True, 
                 headless: bool = False,
                 debug_port: int = 9222,
                 max_retries: int = 3,
                 delay_range: Tuple[float, float] = (2.0, 5.0),
                 user_agent: str = None):
        """Initialize the scraper with configuration"""
        self.use_existing_browser = use_existing_browser
        self.headless = headless
        self.debug_port = debug_port
        self.max_retries = max_retries
        self.delay_range = delay_range
        self.user_agent = user_agent
        self.driver = None
        self.checkpoint_file = "scraper_checkpoint.json"
        
    def __enter__(self):
        """Initialize the driver when entering context"""
        self.driver = ChromeManager.create_driver(
            use_existing_browser=self.use_existing_browser, 
            headless=self.headless,
            port=self.debug_port,
            user_agent=self.user_agent
        )
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close the driver when exiting context"""
        if self.driver and not self.use_existing_browser:
            self.driver.quit()
            logger.info("Browser closed.")
    
    def random_delay(self, message: str = "Waiting"):
        """Add a random delay to simulate human behavior"""
        delay = self.delay_range[0] + random.random() * (self.delay_range[1] - self.delay_range[0])
        logger.info(f"{message} {delay:.2f} seconds...")
        time.sleep(delay)
    
    def navigate_with_retry(self, url: str) -> bool:
        """Navigate to a URL with retry mechanism"""
        for attempt in range(self.max_retries):
            try:
                self.driver.get(url)
                
                # Wait for the page to load (check for common elements)
                try:
                    WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((By.TAG_NAME, "body"))
                    )
                    return True
                except TimeoutException:
                    logger.warning(f"Timeout waiting for page to load: {url}")
                    
                # Check if CAPTCHA is present
                if "captcha" in self.driver.page_source.lower() or "robot" in self.driver.page_source.lower():
                    logger.warning("CAPTCHA detected! Please solve it manually.")
                    input("Press Enter after solving the CAPTCHA...")
            
            except WebDriverException as e:
                logger.error(f"Navigation error (attempt {attempt+1}/{self.max_retries}): {str(e)}")
                
            if attempt < self.max_retries - 1:
                wait_time = (attempt + 1) * 2  # Exponential backoff
                logger.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
        
        logger.error(f"Failed to navigate to {url} after {self.max_retries} attempts")
        return False
    
    def save_checkpoint(self, projects: List[Project], completed_urls: List[str]):
        """Save progress checkpoint"""
        checkpoint_data = {
            "timestamp": time.time(),
            "projects": [p.to_dict() for p in projects],
            "completed_urls": completed_urls
        }
        
        with open(self.checkpoint_file, "w", encoding="utf-8") as f:
            json.dump(checkpoint_data, f, indent=2)
        
        logger.info(f"Checkpoint saved: {len(projects)} projects, {len(completed_urls)} completed URLs")
    
    def load_checkpoint(self) -> Tuple[List[Project], List[str]]:
        """Load progress from checkpoint if exists"""
        if not os.path.exists(self.checkpoint_file):
            return [], []
        
        try:
            with open(self.checkpoint_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            
            projects = []
            for p_dict in data["projects"]:
                # Convert categories back to list if it was saved as string
                if isinstance(p_dict.get("categories", ""), str):
                    p_dict["categories"] = [c.strip() for c in p_dict.get("categories", "").split(",") if c.strip()]
                
                # Create Project object
                projects.append(Project(**p_dict))
            
            logger.info(f"Loaded checkpoint: {len(projects)} projects, {len(data['completed_urls'])} completed URLs")
            return projects, data.get("completed_urls", [])
        
        except Exception as e:
            logger.error(f"Error loading checkpoint: {str(e)}")
            return [], []
    
    def extract_projects_from_soup(self, soup: BeautifulSoup) -> List[Project]:
        """Extract project information from BeautifulSoup object"""
        # Try various selectors to find project cards
        selectors = [
            '.js-react-proj-card', 
            '[data-testid="project-card"]',
            '.ProjectCard-module_root',
            'div[role="presentation"]',
            '.grid-project',
            '.js-project-card',
            '.project-card'
        ]
        
        project_cards = None
        
        for selector in selectors:
            cards = soup.select(selector)
            logger.info(f"Selector '{selector}': {len(cards)} elements found")
            if cards:
                project_cards = cards
                break
        
        if not project_cards:
            # Try a broader approach - look for project card patterns
            logger.info("No project cards found with standard selectors. Trying alternate methods...")
            
            # Look for elements containing typical project card elements
            potential_cards = soup.find_all(['div', 'article'], class_=re.compile(r'card|project', re.I))
            if potential_cards:
                project_cards = potential_cards
                logger.info(f"Found {len(project_cards)} potential cards by pattern matching")
            else:
                # Try finding by link pattern
                links = soup.find_all('a', href=re.compile(r'/projects/'))
                if links:
                    parent_elements = set(link.parent.parent for link in links)
                    project_cards = list(parent_elements)
                    logger.info(f"Found {len(project_cards)} potential cards by project link pattern")
        
        if not project_cards:
            logger.warning("Could not find any project cards on the page.")
            return []
        
        logger.info(f"Found {len(project_cards)} project cards")
        projects = []
        
        for i, card in enumerate(project_cards):
            try:
                project = self.extract_project_from_card(card, i)
                if project:
                    projects.append(project)
                    logger.info(f"Extracted data for project: {project.title}")
            except Exception as e:
                logger.error(f"Error extracting project data from card {i}: {e}")
        
        return projects
    
    def extract_project_from_card(self, card: Tag, index: int) -> Optional[Project]:
        """Extract project information from a single card element"""
        # Find the project URL - look for links containing /projects/
        links = card.find_all('a')
        project_links = [link for link in links if '/projects/' in link.get('href', '')]
        
        url = "No URL found"
        if project_links:
            main_link = project_links[0]
            href = main_link.get('href', '')
            if href.startswith('/'):
                url = f"https://www.kickstarter.com{href}"
            else:
                url = href
        elif links:
            # Fallback to any link if no project link is found
            href = links[0].get('href', '')
            if href:
                if href.startswith('/'):
                    url = f"https://www.kickstarter.com{href}"
                else:
                    url = href
        
        if url == "No URL found" or not url.startswith('http'):
            return None
        
        # Extract title - try multiple approaches
        title_candidates = [
            card.select_one('h3'),
            card.select_one('h2'),
            card.select_one('.type-28'),
            card.select_one('[data-testid="project-card-title"]')
        ]
        
        # Also check if any link text looks like a title
        for link in links:
            link_text = link.get_text(strip=True)
            if len(link_text) > 10:
                title_candidates.append(link)
        
        title_elem = next((elem for elem in title_candidates if elem is not None), None)
        title = "Title not found"
        if title_elem:
            title = title_elem.get_text(strip=True)
        else:
            # Last resort - get any substantial text from the card
            all_text = card.get_text(strip=True)
            if len(all_text) > 10:
                title = all_text[:50] + ("..." if len(all_text) > 50 else "")
        
        if title == "Title not found":
            return None
        
        # Create basic project object
        project = Project(url=url, title=title)
        
        # Extract creator
        creator_candidates = [
            card.select_one('.CreatorName-module_root'),
            card.select_one('.type-14'),
            card.select_one('[data-testid="creator-name"]')
        ]
        
        creator_elem = next((elem for elem in creator_candidates if elem is not None), None)
        if creator_elem:
            project.creator = creator_elem.get_text(strip=True)
        
        # Extract funding status - look for text with funding keywords
        funding_texts = []
        for elem in card.find_all(text=re.compile(r'fund|back|pledge|success|100%|ended', re.I)):
            if elem.strip():
                funding_texts.append(elem.strip())
        
        if funding_texts:
            project.funding_info = " | ".join(funding_texts)
        
        # Extract category if available
        category_link = card.find('a', href=re.compile(r'/discover/categories/'))
        if category_link:
            category_text = category_link.get_text(strip=True)
            if category_text:
                project.categories.append(category_text)
        
        return project
    
    def get_detailed_project_info(self, project: Project) -> Project:
        """Visit project page and extract detailed information"""
        if not project.url or project.url == "No URL found" or not project.url.startswith('http'):
            logger.warning(f"Skipping project {project.title} - No valid URL")
            return project
        
        # Skip if already fetched
        if project.detailed_fetched:
            return project
        
        url = project.url
        try:
            logger.info(f"Visiting: {url}")
            
            if not self.navigate_with_retry(url):
                return project
            
            # Add a small delay to allow dynamic content to load
            self.random_delay("Waiting for page content to load")
            
            # Get the page source
            html_content = self.driver.page_source
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Project description
            desc_candidates = [
                soup.select_one('.full-description'),
                soup.select_one('.project-description'),
                soup.select_one('.description'),
                soup.select_one('[data-test-id="project-description"]'),
                soup.select_one('.campaign-project-description')
            ]
            
            desc_elem = next((elem for elem in desc_candidates if elem is not None), None)
            if desc_elem:
                project.full_description = desc_elem.get_text(strip=True).replace('\n', ' ').replace('\r', '')
            
            # Funding goal
            goal_candidates = [
                soup.select_one('.money.goal'),
                soup.select_one('.goal'),
                soup.select_one('[data-test-id="funding-goal"]'),
                soup.select_one('.funding-goal')
            ]
            
            goal_elem = next((elem for elem in goal_candidates if elem is not None), None)
            if goal_elem:
                project.funding_goal = goal_elem.get_text(strip=True)
            
            # Campaign status
            status_candidates = [
                soup.select_one('.project-state'),
                soup.select_one('.NS_projects__header_metadata'),
                soup.select_one('[data-test-id="campaign-status"]')
            ]
            
            status_elem = next((elem for elem in status_candidates if elem is not None), None)
            if status_elem:
                project.campaign_status = status_elem.get_text(strip=True)
            
            # Backers count
            backers_candidates = [
                soup.select_one('.backers-count'),
                soup.select_one('.num-backers'),
                soup.select_one('[data-test-id="backers-count"]'),
                soup.select_one('.project-state-backings')
            ]
            
            backers_elem = next((elem for elem in backers_candidates if elem is not None), None)
            if backers_elem:
                project.backers_count = backers_elem.get_text(strip=True)
            
            # Campaign end date
            date_candidates = [
                soup.select_one('.js-campaign-end-date'),
                soup.select_one('.campaign-end-date'),
                soup.select_one('[data-test-id="end-date"]'),
                soup.select_one('.project-timing-info')
            ]
            
            date_elem = next((elem for elem in date_candidates if elem is not None), None)
            if date_elem:
                project.end_date = date_elem.get_text(strip=True)
            
            # Creator details
            creator_bio_candidates = [
                soup.select_one('.creator-bio'),
                soup.select_one('.creator-details'),
                soup.select_one('[data-test-id="creator-bio"]'),
                soup.select_one('.bio')
            ]
            
            creator_bio_elem = next((elem for elem in creator_bio_candidates if elem is not None), None)
            if creator_bio_elem:
                project.creator_bio = creator_bio_elem.get_text(strip=True).replace('\n', ' ').replace('\r', '')
            
            # Project location
            location_candidates = [
                soup.select_one('.location'),
                soup.select_one('.project-location'),
                soup.select_one('[data-test-id="location"]'),
                soup.select_one('.project-state-location')
            ]
            
            location_elem = next((elem for elem in location_candidates if elem is not None), None)
            if location_elem:
                project.location = location_elem.get_text(strip=True)
            
            # Project updates count
            updates_candidates = [
                soup.select_one('.js-updates-count'),
                soup.select_one('.updates-count'),
                soup.select_one('[data-test-id="updates-count"]')
            ]
            
            updates_elem = next((elem for elem in updates_candidates if elem is not None), None)
            if updates_elem:
                project.updates_count = updates_elem.get_text(strip=True)
            
            # Comments count
            comments_candidates = [
                soup.select_one('.js-comments-count'),
                soup.select_one('.comments-count'),
                soup.select_one('[data-test-id="comments-count"]')
            ]
            
            comments_elem = next((elem for elem in comments_candidates if elem is not None), None)
            if comments_elem:
                project.comments_count = comments_elem.get_text(strip=True)
            
            # Categories - more detailed categories from project page
            category_elements = soup.select('a[href*="/discover/categories/"]')
            if category_elements:
                project.categories = [elem.get_text(strip=True) for elem in category_elements if elem.get_text(strip=True)]
            
            # Try to find keywords in the full page to extract more info
            if not project.full_description:
                # Look for any paragraph with substantial content
                for p in soup.find_all('p'):
                    text = p.get_text(strip=True)
                    if len(text) > 100:  # Only consider paragraphs with substantial content
                        project.full_description = text.replace('\n', ' ').replace('\r', '')
                        logger.info("Found description in paragraph")
                        break
            
            # Mark as successfully fetched
            project.detailed_fetched = True
            logger.info(f"Successfully extracted details for: {project.title}")
            
        except Exception as e:
            logger.error(f"Error processing {url}: {e}")
        
        return project
    
    def fetch_detailed_info_parallel(self, projects: List[Project], max_workers: int = 4) -> List[Project]:
        """Fetch detailed information for each project in parallel"""
        logger.info(f"Fetching detailed information for {len(projects)} projects using {max_workers} workers")
        
        # Can't use parallel processing with an existing browser session
        if self.use_existing_browser:
            with tqdm(total=len(projects), desc="Fetching project details") as pbar:
                for i, project in enumerate(projects):
                    updated_project = self.get_detailed_project_info(project)
                    projects[i] = updated_project
                    pbar.update(1)
                    
                    # Add random delay between requests
                    if i < len(projects) - 1:
                        self.random_delay("Waiting before next project")
            
            return projects
        
        # Create a list to store results
        updated_projects = []
        completed_urls = []
        checkpoint_counter = 0
        
        # Create driver instances for each worker
        drivers = []
        for _ in range(max_workers):
            driver = ChromeManager.create_driver(
                use_existing_browser=False,
                headless=self.headless,
                user_agent=self.user_agent
            )
            drivers.append(driver)
        
        try:
            # Function to process a project with a specific driver
            def process_project(args):
                idx, project, driver_idx = args
                driver = drivers[driver_idx]
                
                try:
                    # Navigate to project page
                    driver.get(project.url)
                    
                    # Add a small delay
                    time.sleep(2 + random.random() * 2)
                    
                    # Get the page source
                    html_content = driver.page_source
                    soup = BeautifulSoup(html_content, 'html.parser')
                    
                    # Extract project information using the HTML
                    # Create a copy of the project
                    updated_project = Project(**asdict(project))
                    
                    # Project description
                    desc_elem = soup.select_one('.full-description, .project-description, .description, [data-test-id="project-description"], .campaign-project-description')
                    if desc_elem:
                        updated_project.full_description = desc_elem.get_text(strip=True).replace('\n', ' ').replace('\r', '')
                    
                    # Other fields... (similar to get_detailed_project_info)
                    
                    # Mark as fetched
                    updated_project.detailed_fetched = True
                    
                    return updated_project, project.url
                except Exception as e:
                    logger.error(f"Error in worker {driver_idx} processing {project.url}: {e}")
                    return project, None
            
            # Distribute projects among workers
            tasks = []
            for i, project in enumerate(projects):
                if not project.detailed_fetched:
                    driver_idx = i % max_workers
                    tasks.append((i, project, driver_idx))
                else:
                    updated_projects.append(project)
                    completed_urls.append(project.url)
            
            # Process in parallel
            with tqdm(total=len(tasks), desc="Fetching project details") as pbar:
                for i, task in enumerate(tasks):
                    _, project, driver_idx = task
                    
                    # Process project
                    updated_project, url = process_project(task)
                    updated_projects.append(updated_project)
                    
                    if url:
                        completed_urls.append(url)
                    
                    pbar.update(1)
                    
                    # Save checkpoint periodically
                    checkpoint_counter += 1
                    if checkpoint_counter >= 5:
                        self.save_checkpoint(updated_projects, completed_urls)
                        checkpoint_counter = 0
                    
                    # Add random delay between requests
                    if i < len(tasks) - 1:
                        time.sleep(1 + random.random() * 2)
            
            return updated_projects
            
        finally:
            # Close all driver instances
            for driver in drivers:
                try:
                    driver.quit()
                except:
                    pass
    
    def scrape_kickstarter_url(self, url: str, output_csv: str = "kickstarter_data.csv") -> List[Project]:
        """Main function to scrape project information from a Kickstarter URL"""
        try:
            logger.info(f"Preparing to scrape: {url}")
            
            # Load checkpoint if exists
            projects, completed_urls = self.load_checkpoint()
            if projects:
                resume = input(f"Found checkpoint with {len(projects)} projects. Resume? (y/n): ")
                if resume.lower() != 'y':
                    projects = []
                    completed_urls = []
            
            # Skip scraping if we have projects from checkpoint
            if not projects:
                if not self.navigate_with_retry(url):
                    return []
                
                # Add a random delay to simulate human behavior and allow the page to fully load
                self.random_delay("Waiting for page to load completely")
                
                # Save the HTML content to a file for debugging
                html_content = self.driver.page_source
                logger.info(f"Retrieved page content ({len(html_content)} bytes)")
                
                with open("kickstarter_page.html", "w", encoding="utf-8") as file:
                    file.write(html_content)
                logger.info("HTML content saved to kickstarter_page.html")
                
                # Parse the HTML with BeautifulSoup
                soup = BeautifulSoup(html_content, 'html.parser')
                
                # Extract project data
                projects = self.extract_projects_from_soup(soup)
                
                # Save initial data
                if projects:
                    # Save the extracted data to JSON for backup
                    with open("kickstarter_projects.json", "w", encoding="utf-8") as f:
                        json.dump([p.to_dict() for p in projects], f, indent=2, ensure_ascii=False)
                    logger.info(f"Successfully extracted {len(projects)} projects")
                    logger.info("Project data saved to kickstarter_projects.json")
                else:
                    logger.warning("No projects were found on the page.")
                    return []
            
            # Ask if user wants to fetch detailed information
            if projects and not all(p.detailed_fetched for p in projects):
                get_details = input("Do you want to fetch detailed information for each project? (y/n): ")
                if get_details.lower() == 'y':
                    projects = self.fetch_detailed_info_parallel(projects)
                    
                    # Save the detailed data to JSON for backup
                    with open("kickstarter_detailed_projects.json", "w", encoding="utf-8") as f:
                        json.dump([p.to_dict() for p in projects], f, indent=2, ensure_ascii=False)
                    logger.info("Detailed project data saved to kickstarter_detailed_projects.json")
            
            # Save to CSV
            self.save_to_csv(projects, output_csv)
            logger.info(f"All project data saved to {output_csv}")
            
            return projects
            
        except Exception as e:
            logger.error(f"An error occurred: {e}", exc_info=True)
            return []
    
    def save_to_csv(self, projects: List[Project], output_csv: str):
        """Save project data to CSV file"""
        # Define the fields we want in the final CSV (in order)
        fields_to_include = [
            'title', 
            'creator',
            'url',
            'funding_info',
            'campaign_status',
            'full_description',
            'funding_goal',
            'backers_count',
            'end_date',
            'creator_bio',
            'location',
            'updates_count',
            'comments_count',
            'categories',
            'last_updated',
            'created_at',
            'detailed_fetched'
        ]
        
        # Get all fieldnames that actually exist in our data
        all_fieldnames = set()
        for project in projects:
            all_fieldnames.update(project.to_dict().keys())
        
        # Prioritize our desired fields, then add any other fields that exist
        fields_to_exclude = ['image_url', 'card_index', 'percent_funded']
        fieldnames = [f for f in fields_to_include if f in all_fieldnames]
        fieldnames.extend(sorted([f for f in all_fieldnames 
                                 if f not in fields_to_include and f not in fields_to_exclude]))
        
        # Save to the final CSV file
        with open(output_csv, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            # Clean each row before writing
            for project in projects:
                # Convert to dict
                project_dict = project.to_dict()
                
                # Remove unwanted fields
                for field in fields_to_exclude:
                    if field in project_dict:
                        del project_dict[field]
                
                writer.writerow(project_dict)