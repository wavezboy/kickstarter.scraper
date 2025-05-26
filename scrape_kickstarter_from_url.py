import time
import random
import os
import json
import csv
import subprocess
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import re

def start_chrome_with_debugging():
    """Start Chrome with remote debugging enabled"""
    try:
        # Get Chrome path - this is the default location for Windows
        chrome_path = r"C:\Program Files\Google\Chrome\Application\chrome.exe"
        
        # Check if Chrome exists at the default path
        if not os.path.exists(chrome_path):
            chrome_path = r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe"
            if not os.path.exists(chrome_path):
                return False, "Chrome executable not found in default locations"
        
        # Start Chrome with debugging
        subprocess.Popen([
            chrome_path,
            "--remote-debugging-port=9222",
            "--user-data-dir=C:\\temp\\chrome_debug_profile"
        ])
        print("Chrome started with debugging enabled")
        time.sleep(3)  # Give Chrome time to start
        return True, "Chrome started successfully"
    except Exception as e:
        return False, f"Failed to start Chrome: {str(e)}"

def scrape_kickstarter_from_url(url, output_csv="kickstarter_data.csv", use_existing_browser=True):
    """
    Scrapes project information directly from a Kickstarter URL
    and extracts detailed data about each project
    """
    try:
        print(f"Preparing to scrape: {url}")
        
        # Set up Chrome
        chrome_options = Options()
        
        if use_existing_browser:
            print("Attempting to connect to existing Chrome browser...")
            chrome_options.add_experimental_option("debuggerAddress", "127.0.0.1:9222")
        else:
            print("Setting up new Chrome browser...")
            chrome_options.add_argument("--window-size=1920,1080")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36")
        
        # Setup the Chrome WebDriver
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        
        print(f"Navigating to {url}")
        driver.get(url)
        
        # Add a random delay to simulate human behavior and allow the page to fully load
        delay = 5 + random.random() * 5  # 5-10 seconds
        print(f"Waiting {delay:.2f} seconds for page to load completely...")
        time.sleep(delay)
        
        # Save the HTML content to a file for debugging
        html_content = driver.page_source
        print(f"Retrieved page content ({len(html_content)} bytes)")
        
        with open("kickstarter_page.html", "w", encoding="utf-8") as file:
            file.write(html_content)
        print("HTML content saved to kickstarter_page.html")
        
        # Parse the HTML with BeautifulSoup
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Extract project data
        projects = extract_projects_from_soup(soup)
        
        if projects:
            print(f"Successfully extracted {len(projects)} projects")
            
            # Save the extracted data to JSON for backup
            with open("kickstarter_projects.json", "w", encoding="utf-8") as f:
                json.dump(projects, f, indent=2, ensure_ascii=False)
            print("Project data saved to kickstarter_projects.json")
            
            # Ask if user wants to fetch detailed information
            get_details = input("Do you want to fetch detailed information for each project? (y/n): ")
            if get_details.lower() == 'y':
                projects = get_detailed_project_info(driver, projects)
                
                # Save the detailed data to JSON for backup
                with open("kickstarter_detailed_projects.json", "w", encoding="utf-8") as f:
                    json.dump(projects, f, indent=2, ensure_ascii=False)
            
            # Define the fields we want in the final CSV (in order)
            fields_to_include = [
                'title', 
                'creator',
                'url',
                'funding_info',
                'full_description',
                'funding_goal',
                'backers_count',
                'end_date',
                'creator_bio',
                'location',
                'updates_count',
                'comments_count'
            ]
            
            # Get all fieldnames that actually exist in our data
            all_fieldnames = set()
            for project in projects:
                all_fieldnames.update(project.keys())
            
            # Prioritize our desired fields, then add any other fields that exist
            # (except for the ones we want to exclude)
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
                    # Remove unwanted fields
                    for field in fields_to_exclude:
                        if field in project:
                            del project[field]
                    
                    writer.writerow(project)
                    
            print(f"All project data saved to {output_csv}")
            
        else:
            print("No projects were found on the page.")
        
        # Don't close the browser if we're using an existing session
        if not use_existing_browser:
            driver.quit()
            print("Browser closed.")
        else:
            print("Left browser open (using existing session)")
        
        return projects
    
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

def extract_projects_from_soup(soup):
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
        print(f"Selector '{selector}': {len(cards)} elements found")
        if cards:
            project_cards = cards
            break
    
    if not project_cards:
        # Try a broader approach - look for project card patterns
        print("No project cards found with standard selectors. Trying alternate methods...")
        
        # Look for elements containing typical project card elements
        potential_cards = soup.find_all(['div', 'article'], class_=re.compile(r'card|project', re.I))
        if potential_cards:
            project_cards = potential_cards
            print(f"Found {len(project_cards)} potential cards by pattern matching")
        else:
            # Try finding by link pattern
            links = soup.find_all('a', href=re.compile(r'/projects/'))
            if links:
                parent_elements = set(link.parent.parent for link in links)
                project_cards = list(parent_elements)
                print(f"Found {len(project_cards)} potential cards by project link pattern")
    
    if not project_cards:
        print("Could not find any project cards on the page.")
        return []
    
    print(f"Found {len(project_cards)} project cards")
    projects = []
    
    for i, card in enumerate(project_cards):
        try:
            # Create a dictionary for this project
            project_data = {}
            
            # Find the project URL - look for links containing /projects/
            links = card.find_all('a')
            project_links = [link for link in links if '/projects/' in link.get('href', '')]
            
            if project_links:
                main_link = project_links[0]
                href = main_link.get('href', '')
                if href.startswith('/'):
                    project_data['url'] = f"https://www.kickstarter.com{href}"
                else:
                    project_data['url'] = href
            elif links:
                # Fallback to any link if no project link is found
                href = links[0].get('href', '')
                if href:
                    if href.startswith('/'):
                        project_data['url'] = f"https://www.kickstarter.com{href}"
                    else:
                        project_data['url'] = href
                else:
                    project_data['url'] = "No URL found"
            else:
                project_data['url'] = "No URL found"
            
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
            if title_elem:
                project_data['title'] = title_elem.get_text(strip=True)
            else:
                # Last resort - get any substantial text from the card
                all_text = card.get_text(strip=True)
                if len(all_text) > 10:
                    project_data['title'] = all_text[:50] + ("..." if len(all_text) > 50 else "")
                else:
                    project_data['title'] = "Title not found"
            
            # Extract creator
            creator_candidates = [
                card.select_one('.CreatorName-module_root'),
                card.select_one('.type-14'),
                card.select_one('[data-testid="creator-name"]')
            ]
            
            creator_elem = next((elem for elem in creator_candidates if elem is not None), None)
            if creator_elem:
                project_data['creator'] = creator_elem.get_text(strip=True)
            
            # Extract funding status - look for text with funding keywords
            funding_texts = []
            for elem in card.find_all(text=re.compile(r'fund|back|pledge|success|100%|ended', re.I)):
                if elem.strip():
                    funding_texts.append(elem.strip())
            
            if funding_texts:
                project_data['funding_info'] = " | ".join(funding_texts)
            
            # Add to projects list if we have a valid URL and title
            if project_data['url'] != "No URL found" and project_data['title'] != "Title not found":
                projects.append(project_data)
                print(f"Extracted data for project: {project_data['title']}")
            else:
                print(f"Skipping project with insufficient data: {project_data}")
            
        except Exception as e:
            print(f"Error extracting project data: {e}")
    
    return projects

def get_detailed_project_info(driver, projects):
    """Visit each project page and extract detailed information"""
    print("\nFetching detailed information for each project...")
    
    for i, project in enumerate(projects):
        if 'url' not in project or project['url'] == "No URL found" or not project['url'].startswith('http'):
            print(f"Skipping project {i+1} - No valid URL")
            continue
        
        url = project['url']
        try:
            print(f"[{i+1}/{len(projects)}] Visiting: {url}")
            driver.get(url)
            
            # Random delay to avoid detection
            delay = 3 + random.random() * 3
            print(f"Waiting {delay:.2f} seconds...")
            time.sleep(delay)
            
            # Get the page source
            html_content = driver.page_source
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Extract detailed information
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
                project['full_description'] = desc_elem.get_text(strip=True).replace('\n', ' ').replace('\r', '')
            
            # Funding goal
            goal_candidates = [
                soup.select_one('.money.goal'),
                soup.select_one('.goal'),
                soup.select_one('[data-test-id="funding-goal"]'),
                soup.select_one('.funding-goal')
            ]
            
            goal_elem = next((elem for elem in goal_candidates if elem is not None), None)
            if goal_elem:
                project['funding_goal'] = goal_elem.get_text(strip=True)
            
            # Backers count
            backers_candidates = [
                soup.select_one('.backers-count'),
                soup.select_one('.num-backers'),
                soup.select_one('[data-test-id="backers-count"]'),
                soup.select_one('.project-state-backings')
            ]
            
            backers_elem = next((elem for elem in backers_candidates if elem is not None), None)
            if backers_elem:
                project['backers_count'] = backers_elem.get_text(strip=True)
            
            # Campaign end date
            date_candidates = [
                soup.select_one('.js-campaign-end-date'),
                soup.select_one('.campaign-end-date'),
                soup.select_one('[data-test-id="end-date"]'),
                soup.select_one('.project-timing-info')
            ]
            
            date_elem = next((elem for elem in date_candidates if elem is not None), None)
            if date_elem:
                project['end_date'] = date_elem.get_text(strip=True)
            
            # Creator details
            creator_bio_candidates = [
                soup.select_one('.creator-bio'),
                soup.select_one('.creator-details'),
                soup.select_one('[data-test-id="creator-bio"]'),
                soup.select_one('.bio')
            ]
            
            creator_bio_elem = next((elem for elem in creator_bio_candidates if elem is not None), None)
            if creator_bio_elem:
                project['creator_bio'] = creator_bio_elem.get_text(strip=True).replace('\n', ' ').replace('\r', '')
            
            # Project location
            location_candidates = [
                soup.select_one('.location'),
                soup.select_one('.project-location'),
                soup.select_one('[data-test-id="location"]'),
                soup.select_one('.project-state-location')
            ]
            
            location_elem = next((elem for elem in location_candidates if elem is not None), None)
            if location_elem:
                project['location'] = location_elem.get_text(strip=True)
            
            # Project updates count
            updates_candidates = [
                soup.select_one('.js-updates-count'),
                soup.select_one('.updates-count'),
                soup.select_one('[data-test-id="updates-count"]')
            ]
            
            updates_elem = next((elem for elem in updates_candidates if elem is not None), None)
            if updates_elem:
                project['updates_count'] = updates_elem.get_text(strip=True)
            
            # Comments count
            comments_candidates = [
                soup.select_one('.js-comments-count'),
                soup.select_one('.comments-count'),
                soup.select_one('[data-test-id="comments-count"]')
            ]
            
            comments_elem = next((elem for elem in comments_candidates if elem is not None), None)
            if comments_elem:
                project['comments_count'] = comments_elem.get_text(strip=True)
            
            # Try to find keywords in the full page to extract more info
            if 'full_description' not in project:
                # Look for any paragraph with substantial content
                for p in soup.find_all('p'):
                    text = p.get_text(strip=True)
                    if len(text) > 100:  # Only consider paragraphs with substantial content
                        project['full_description'] = text.replace('\n', ' ').replace('\r', '')
                        print("  Found description in paragraph")
                        break
            
            print(f"Successfully extracted details for: {project.get('title', 'Unknown project')}")
            
        except Exception as e:
            print(f"Error processing {url}: {e}")
        
        # Add random delay between requests
        if i < len(projects) - 1:
            wait_time = 2 + random.random() * 3
            print(f"Waiting {wait_time:.2f} seconds before next project...")
            time.sleep(wait_time)
    
    return projects

if __name__ == "__main__":
    print("Kickstarter Direct URL Scraper")
    print("=============================")
    print("This script scrapes project data directly from a Kickstarter URL")
    
    print("\nOptions:")
    print("1. Connect to existing Chrome session (recommended to avoid CAPTCHA)")
    print("2. Start Chrome with debugging enabled")
    print("3. Use a fresh Chrome instance (may trigger CAPTCHA)")
    
    choice = input("Enter your choice (1-3): ")
    
    use_existing_browser = True
    if choice == '1':
        print("Make sure Chrome is running with remote debugging enabled (--remote-debugging-port=9222)")
        input("Press Enter when ready to continue...")
    elif choice == '2':
        success, message = start_chrome_with_debugging()
        if success:
            print("Chrome started with debugging enabled.")
            print("Please log in to your Kickstarter account if needed")
            input("Press Enter when ready to continue...")
        else:
            print(f"Error starting Chrome: {message}")
            print("Falling back to option 3 (fresh Chrome instance)")
            use_existing_browser = False
    else:
        use_existing_browser = False
        print("Using a fresh Chrome instance (may trigger CAPTCHA)...")
    
    # Ask for the URL to scrape
    default_url = "https://www.kickstarter.com/discover/advanced?category_id=51&raised=1&sort=end_date&seed=2910832&page=1"
    url = input(f"Enter Kickstarter URL to scrape [{default_url}]: ").strip() or default_url
    
    # Ask for the output file name
    output_file = input("Enter the output CSV file name [kickstarter_data.csv]: ").strip() or "kickstarter_data.csv"
    
    # Run the scraper
    projects = scrape_kickstarter_from_url(url, output_file, use_existing_browser)
    
    if projects:
        print(f"\nSuccessfully scraped {len(projects)} projects from {url}")
        print(f"All data has been saved to {output_file}")
    else:
        print(f"\nFailed to scrape projects from {url}") 