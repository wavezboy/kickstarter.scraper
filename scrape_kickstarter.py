import time
import random
import os
import subprocess
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

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

def get_kickstarter_page(use_existing_browser=True):
    url = "https://www.kickstarter.com/discover/advanced?category_id=51&raised=1&sort=end_date&seed=2910832&page=1"
    # url = "https://www.kickstarter.com/projects/493415651/journey-ai-travel-planner?ref=discovery_category_ending_soon&total_hits=29&category_id=51"
    
    try:
        chrome_options = Options()
        
        if use_existing_browser:
            print("Attempting to connect to existing Chrome browser...")
            chrome_options.add_experimental_option("debuggerAddress", "127.0.0.1:9222")
        else:
            print("Setting up new Chrome browser...")
            # Add normal browser configuration
            chrome_options.add_argument("--window-size=1920,1080")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36")
        
        # Setup the Chrome WebDriver
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        
        print(f"Navigating to {url}")
        # Navigate to the page
        driver.get(url)
        
        # Add a random delay to simulate human behavior and allow the page to fully load
        delay = 5 + random.random() * 5  # 5-10 seconds
        print(f"Waiting {delay:.2f} seconds for page to load completely...")
        time.sleep(delay)
        
        # Get the page source
        html_content = driver.page_source
        print(f"Successfully retrieved page content. Length: {len(html_content)} bytes")
        
        # Save the HTML content to a file
        with open("kickstarter_page.html", "w", encoding="utf-8") as file:
            file.write(html_content)
        print("HTML content saved to kickstarter_page.html")
        
        if not use_existing_browser:
            # Close the browser only if we created it
            driver.quit()
            print("Browser closed.")
        else:
            print("Finished using the browser (keeping it open).")
        
        return html_content
        
    except Exception as e:
        print(f"An error occurred: {e}")
        if not use_existing_browser and 'driver' in locals():
            try:
                driver.quit()
            except:
                pass
        return None

if __name__ == "__main__":
    print("Options:")
    print("1. Connect to an existing Chrome session with debugging enabled")
    print("2. Start a new Chrome session with debugging enabled")
    print("3. Use a fresh Chrome instance (may trigger CAPTCHA)")
    
    choice = input("Enter your choice (1-3): ")
    
    if choice == "1":
        print("Make sure Chrome is already running with '--remote-debugging-port=9222'")
        print("If Chrome is not already running with debugging, choose option 2 instead")
        html_content = get_kickstarter_page(use_existing_browser=True)
    elif choice == "2":
        success, message = start_chrome_with_debugging()
        if success:
            print("Chrome has been started. Please log in to your Kickstarter account if needed.")
            input("Press Enter when you're ready to continue...")
            html_content = get_kickstarter_page(use_existing_browser=True)
        else:
            print(f"Error: {message}")
            print("Falling back to option 3 (fresh Chrome instance)")
            html_content = get_kickstarter_page(use_existing_browser=False)
    else:
        print("Using a fresh Chrome instance...")
        html_content = get_kickstarter_page(use_existing_browser=False)
    
    if html_content:
        # Parse the HTML with BeautifulSoup and print some basic info
        soup = BeautifulSoup(html_content, 'html.parser')
        print("\nPage Information:")
        print(f"Title: {soup.title.text if soup.title else 'No title found'}")
        projects = soup.select('.js-react-proj-card')
        print(f"Number of projects found: {len(projects)}") 