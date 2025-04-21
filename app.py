#!/usr/bin/env python3
"""
Cricket Odds API for BetBhai.io - Enhanced for Reliability and Performance
"""

import os
import re
import time
import json
import logging
import threading
import asyncio
import uvicorn
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from fastapi import FastAPI, HTTPException, BackgroundTasks, status, Query, Request, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, RedirectResponse
from pydantic import BaseModel
from fastapi.concurrency import run_in_threadpool
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException
import concurrent.futures

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("cricket_odds_api.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Make data directory
DATA_DIR = os.environ.get('DATA_DIR', 'data')
os.makedirs(DATA_DIR, exist_ok=True)

# Cache and performance settings
CACHE_TIMEOUT = int(os.environ.get('CACHE_TIMEOUT', 1))  # 1 second cache by default
MAX_WORKERS = int(os.environ.get('MAX_WORKERS', 1))  # Limit concurrent browser instances
RATE_LIMIT = int(os.environ.get('RATE_LIMIT', 100))  # Maximum requests per minute
RATE_LIMIT_WINDOW = 60  # seconds

# Initialize FastAPI app with correct route handling
app = FastAPI(
    title="Cricket Odds API",
    description="API for real-time cricket odds from betbhai.io",
    version="2.1.0",
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Data models
class OddItem(BaseModel):
    position: int
    price: str
    volume: Optional[str] = None

class OddsData(BaseModel):
    back: List[OddItem] = []
    lay: List[OddItem] = []

class Match(BaseModel):
    id: str
    timestamp: str
    team1: Optional[str] = None
    team2: Optional[str] = None
    date: Optional[str] = None
    time: Optional[str] = None
    in_play: Optional[bool] = False
    score: Optional[List[str]] = None
    odds: Optional[OddsData] = None

class ScraperStatus(BaseModel):
    status: str
    last_updated: Optional[str] = None
    matches_count: int = 0
    is_running: bool
    error_count: int
    uptime_seconds: int = 0
    changes_since_last_update: int = 0
    cache_hit_ratio: float = 0.0
    memory_usage_mb: float = 0.0

# Global state
DATA_FILE = os.path.join(DATA_DIR, "cricket_odds_latest.json")
ID_MAPPING_FILE = os.path.join(DATA_DIR, "cricket_match_id_mapping.json")
BACKUP_FILE = os.path.join(DATA_DIR, "cricket_odds_backup.json")

scraper_state = {
    "data": {"matches": []},
    "status": "idle",
    "last_updated": None,
    "is_running": False,
    "start_time": None,
    "error_count": 0,
    "changes_since_last_update": 0,
    "id_mapping": {},
    "match_history": {},
    "lock": threading.Lock(),
    "cache_hits": 0,
    "cache_misses": 0,
    "api_requests": 0,
    "rate_limit_counter": {},
}

# Rate limiter middleware
@app.middleware("http")
async def rate_limiter(request: Request, call_next):
    client_ip = request.client.host
    current_time = int(time.time())
    
    # Initialize rate limit counter for this IP if not exists
    if client_ip not in scraper_state["rate_limit_counter"]:
        scraper_state["rate_limit_counter"][client_ip] = []
    
    # Remove outdated timestamps
    scraper_state["rate_limit_counter"][client_ip] = [
        ts for ts in scraper_state["rate_limit_counter"][client_ip] 
        if ts > current_time - RATE_LIMIT_WINDOW
    ]
    
    # Check if rate limit is exceeded
    if len(scraper_state["rate_limit_counter"][client_ip]) >= RATE_LIMIT:
        return JSONResponse(
            status_code=429,
            content={"detail": "Rate limit exceeded. Try again later."}
        )
    
    # Add current timestamp to the list
    scraper_state["rate_limit_counter"][client_ip].append(current_time)
    
    # Increment API request counter
    with scraper_state["lock"]:
        scraper_state["api_requests"] += 1
    
    # Process the request
    response = await call_next(request)
    return response

# Memory usage monitoring
def get_memory_usage():
    """Get current memory usage in MB"""
    try:
        import psutil
        process = psutil.Process(os.getpid())
        memory_info = process.memory_info()
        return memory_info.rss / 1024 / 1024
    except ImportError:
        return 0

class CricketOddsScraper:
    """Enhanced scraper for extracting cricket odds from betbhai.io"""
    
    def __init__(self, url="https://www.betbhai.io/"):
        self.url = url
        self.driver = None
        self.error_count = 0
        self.max_continuous_errors = 10
        self.force_refresh = False
        self.last_full_refresh = time.time()
        self.full_refresh_interval = 60  # Seconds between full page refreshes
        self.browser_pool = None
        self.browser_pool_lock = threading.Lock()
        self.browser_pool_size = MAX_WORKERS
    
    def setup_driver(self):
        """Set up the WebDriver with improved error handling"""
        try:
            # Close existing driver if any
            if self.driver:
                try:
                    self.driver.quit()
                except Exception as e:
                    logger.debug(f"Error closing driver: {str(e)}")
            
            # Configure Chrome options for Render with improved stability
            chrome_options = Options()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--window-size=1280,720")
            chrome_options.add_argument("--disable-extensions")
            chrome_options.add_argument("--disable-infobars")
            chrome_options.add_argument("--disable-notifications")
            chrome_options.add_argument("--disable-popup-blocking")
            chrome_options.add_argument("--blink-settings=imagesEnabled=false")  # Disable images for performance
            
            # Memory settings
            chrome_options.add_argument("--js-flags=--max-old-space-size=256")  # Limit JS memory
            
            # Add user agent to avoid detection
            chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
            
            # Set page load strategy to eager for faster loading
            chrome_options.page_load_strategy = 'eager'
            
            # Create driver with retry logic
            for attempt in range(3):
                try:
                    self.driver = webdriver.Chrome(options=chrome_options)
                    logger.info("Successfully created WebDriver")
                    return True
                except Exception as e:
                    logger.warning(f"Driver creation attempt {attempt+1} failed: {str(e)}")
                    time.sleep(2 ** attempt)  # Exponential backoff
            
            logger.error("All driver creation attempts failed")
            self.error_count += 1
            return False
            
        except Exception as e:
            logger.error(f"Error setting up driver: {str(e)}")
            self.error_count += 1
            return False
    
    def navigate_to_site(self):
        """Navigate to the website with improved error handling"""
        try:
            # Check if driver exists
            if not self.driver:
                logger.warning("Driver not initialized, setting up...")
                if not self.setup_driver():
                    return False
            
            # Navigate with retry logic
            for attempt in range(3):
                try:
                    self.driver.get(self.url)
                    # Wait for the page to load with shorter timeout
                    WebDriverWait(self.driver, 15).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, ".inplay-item-list"))
                    )
                    logger.info("Successfully navigated to the website")
                    self.last_full_refresh = time.time()
                    return True
                except TimeoutException:
                    logger.warning(f"Navigation timeout on attempt {attempt+1}")
                    if attempt == 2:  # Last attempt
                        logger.error("Navigation failed after all retries")
                        self.error_count += a
                        return False
                    time.sleep(2 ** attempt)  # Exponential backoff
                except Exception as e:
                    logger.error(f"Navigation error on attempt {attempt+1}: {str(e)}")
                    if attempt == 2:  # Last attempt
                        self.error_count += 1
                        return False
                    time.sleep(2 ** attempt)  # Exponential backoff
        except Exception as e:
            logger.error(f"Error in navigate_to_site: {str(e)}")
            self.error_count += 1
            return False
    
    def _create_stable_id(self, team1: str, team2: str) -> str:
        """Create a stable ID based on team names"""
        if not team1:
            return "unknown_match"
        
        # Sort team names for consistency
        teams = sorted([team1, team2]) if team2 and team1 != team2 else [team1]
        
        # Normalize team names
        normalized = []
        for team in teams:
            team = "".join(c.lower() if c.isalnum() else '_' for c in team)
            team = re.sub(r'_+', '_', team).strip('_')
            normalized.append(team)
        
        # Join team names with vs
        match_key = "__vs__".join(normalized)
        
        return match_key
    
    def extract_cricket_odds(self):
        """Extract cricket odds data with improved error handling and robustness"""
        matches = []
        
        try:
            # Check if we need a full page refresh
            current_time = time.time()
            if current_time - self.last_full_refresh > self.full_refresh_interval:
                logger.info(f"Performing scheduled full page refresh after {self.full_refresh_interval} seconds")
                if not self.navigate_to_site():
                    # If navigation fails, return empty result and let recovery mechanisms handle it
                    return []
            
            # Force DOM refresh to catch all updates - with error handling
            try:
                self.driver.execute_script("return document.body.innerHTML;")
            except Exception as e:
                logger.warning(f"DOM refresh failed: {str(e)}")
                # Attempt to recover
                if not self.navigate_to_site():
                    return []
            
            # Find cricket sections - with retry for StaleElementReferenceException
            max_retries = 3
            for retry in range(max_retries):
                try:
                    cricket_sections = self.driver.find_elements(By.CSS_SELECTOR, 'ion-list.inplay-item-list')
                    break
                except StaleElementReferenceException:
                    if retry == max_retries - 1:
                        logger.warning("Max retries reached when finding cricket sections")
                        return []
                    logger.debug(f"Stale element when finding cricket sections, retry {retry+1}")
                    time.sleep(0.5)
                except Exception as e:
                    logger.error(f"Error finding cricket sections: {str(e)}")
                    return []
            
            if not cricket_sections:
                logger.warning("No cricket sections found")
                # This could be a legitimate case (no cricket matches) or a page structure change
                # Force a refresh next time
                self.last_full_refresh = 0
                return []
            
            # Process each section
            for section_idx, section in enumerate(cricket_sections):
                try:
                    # Check if this is the cricket section
                    try:
                        header_content = section.find_element(By.CSS_SELECTOR, '.inplay-item-list__header-content')
                        header_text = header_content.text.lower()
                        
                        is_cricket_section = False
                        
                        # Check if header contains "cricket" text
                        if 'cricket' in header_text:
                            is_cricket_section = True
                        else:
                            # Try to find cricket icon
                            try:
                                cricket_icons = section.find_elements(By.CSS_SELECTOR, '.inplay-content__logo-icon--cricket')
                                if cricket_icons:
                                    is_cricket_section = True
                            except Exception:
                                pass
                        
                        if not is_cricket_section:
                            continue  # Not a cricket section
                            
                    except Exception as e:
                        logger.debug(f"Error checking if section {section_idx} is cricket: {str(e)}")
                        continue
                    
                    # Get all match items with retry for stale elements
                    match_items = []
                    for retry in range(max_retries):
                        try:
                            match_items = section.find_elements(By.CSS_SELECTOR, '.inplay-item')
                            break
                        except StaleElementReferenceException:
                            if retry == max_retries - 1:
                                logger.debug(f"Max retries reached when finding match items in section {section_idx}")
                                break
                            time.sleep(0.5)
                        except Exception as e:
                            logger.debug(f"Error finding match items in section {section_idx}: {str(e)}")
                            break
                    
                    # Process each match
                    for item_idx, item in enumerate(match_items):
                        try:
                            # Extract team names
                            team1 = ""
                            team2 = ""
                            
                            # Try to extract team names with retry for stale elements
                            for retry in range(max_retries):
                                try:
                                    player_elems = item.find_elements(By.CSS_SELECTOR, '.inplay-item__player span')
                                    team1 = player_elems[0].text if len(player_elems) >= 1 else ""
                                    team2 = player_elems[1].text if len(player_elems) > 1 else ""
                                    break
                                except StaleElementReferenceException:
                                    if retry == max_retries - 1:
                                        logger.debug(f"Max retries reached when extracting team names for match {item_idx}")
                                        break
                                    time.sleep(0.5)
                                except Exception as e:
                                    logger.debug(f"Error extracting team names for match {item_idx}: {str(e)}")
                                    break
                            
                            # Skip if we couldn't get team names
                            if not team1:
                                continue
                            
                            # Create a stable ID
                            stable_id = self._create_stable_id(team1, team2)
                            
                            # Initialize match data
                            match_data = {
                                'id': f"match_{stable_id}",
                                'timestamp': datetime.now().isoformat(),
                                'team1': team1,
                                'team2': team2
                            }
                            
                            # Extract date and time
                            try:
                                date_elems = item.find_elements(By.CSS_SELECTOR, '.date-content .inPlayDate-content__date')
                                time_elems = item.find_elements(By.CSS_SELECTOR, '.date-content .inPlayDate-content__time')
                                
                                if date_elems and time_elems:
                                    match_data['date'] = date_elems[0].text
                                    match_data['time'] = time_elems[0].text
                            except Exception as e:
                                logger.debug(f"Error extracting date/time for match {item_idx}: {str(e)}")
                            
                            # Extract current score
                            try:
                                score_elem = item.find_elements(By.CSS_SELECTOR, '.score-content:not(.empty)')
                                if score_elem:
                                    score_spans = score_elem[0].find_elements(By.TAG_NAME, 'span')
                                    if score_spans:
                                        match_data['score'] = [span.text for span in score_spans]
                                        match_data['in_play'] = True
                                else:
                                    match_data['in_play'] = False
                            except Exception as e:
                                logger.debug(f"Error extracting score for match {item_idx}: {str(e)}")
                                match_data['in_play'] = False
                            
                            # Extract odds
                            odds = {'back': [], 'lay': []}
                            
                            # Back odds
                            try:
                                back_buttons = item.find_elements(By.CSS_SELECTOR, '.odd-button.back-color')
                                for i, button in enumerate(back_buttons):
                                    try:
                                        price_elem = button.find_elements(By.CSS_SELECTOR, '.odd-button__price')
                                        volume_elem = button.find_elements(By.CSS_SELECTOR, '.odd-button__volume')
                                        
                                        if price_elem and price_elem[0].text and price_elem[0].text != '-':
                                            odds['back'].append({
                                                'position': i,
                                                'price': price_elem[0].text,
                                                'volume': volume_elem[0].text if volume_elem else None
                                            })
                                    except StaleElementReferenceException:
                                        continue
                                    except Exception as e:
                                        logger.debug(f"Error extracting back odd {i} for match {item_idx}: {str(e)}")
                            except Exception as e:
                                logger.debug(f"Error extracting back odds for match {item_idx}: {str(e)}")
                            
                            # Lay odds
                            try:
                                lay_buttons = item.find_elements(By.CSS_SELECTOR, '.odd-button.lay-color')
                                for i, button in enumerate(lay_buttons):
                                    try:
                                        price_elem = button.find_elements(By.CSS_SELECTOR, '.odd-button__price')
                                        volume_elem = button.find_elements(By.CSS_SELECTOR, '.odd-button__volume')
                                        
                                        if price_elem and price_elem[0].text and price_elem[0].text != '-':
                                            odds['lay'].append({
                                                'position': i,
                                                'price': price_elem[0].text,
                                                'volume': volume_elem[0].text if volume_elem else None
                                            })
                                    except StaleElementReferenceException:
                                        continue
                                    except Exception as e:
                                        logger.debug(f"Error extracting lay odd {i} for match {item_idx}: {str(e)}")
                            except Exception as e:
                                logger.debug(f"Error extracting lay odds for match {item_idx}: {str(e)}")
                            
                            match_data['odds'] = odds
                            matches.append(match_data)
                        except Exception as e:
                            logger.debug(f"Error processing match {item_idx}: {str(e)}")
                            continue
                except Exception as e:
                    logger.debug(f"Error processing section {section_idx}: {str(e)}")
                    continue
            
            if matches:
                logger.info(f"Extracted {len(matches)} cricket matches")
                self.error_count = 0
            else:
                # If no matches found but no errors, it might be legitimate (no cricket matches available)
                # or a page structure change. Either way, force a refresh
                logger.warning("No cricket matches found, will refresh page on next attempt")
                self.last_full_refresh = 0
                self.error_count += 1
            
            return matches
            
        except Exception as e:
            logger.error(f"Error extracting cricket odds: {str(e)}")
            self.error_count += 1
            return []
    
    def _has_odds_changed(self, old_match, new_match):
        """Compare odds between old and new match data to detect changes"""
        # Check if score changed
        if old_match.get("score") != new_match.get("score"):
            return True
            
        # Check if in_play status changed
        if old_match.get("in_play") != new_match.get("in_play"):
            return True
        
        # Check for odds changes
        old_odds = old_match.get("odds", {})
        new_odds = new_match.get("odds", {})
        
        # Compare back odds
        old_back = old_odds.get("back", [])
        new_back = new_odds.get("back", [])
        
        if len(old_back) != len(new_back):
            return True
            
        for i, (old_odd, new_odd) in enumerate(zip(old_back, new_back)):
            if old_odd.get("price") != new_odd.get("price"):
                return True
            if old_odd.get("volume") != new_odd.get("volume"):
                return True
        
        # Compare lay odds
        old_lay = old_odds.get("lay", [])
        new_lay = new_odds.get("lay", [])
        
        if len(old_lay) != len(new_lay):
            return True
            
        for i, (old_odd, new_odd) in enumerate(zip(old_lay, new_lay)):
            if old_odd.get("price") != new_odd.get("price"):
                return True
            if old_odd.get("volume") != new_odd.get("volume"):
                return True
        
        return False
    
    def update_global_state(self, new_matches):
        """Update the global state with new matches data with improved change tracking"""
        try:
            changes_made = 0
            current_time = datetime.now().isoformat()
            
            with scraper_state["lock"]:
                # Create output data structure
                output_data = {
                    'timestamp': current_time,
                    'updated': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    'matches': new_matches
                }
                
                # More sophisticated change detection
                old_matches = scraper_state["data"].get("matches", [])
                old_match_ids = {m.get("id"): m for m in old_matches}
                new_match_ids = {m.get("id"): m for m in new_matches}
                
                # Check for added or removed matches
                added_matches = set(new_match_ids.keys()) - set(old_match_ids.keys())
                removed_matches = set(old_match_ids.keys()) - set(new_match_ids.keys())
                initial_changes = len(added_matches) + len(removed_matches)
                changes_made += initial_changes
                
                # Check for updated odds in existing matches
                for match_id in set(old_match_ids.keys()) & set(new_match_ids.keys()):
                    old_match = old_match_ids[match_id]
                    new_match = new_match_ids[match_id]
                    
                    # Detailed check for specific changes
                    if self._has_odds_changed(old_match, new_match):
                        changes_made += 1
                
                # Update global state
                scraper_state["data"] = output_data
                scraper_state["last_updated"] = current_time
                scraper_state["status"] = "running"
                scraper_state["changes_since_last_update"] = changes_made
                
                # Save data to file with more frequent updates
                last_saved = getattr(self, 'last_saved', None)
                now = datetime.now()
                
                # Save data if changes were made or every 5 seconds
                if (changes_made > 0 or 
                    last_saved is None or 
                    (now - last_saved).total_seconds() > 5):
                    self._save_data_files(output_data)
                    self.last_saved = now
                    logger.info(f"Data saved to disk{' with changes' if changes_made > 0 else ''}")
                
                # Always log when we extract data, regardless of changes
                timestamp = now.strftime("%H:%M:%S")
                logger.info(f"Data updated at {timestamp} with {len(new_matches)} matches" + 
                           (f" ({changes_made} changes)" if changes_made > 0 else ""))
                return True
        except Exception as e:
            logger.error(f"Error updating global state: {str(e)}")
            self.error_count += 1
            return False
    
    def _save_data_files(self, output_data):
        """Save data to files with backup mechanism"""
        try:
            # First, create a backup of the current data
            try:
                if os.path.exists(DATA_FILE):
                    with open(DATA_FILE, 'r', encoding='utf-8') as f:
                        current_data = json.load(f)
                    
                    # Save backup
                    with open(BACKUP_FILE, 'w', encoding='utf-8') as f:
                        json.dump(current_data, f, ensure_ascii=False)
            except Exception as e:
                logger.warning(f"Error creating backup: {str(e)}")
            
            # Save the main data file
            with open(DATA_FILE, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, ensure_ascii=False)
            
            logger.info("Data saved to disk")
            return True
        except Exception as e:
            logger.error(f"Error saving data file: {str(e)}")
            return False
    
    def run(self, interval=1):
        """Run the scraper with enhanced reliability and performance monitoring"""
        with scraper_state["lock"]:
            scraper_state["is_running"] = True
            scraper_state["start_time"] = datetime.now()
            scraper_state["status"] = "starting"
        
        logger.info(f"Starting cricket odds scraper with {interval} second interval")
        
        # Track successful extraction stats
        last_successful_extraction = None
        page_refresh_interval = 60  # Force full page refresh every 60 seconds
        
        if not self.setup_driver():
            logger.error("Failed to set up WebDriver. Exiting.")
            with scraper_state["lock"]:
                scraper_state["is_running"] = False
                scraper_state["status"] = "failed"
            return
        
        try:
            # Navigate to the site initially
            if not self.navigate_to_site():
                logger.error("Failed to navigate to the website. Exiting.")
                with scraper_state["lock"]:
                    scraper_state["is_running"] = False
                    scraper_state["status"] = "failed"
                return
            
            # Recover data from backup if available and no current data
            self._recover_from_backup_if_needed()
            
            # Update status to running
            with scraper_state["lock"]:
                scraper_state["status"] = "running"
            
            # Create a counter for tracking iterations
            iteration_counter = 0
            health_check_interval = 20  # Health check every 20 iterations
            
            while scraper_state["is_running"]:
                try:
                    start_time = time.time()
                    iteration_counter += 1
                    
                    # Log heartbeat and perform health check
                    if iteration_counter % health_check_interval == 0:
                        logger.info(f"Scraper heartbeat: iteration {iteration_counter}")
                        self._perform_health_check()
                    
                    # Check if we need to do a full page refresh
                    current_time = time.time()
                    if last_successful_extraction and (current_time - last_successful_extraction) > page_refresh_interval:
                        logger.info(f"Forcing page refresh after {page_refresh_interval} seconds without updates")
                        self.navigate_to_site()
                    
                    # Extract and update data
                    matches = self.extract_cricket_odds()
                    
                    if matches:
                        self.update_global_state(matches)
                        last_successful_extraction = time.time()
                    
                    # Update error count
                    with scraper_state["lock"]:
                        scraper_state["error_count"] = self.error_count
                    
                    # Always update last_updated timestamp to show we're alive
                    with scraper_state["lock"]:
                        scraper_state["last_updated"] = datetime.now().isoformat()
                    
                    # Ensure we keep to the interval exactly
                    elapsed = time.time() - start_time
                    sleep_time = max(0, interval - elapsed)
                    
                    if iteration_counter % 30 == 0:
                        logger.info(f"Iteration time: {elapsed:.3f}s, sleeping for {sleep_time:.3f}s")
                    
                    # Memory usage monitoring
                    if iteration_counter % 60 == 0:  # Check memory usage every minute
                        memory_usage = get_memory_usage()
                        logger.info(f"Memory usage: {memory_usage:.2f} MB")
                    
                    if sleep_time > 0:
                        time.sleep(sleep_time)
                        
                except Exception as e:
                    logger.error(f"Error in scraper loop: {str(e)}")
                    time.sleep(1)  # Short recovery sleep
                    
                    # Check if we need to reset the driver
                    if self.error_count > self.max_continuous_errors:
                        logger.warning("Too many errors, resetting driver")
                        if not self.setup_driver() or not self.navigate_to_site():
                            logger.error("Driver reset failed")
                        else:
                            self.error_count = 0
                    
        except Exception as e:
            logger.error(f"Unexpected error in scraper: {str(e)}")
        finally:
            # Clean up
            try:
                if self.driver:
                    self.driver.quit()
            except:
                pass
            
            with scraper_state["lock"]:
                scraper_state["is_running"] = False
                scraper_state["status"] = "stopped"
    
    def _perform_health_check(self):
        """Perform health check and maintenance tasks"""
        try:
            # Check if browser is responsive
            if self.driver:
                try:
                    # Quick JS execution to verify browser is responsive
                    self.driver.execute_script("return navigator.userAgent")
                    logger.debug("Browser is responsive")
                except Exception as e:
                    logger.warning(f"Browser health check failed: {str(e)}")
                    logger.info("Reinitializing browser")
                    self.setup_driver()
                    self.navigate_to_site()
            
            # Garbage collection to free memory
            import gc
            gc.collect()
            
            # Clean up rate limit data
            current_time = int(time.time())
            cutoff_time = current_time - RATE_LIMIT_WINDOW
            
            with scraper_state["lock"]:
                # Remove old rate limit entries
                for ip in list(scraper_state["rate_limit_counter"].keys()):
                    scraper_state["rate_limit_counter"][ip] = [
                        ts for ts in scraper_state["rate_limit_counter"][ip] 
                        if ts > cutoff_time
                    ]
                    
                    # Remove empty entries
                    if not scraper_state["rate_limit_counter"][ip]:
                        del scraper_state["rate_limit_counter"][ip]
            
            logger.debug("Health check completed")
            
        except Exception as e:
            logger.error(f"Error in health check: {str(e)}")
    
    def _recover_from_backup_if_needed(self):
        """Recover data from backup if needed"""
        try:
            with scraper_state["lock"]:
                # Check if we have current data
                if not scraper_state["data"].get("matches"):
                    # Try to load from main data file first
                    if os.path.exists(DATA_FILE):
                        try:
                            with open(DATA_FILE, 'r', encoding='utf-8') as f:
                                data = json.load(f)
                                if data.get("matches"):
                                    scraper_state["data"] = data
                                    logger.info(f"Recovered {len(data.get('matches', []))} matches from data file")
                                    return
                        except Exception as e:
                            logger.warning(f"Error loading data file: {str(e)}")
                    
                    # Try backup file
                    if os.path.exists(BACKUP_FILE):
                        try:
                            with open(BACKUP_FILE, 'r', encoding='utf-8') as f:
                                data = json.load(f)
                                if data.get("matches"):
                                    scraper_state["data"] = data
                                    logger.info(f"Recovered {len(data.get('matches', []))} matches from backup file")
                                    return
                        except Exception as e:
                            logger.warning(f"Error loading backup file: {str(e)}")
        except Exception as e:
            logger.error(f"Error in recovery: {str(e)}")

# Start the scraper in a background thread
def start_scraper_thread():
    if not scraper_state["is_running"]:
        scraper = CricketOddsScraper()
        thread = threading.Thread(target=scraper.run, args=(1,), daemon=True)
        thread.start()
        logger.info("Scraper thread started with 1-second update interval")
        return True
    else:
        return False

# On startup
@app.on_event("startup")
async def startup_event():
    """Start the application and initialize the scraper"""
    # Initialize scraper state
    scraper_state["start_time"] = datetime.now()
    
    # Load existing data from file if available
    try:
        if os.path.exists(DATA_FILE):
            with open(DATA_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if data and data.get("matches"):
                    scraper_state["data"] = data
                    logger.info(f"Loaded {len(data.get('matches', []))} matches from existing data file")
    except Exception as e:
        logger.warning(f"Error loading existing data: {str(e)}")
    
    # Start the scraper automatically
    if start_scraper_thread():
        logger.info("API started and scraper initialized successfully")
    else:
        logger.warning("API started but scraper was already running")
    
    # Schedule a maintenance task to check scraper health
    background_tasks = BackgroundTasks()
    background_tasks.add_task(monitor_scraper_health)

async def monitor_scraper_health():
    """Monitor scraper health and restart if needed"""
    while True:
        try:
            # Check if updates are happening
            with scraper_state["lock"]:
                last_updated = scraper_state.get("last_updated")
                is_running = scraper_state.get("is_running", False)
            
            if last_updated:
                last_updated_time = datetime.fromisoformat(last_updated)
                current_time = datetime.now()
                
                # If no updates for more than 30 seconds, restart scraper
                if (current_time - last_updated_time).total_seconds() > 30:
                    logger.warning("No updates for 30+ seconds. Restarting scraper.")
                    
                    # Stop the scraper if it's running
                    with scraper_state["lock"]:
                        scraper_state["is_running"] = False
                    
                    # Wait a moment for cleanup
                    await asyncio.sleep(5)
                    
                    # Start a new scraper
                    start_scraper_thread()
            
            # Check if scraper is marked as running but not actually updating
            elif not is_running:
                logger.warning("Scraper not running. Starting it.")
                start_scraper_thread()
                
            # Check again after 15 seconds
            await asyncio.sleep(15)
            
        except Exception as e:
            logger.error(f"Error in scraper health monitor: {e}")
            await asyncio.sleep(10)

# Cache management
def should_use_cache(request: Request):
    """Determine if cache should be used based on request headers"""
    # Check cache control headers
    cache_control = request.headers.get("Cache-Control", "")
    if "no-cache" in cache_control:
        return False
    
    # Default to using cache
    return True

# API Endpoints

@app.get("/", tags=["Root"], include_in_schema=True)
async def root():
    """Root endpoint with API information"""
    # This is the fixed root endpoint that will handle GET /
    return {
        "name": "Cricket Odds API",
        "version": "2.1.0",
        "description": "API for real-time cricket odds from betbhai.io",
        "endpoints": [
            {"path": "/matches", "description": "Get all cricket matches"},
            {"path": "/matches/{match_id}", "description": "Get a specific match by ID"},
            {"path": "/status", "description": "Get the scraper status"},
            {"path": "/refresh", "description": "Force a refresh of the data"}
        ],
        "status": "running" if scraper_state["is_running"] else "stopped"
    }

@app.get("/matches", response_model=List[Match], tags=["Matches"])
async def get_matches(
    request: Request,
    team: Optional[str] = Query(None, description="Filter by team name"),
    in_play: Optional[bool] = Query(None, description="Filter by in-play status"),
    use_cache: bool = Depends(should_use_cache)
):
    """Get all cricket matches with optional filtering and caching"""
    # Cache management
    if use_cache:
        with scraper_state["lock"]:
            scraper_state["cache_hits"] += 1
    else:
        with scraper_state["lock"]:
            scraper_state["cache_misses"] += 1
    
    with scraper_state["lock"]:
        matches = scraper_state["data"].get("matches", [])
        last_updated = scraper_state["last_updated"]
    
    # Log every API request with timestamp for debugging
    logger.debug(f"GET /matches request at {datetime.now().strftime('%H:%M:%S')} - last data update: {last_updated}")
    
    # Apply filters if provided
    if team:
        team_lower = team.lower()
        matches = [
            m for m in matches 
            if (m.get("team1", "").lower().find(team_lower) != -1 or 
                m.get("team2", "").lower().find(team_lower) != -1)
        ]
    
    if in_play is not None:
        matches = [m for m in matches if m.get("in_play") == in_play]
    
    # Set cache control headers
    response = JSONResponse(content=matches)
    response.headers["Cache-Control"] = f"max-age={CACHE_TIMEOUT}"
    response.headers["Last-Modified"] = last_updated or datetime.now().isoformat()
    
    return response

@app.get("/matches/{match_id}", tags=["Matches"])
async def get_match(
    match_id: str,
    request: Request,
    use_cache: bool = Depends(should_use_cache)
):
    """Get a specific cricket match by ID with caching"""
    # Cache management
    if use_cache:
        with scraper_state["lock"]:
            scraper_state["cache_hits"] += 1
    else:
        with scraper_state["lock"]:
            scraper_state["cache_misses"] += 1
    
    with scraper_state["lock"]:
        matches = scraper_state["data"].get("matches", [])
        last_updated = scraper_state["last_updated"]
    
    for match in matches:
        if match.get("id") == match_id:
            # Set cache control headers
            response = JSONResponse(content=match)
            response.headers["Cache-Control"] = f"max-age={CACHE_TIMEOUT}"
            response.headers["Last-Modified"] = last_updated or datetime.now().isoformat()
            return response
    
    # Match not found
    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"Match with ID {match_id} not found"
    )

@app.get("/status", response_model=ScraperStatus, tags=["System"])
async def get_status():
    """Get the current status of the scraper with enhanced metrics"""
    with scraper_state["lock"]:
        uptime = (datetime.now() - scraper_state["start_time"]).total_seconds() if scraper_state["start_time"] else 0
        
        # Calculate cache hit ratio
        total_requests = scraper_state["cache_hits"] + scraper_state["cache_misses"]
        cache_hit_ratio = scraper_state["cache_hits"] / total_requests if total_requests > 0 else 0
        
        # Get memory usage
        memory_usage = get_memory_usage()
        
        return {
            "status": scraper_state["status"],
            "last_updated": scraper_state["last_updated"],
            "matches_count": len(scraper_state["data"].get("matches", [])),
            "is_running": scraper_state["is_running"],
            "error_count": scraper_state["error_count"],
            "uptime_seconds": int(uptime),
            "changes_since_last_update": scraper_state.get("changes_since_last_update", 0),
            "cache_hit_ratio": cache_hit_ratio,
            "memory_usage_mb": memory_usage
        }

@app.post("/refresh", tags=["System"])
async def force_refresh():
    """Force a refresh of the cricket odds data"""
    if not scraper_state["is_running"]:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"detail": "Scraper is not running. Start it first."}
        )
    
    # Set the force refresh flag
    with scraper_state["lock"]:
        scraper_state["status"] = "refreshing"
        scraper_state["force_refresh"] = True
    
    return {"message": "Refresh requested successfully"}

@app.post("/start", tags=["System"])
async def start_scraper(background_tasks: BackgroundTasks):
    """Start the cricket odds scraper"""
    if scraper_state["is_running"]:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"detail": "Scraper is already running"}
        )
    
    # Start the scraper in a background thread
    background_tasks.add_task(start_scraper_thread)
    
    return {"message": "Scraper starting..."}

@app.post("/stop", tags=["System"])
async def stop_scraper():
    """Stop the cricket odds scraper"""
    if not scraper_state["is_running"]:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"detail": "Scraper is not running"}
        )
    
    # Stop the scraper
    with scraper_state["lock"]:
        scraper_state["is_running"] = False
        scraper_state["status"] = "stopping"
    
    return {"message": "Scraper shutdown initiated"}

# Health check endpoint for monitoring
@app.get("/health", tags=["System"])
async def health_check():
    """Health check endpoint for monitoring"""
    # Check if scraper is running and recently updated
    with scraper_state["lock"]:
        is_running = scraper_state["is_running"]
        last_updated = scraper_state["last_updated"]
    
    if not is_running:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"status": "unhealthy", "reason": "Scraper not running"}
        )
    
    if last_updated:
        last_updated_time = datetime.fromisoformat(last_updated)
        current_time = datetime.now()
        
        # If no updates for more than 60 seconds, consider unhealthy
        if (current_time - last_updated_time).total_seconds() > 60:
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content={"status": "unhealthy", "reason": "Scraper not updating"}
            )
    
    return {"status": "healthy"}

# On shutdown
@app.on_event("shutdown")
async def shutdown_event():
    """Shutdown the application and stop the scraper"""
    # Stop the scraper if running
    with scraper_state["lock"]:
        scraper_state["is_running"] = False
        logger.info("API shutting down, stopping scraper")

if __name__ == "__main__":
    # Use the PORT environment variable provided by Render
    port = int(os.environ.get("PORT", 10000))
    
    # Start the uvicorn server with improved settings
    uvicorn.run(
        "app:app", 
        host="0.0.0.0", 
        port=port, 
        reload=False,
        workers=1,  # Single worker to avoid multiple scrapers
        timeout_keep_alive=120,  # Longer keep-alive
        log_level="info"
    )
