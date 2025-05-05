#!/usr/bin/env python3
"""
Cricket Odds API for BetBhai.io - Fixed for Render with fetch-quit-wait-repeat cycle
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
from datetime import datetime
from fastapi import FastAPI, HTTPException, BackgroundTasks, status, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, RedirectResponse
from pydantic import BaseModel
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException

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

# Initialize FastAPI app with correct route handling
app = FastAPI(
    title="Cricket Odds API",
    description="API for real-time cricket odds from betbhai.io",
    version="2.0.2",
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

# Global state
DATA_FILE = os.path.join(DATA_DIR, "cricket_odds_latest.json")
ID_MAPPING_FILE = os.path.join(DATA_DIR, "cricket_match_id_mapping.json")
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
    "lock": threading.Lock()
}

class CricketOddsScraper:
    """Scraper for extracting cricket odds from betbhai.io - with fetch-quit-wait-repeat cycle"""
    def __init__(self, url="https://www.betbhai.io/"):
        self.url = url
        self.driver = None
        self.error_count = 0
        self.max_continuous_errors = 10
        self.force_refresh = False

    def setup_driver(self):
        """Set up the WebDriver - simplified for Render compatibility"""
        try:
            # Close existing driver if any
            if self.driver:
                try:
                    self.driver.quit()
                except:
                    pass
            # Configure Chrome options for Render
            chrome_options = Options()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--window-size=1280,720")
            chrome_options.add_argument("--disable-extensions")
            # Add user agent to avoid detection
            chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
            # Try with system-wide ChromeDriver (simplest approach for Render)
            self.driver = webdriver.Chrome(options=chrome_options)
            logger.info("Successfully created WebDriver with system-wide ChromeDriver")
            return True
        except Exception as e:
            logger.error(f"Error setting up driver: {str(e)}")
            self.error_count += 1
            return False

    def navigate_to_site(self):
        """Navigate to the website and wait for it to load"""
        try:
            self.driver.get(self.url)
            # Wait for the page to load
            WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".inplay-item-list"))
            )
            logger.info("Successfully navigated to the website")
            return True
        except Exception as e:
            logger.error(f"Error navigating to site: {str(e)}")
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
        """Extract cricket odds data from the loaded page with improved data refresh"""
        matches = []
        try:
            # Always force DOM refresh to catch all updates
            self.driver.execute_script("return document.body.innerHTML;")
            # Find cricket sections
            cricket_sections = self.driver.find_elements(By.CSS_SELECTOR, 'ion-list.inplay-item-list')
            for section in cricket_sections:
                try:
                    # Check if this is the cricket section by looking for the text or icon
                    header_content = section.find_element(By.CSS_SELECTOR, '.inplay-item-list__header-content')
                    header_text = header_content.text.lower()
                    # Check if header contains "cricket" text
                    if 'cricket' not in header_text:
                        # Also try to find cricket icon
                        try:
                            cricket_icons = section.find_elements(By.CSS_SELECTOR, '.inplay-content__logo-icon--cricket')
                            if not cricket_icons:
                                continue  # Not a cricket section
                        except:
                            continue  # Not a cricket section
                    # Get all match items in this section
                    match_items = section.find_elements(By.CSS_SELECTOR, '.inplay-item')
                    for item in match_items:
                        try:
                            # Extract team names with better error handling
                            try:
                                player_elems = item.find_elements(By.CSS_SELECTOR, '.inplay-item__player span')
                                team1 = player_elems[0].text if len(player_elems) >= 1 else ""
                                team2 = player_elems[1].text if len(player_elems) > 1 else ""
                            except StaleElementReferenceException:
                                # If we hit stale element, force a refresh and retry immediately
                                logger.debug("Stale element encountered, refreshing reference")
                                self.driver.execute_script("return document.body.innerHTML;")
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
                            date_elems = item.find_elements(By.CSS_SELECTOR, '.date-content .inPlayDate-content__date')
                            time_elems = item.find_elements(By.CSS_SELECTOR, '.date-content .inPlayDate-content__time')
                            if date_elems and time_elems:
                                match_data['date'] = date_elems[0].text
                                match_data['time'] = time_elems[0].text
                            # Extract current score
                            score_elem = item.find_elements(By.CSS_SELECTOR, '.score-content:not(.empty)')
                            if score_elem:
                                score_spans = score_elem[0].find_elements(By.TAG_NAME, 'span')
                                if score_spans:
                                    match_data['score'] = [span.text for span in score_spans]
                                    match_data['in_play'] = True
                            else:
                                match_data['in_play'] = False
                            # Extract odds
                            odds = {'back': [], 'lay': []}
                            # Back odds
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
                            # Lay odds
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
                            match_data['odds'] = odds
                            matches.append(match_data)
                        except Exception as e:
                            logger.debug(f"Error processing match: {str(e)}")
                            continue
                except Exception as e:
                    logger.debug(f"Error processing section: {str(e)}")
                    continue
            if matches:
                logger.info(f"Extracted {len(matches)} cricket matches")
                self.error_count = 0
            else:
                # If no matches found, force a page refresh to recover
                logger.warning("No cricket matches found, refreshing page")
                self.navigate_to_site()
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
                # Save data if changes were made or every 5 seconds (instead of 30)
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
        """Save data to files"""
        try:
            # Save the main data file
            with open(DATA_FILE, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, ensure_ascii=False)
            logger.info("Data saved to disk")
            return True
        except Exception as e:
            logger.error(f"Error saving data file: {str(e)}")
            return False

    def run(self, interval=10):  # Default interval set to 10 seconds
        """Run the scraper with a fetch-quit-wait-repeat approach for better stability"""
        with scraper_state["lock"]:
            scraper_state["is_running"] = True
            scraper_state["start_time"] = datetime.now()
            scraper_state["status"] = "starting"
        logger.info(f"Starting cricket odds scraper with {interval} second interval - fetch-quit-wait loop mode")
        # Track successful extraction stats
        last_successful_extraction = None
        iteration_counter = 0
        # Update status to running
        with scraper_state["lock"]:
            scraper_state["status"] = "running"
        while scraper_state["is_running"]:
            try:
                start_time = time.time()
                iteration_counter += 1
                # Log heartbeat every 10 iterations
                if iteration_counter % 10 == 0:
                    logger.info(f"Scraper heartbeat: iteration {iteration_counter}")
                # Step 1: Set up a fresh driver for this iteration
                if not self.setup_driver():
                    logger.error("Failed to set up WebDriver. Skipping this iteration.")
                    self.error_count += 1
                    time.sleep(1)
                    continue
                # Step 2: Navigate to the site
                if not self.navigate_to_site():
                    logger.error("Failed to navigate to the website. Skipping this iteration.")
                    self.error_count += 1
                    try:
                        if self.driver:
                            self.driver.quit()
                    except Exception as e:
                        logger.error(f"Error closing driver: {str(e)}")
                    time.sleep(1)
                    continue
                # Step 3: Extract and update data
                matches = self.extract_cricket_odds()
                # Step 4: Close the browser immediately after extraction
                try:
                    if self.driver:
                        self.driver.quit()
                        self.driver = None
                except Exception as e:
                    logger.error(f"Error closing driver: {str(e)}")
                # Step 5: Process the data (after browser is closed to save resources)
                if matches:
                    self.update_global_state(matches)
                    last_successful_extraction = time.time()
                    self.error_count = 0
                else:
                    self.error_count += 1
                    logger.warning(f"No matches found in iteration {iteration_counter}")
                # Update error count in global state
                with scraper_state["lock"]:
                    scraper_state["error_count"] = self.error_count
                    scraper_state["last_updated"] = datetime.now().isoformat()
                # Step 6: Wait for the next iteration, ensuring proper timing
                elapsed = time.time() - start_time
                sleep_time = max(0, interval - elapsed)
                if iteration_counter % 30 == 0:
                    logger.info(f"Iteration time: {elapsed:.3f}s, sleeping for {sleep_time:.3f}s")
                if sleep_time > 0:
                    time.sleep(sleep_time)
            except Exception as e:
                logger.error(f"Error in scraper loop: {str(e)}")
                # Always make sure to close the browser if an error occurs
                try:
                    if self.driver:
                        self.driver.quit()
                        self.driver = None
                except Exception as e:
                    logger.error(f"Error closing driver during error handling: {str(e)}")
                time.sleep(1)  # Short recovery sleep
                # Reset error counter if too many errors
                if self.error_count > self.max_continuous_errors:
                    logger.warning("Too many errors, resetting error counter")
                    self.error_count = 0
        # Final cleanup
        try:
            if self.driver:
                self.driver.quit()
        except:
            pass
        with scraper_state["lock"]:
            scraper_state["is_running"] = False
            scraper_state["status"] = "stopped"

# Start the scraper in a background thread
def start_scraper_thread():
    if not scraper_state["is_running"]:
        scraper = CricketOddsScraper()
        thread = threading.Thread(target=scraper.run, args=(10,), daemon=True)  # Interval set to 10 seconds
        thread.start()
        logger.info("Scraper thread started with 10-second update interval")
        return True
    else:
        return False

# On startup
@app.on_event("startup")
async def startup_event():
    """Start the application and initialize the scraper"""
    # Initialize scraper state
    scraper_state["start_time"] = datetime.now()
    # Start the scraper automatically with a 10-second interval
    if start_scraper_thread():
        logger.info("API started and scraper initialized successfully")
    else:
        logger.warning("API started but scraper was already running")

# Health monitoring coroutine
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
                    # Start a new scraper with a 10-second interval
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

# API Endpoints
@app.get("/", tags=["Root"], include_in_schema=True)
async def root():
    """Root endpoint with API information"""
    return {
        "name": "Cricket Odds API",
        "version": "2.0.2",
        "description": "API for real-time cricket odds from betbhai.io",
        "endpoints": [
            {"path": "/matches", "description": "Get all cricket matches"},
            {"path": "/matches/{match_id}", "description": "Get a specific match by ID"},
            {"path": "/status", "description": "Get the scraper status"},
            {"path": "/refresh", "description": "Force a refresh of the data"}
        ]
    }

@app.get("/matches", response_model=List[Match], tags=["Matches"])
async def get_matches(
    team: Optional[str] = Query(None, description="Filter by team name"),
    in_play: Optional[bool] = Query(None, description="Filter by in-play status")
):
    """Get all cricket matches with optional filtering"""
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
    return matches

@app.get("/matches/{match_id}", tags=["Matches"])
async def get_match(match_id: str):
    """Get a specific cricket match by ID"""
    with scraper_state["lock"]:
        matches = scraper_state["data"].get("matches", [])
    for match in matches:
        if match.get("id") == match_id:
            return match
    # Match not found
    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"Match with ID {match_id} not found"
    )

@app.get("/status", response_model=ScraperStatus, tags=["System"])
async def get_status():
    """Get the current status of the scraper"""
    with scraper_state["lock"]:
        uptime = (datetime.now() - scraper_state["start_time"]).total_seconds() if scraper_state["start_time"] else 0
        return {
            "status": scraper_state["status"],
            "last_updated": scraper_state["last_updated"],
            "matches_count": len(scraper_state["data"].get("matches", [])),
            "is_running": scraper_state["is_running"],
            "error_count": scraper_state["error_count"],
            "uptime_seconds": int(uptime),
            "changes_since_last_update": scraper_state.get("changes_since_last_update", 0)
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
    # Start the uvicorn server
    uvicorn.run("app:app", host="0.0.0.0", port=port, reload=False)
