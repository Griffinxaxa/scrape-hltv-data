#!/usr/bin/env python3
"""
HLTV Enhanced Scraper

This script scrapes match data with enhanced information including:
- Match dates (from Unix timestamp)
- LAN/Online status
- Tournament information
- All existing player and team statistics

Usage:
    python hltv_enhanced_scraper.py --target_match_id 2385589 --num_matches 3
"""

import json
import csv
import sys
import os
import argparse
import time
import signal
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
from statistics import mean
import cloudscraper
from bs4 import BeautifulSoup
import re

class HLTVEnhancedScraper:
    def __init__(self, target_match_id: int, num_matches: int = 3, output_dir: str = "data/enhanced", snapshot_file: str = None):
        self.target_match_id = target_match_id
        self.num_matches = num_matches
        self.output_dir = output_dir
        self.snapshot_file = snapshot_file
        self.snapshot_data = None
        self.snapshot_index = 0
        self.base_url = "https://www.hltv.org"
        self.results_url = f"{self.base_url}/results"
        
        # Delays for respectful scraping (tuned for detailed-stats workflow)
        self.page_delay = 0.3   # Snapshot lookups happen infrequently
        self.match_delay = 0.05  # Single click per match (match -> detailed stats)
        
        # Timeout handling for stuck matches
        self.match_timeout = 45  # 45 seconds per match max
        
        # Large-scale scraping features
        self.match_counter = 0
        self.pause_file = os.path.join(output_dir, "scraper_pause.flag")
        self.progress_file = os.path.join(output_dir, "scraper_progress.json")
        self.matches_per_season = 1750
        
        # Load snapshot if provided
        if self.snapshot_file:
            self.load_snapshot()
        
        # Load progress if resuming
        self.load_progress()
        self.player_stat_delay = 0.8  # Optimized for snapshot mode
        
        # Create cloudscraper session to handle Cloudflare
        self.session = cloudscraper.create_scraper()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
        
        # Create output directory
        os.makedirs(self.output_dir, exist_ok=True)
    
    def load_snapshot(self):
        """Load match snapshot from JSON file"""
        try:
            if os.path.exists(self.snapshot_file):
                with open(self.snapshot_file, 'r') as f:
                    snapshot = json.load(f)
                    self.snapshot_data = snapshot.get('matches', [])
                    print(f"📸 Snapshot loaded: {len(self.snapshot_data)} match IDs")
                    print(f"🔢 First match: ID {self.snapshot_data[0]['match_id']} - {self.snapshot_data[0]['team1']} vs {self.snapshot_data[0]['team2']}")
                    print(f"🔢 Last match: ID {self.snapshot_data[-1]['match_id']} - {self.snapshot_data[-1]['team1']} vs {self.snapshot_data[-1]['team2']}")
            else:
                print(f"⚠️ Snapshot file not found: {self.snapshot_file}")
                self.snapshot_data = None
        except Exception as e:
            print(f"⚠️ Error loading snapshot: {e}")
            self.snapshot_data = None
    
    def load_progress(self):
        """Load scraping progress from file if resuming"""
        try:
            if os.path.exists(self.progress_file):
                with open(self.progress_file, 'r') as f:
                    progress_data = json.load(f)
                    self.match_counter = progress_data.get('match_counter', 0)
                    if self.snapshot_file:
                        self.snapshot_index = progress_data.get('snapshot_index', 0)
                        print(f"🔄 Resuming snapshot scraping from index {self.snapshot_index} (match #{self.match_counter + 1})")
                    else:
                        print(f"🔄 Resuming scraping from match #{self.match_counter + 1}")
            else:
                self.match_counter = 0
                self.snapshot_index = 0
                print(f"🚀 Starting fresh scraping session")
        except Exception as e:
            print(f"⚠️ Error loading progress: {e}. Starting fresh.")
            self.match_counter = 0
            self.snapshot_index = 0
    
    def save_progress(self):
        """Save current scraping progress"""
        try:
            progress_data = {
                'match_counter': self.match_counter,
                'timestamp': datetime.now().isoformat()
            }
            if self.snapshot_file:
                progress_data['snapshot_index'] = self.snapshot_index
            with open(self.progress_file, 'w') as f:
                json.dump(progress_data, f, indent=2)
        except Exception as e:
            print(f"⚠️ Error saving progress: {e}")
    
    def check_pause_signal(self):
        """Check if pause signal file exists"""
        return os.path.exists(self.pause_file)
    
    def handle_pause(self):
        """Handle pause signal - save progress and exit gracefully"""
        print(f"\n⏸️ Pause signal detected! Saving progress...")
        self.save_progress()
        print(f"💾 Progress saved at match #{self.match_counter}")
        print(f"🔄 To resume, delete the pause file: {self.pause_file}")
        print(f"📊 Current season: {self.get_current_season()}")
        return True
    
    def timeout_handler(self, signum, frame):
        """Handle timeout for stuck matches"""
        raise TimeoutError("Match processing timed out")
    
    def is_best_of_one_or_five(self, match_url: str, soup: Optional[BeautifulSoup] = None) -> bool:
        """Check if the match is Best of 1 or Best of 5 by looking in the match page"""
        try:
            if soup is None:
                soup = self.get_page_content(match_url)
                if not soup:
                    return False
            
            # Look for "Best of 1" or "Best of 5" in elements with class "padding preformatted-text"
            bo_elements = soup.find_all(class_='padding preformatted-text')
            for element in bo_elements:
                element_text = element.get_text()
                if 'Best of 1' in element_text or 'Best of 5' in element_text:
                    return True
            
            return False
        except Exception as e:
            return False

    def process_match_with_timeout(self, match_element, match_number, match_info):
        """Process a single match with timeout protection"""
        def process_match():
            soup = self.get_page_content(match_info['match_url'])
            if not soup:
                return None
            
            # Check if this is a Best of 1 or Best of 5 match first
            if self.is_best_of_one_or_five(match_info['match_url'], soup):
                return None
            
            match_metadata = self.extract_enhanced_data_from_soup(soup, match_info)
            if not match_metadata:
                return None
            
            detailed_stats = self.extract_detailed_stats_from_match_page(soup, match_info)
            if not detailed_stats:
                return None
            
            return {
                'match_metadata': match_metadata,
                'detailed_stats': detailed_stats
            }
        
        # Set up timeout
        old_handler = signal.signal(signal.SIGALRM, self.timeout_handler)
        signal.alarm(self.match_timeout)
        
        try:
            return process_match()
        except TimeoutError:
            return None
        except Exception as e:
            return None
        finally:
            # Restore original handler and cancel alarm
            signal.alarm(0)
            signal.signal(signal.SIGALRM, old_handler)
    
    def get_current_season(self):
        """Calculate current season based on match counter"""
        if self.match_counter == 0:
            return 1
        return min(8, ((self.match_counter - 1) // self.matches_per_season) + 1)
    
    def create_pause_file(self):
        """Create pause signal file for graceful stopping"""
        try:
            with open(self.pause_file, 'w') as f:
                f.write(f"Pause requested at {datetime.now().isoformat()}")
            print(f"✅ Pause file created: {self.pause_file}")
        except Exception as e:
            print(f"❌ Error creating pause file: {e}")
        
    def get_page_content(self, url: str, max_retries: int = 3) -> BeautifulSoup:
        """Get page content using cloudscraper with retry logic"""
        for attempt in range(max_retries):
            try:
                response = self.session.get(url)
                if response.status_code == 429:
                    # Rate limited - wait longer before retry
                    wait_time = (attempt + 1) * 10  # 10, 20, 30 seconds
                    print(f"Rate limited, waiting {wait_time} seconds before retry {attempt + 1}/{max_retries}")
                    time.sleep(wait_time)
                    continue
                response.raise_for_status()
                return BeautifulSoup(response.content, 'html.parser')
            except Exception as e:
                if attempt == max_retries - 1:
                    return None
                wait_time = (attempt + 1) * 5  # 5, 10, 15 seconds
                time.sleep(wait_time)
        return None
    
    def extract_match_id_from_url(self, url: str) -> Optional[int]:
        """Extract match ID from HLTV URL"""
        try:
            # Pattern: /matches/1234567/...
            match = re.search(r'/matches/(\d+)/', url)
            if match:
                return int(match.group(1))
        except:
            pass
        return None
    
    def extract_match_date(self, match_url: str) -> Optional[str]:
        """Extract match date from match page"""
        try:
            soup = self.get_page_content(match_url)
            if not soup:
                return None
            
            # Look for the time element with Unix timestamp
            time_elem = soup.select_one('.time[data-unix]')
            if time_elem:
                unix_timestamp = time_elem.get('data-unix')
                if unix_timestamp:
                    # Convert Unix timestamp to ISO format
                    timestamp = int(unix_timestamp) / 1000  # Convert from milliseconds
                    date_obj = datetime.fromtimestamp(timestamp)
                    return date_obj.isoformat() + "Z"
            
            return None
            
        except Exception as e:
            return None
    
    def extract_tournament(self, match_url: str) -> Optional[str]:
        """Extract tournament name from match page"""
        try:
            soup = self.get_page_content(match_url)
            if not soup:
                return None
            
            # Look for tournament in event text-ellipsis class
            tournament_elem = soup.select_one('.event.text-ellipsis')
            if tournament_elem:
                tournament_name = tournament_elem.get_text().strip()
                if tournament_name:
                    return tournament_name
            
            return None
            
        except Exception as e:
            return None
    
    def extract_event_type(self, match_url: str) -> str:
        """Extract LAN/Online status from match page"""
        try:
            soup = self.get_page_content(match_url)
            if not soup:
                return "unknown"
            
            # Look in the padding preformatted-text class
            preformatted_elem = soup.select_one('.padding.preformatted-text')
            if preformatted_elem:
                text = preformatted_elem.get_text().lower()
                if '(lan)' in text:
                    return "lan"
                elif '(online)' in text:
                    return "online"
            
            return "unknown"
            
        except Exception as e:
            return "unknown"
    
    def extract_head_to_head(self, match_url: str, team1_name: str, team2_name: str, winner: str) -> Dict[str, Optional[int]]:
        """Extract head-to-head map wins from match page"""
        try:
            soup = self.get_page_content(match_url)
            if not soup:
                return {"winner_head2head_freq": None, "loser_head2head_freq": None}
            
            # Look for head-to-head sections - try multiple selectors
            head2head_elements = soup.select('.head-to-head-listing, .head-to-head')
            if not head2head_elements:
                # Try alternative selectors
                head2head_elements = soup.select('.head2head, .head-to-head-stats, .h2h, [class*="head-to-head"]')
            
            if not head2head_elements or len(head2head_elements) < 2:
                return {"winner_head2head_freq": None, "loser_head2head_freq": None}
            
            team1_head2head_freq = None
            team2_head2head_freq = None
            
            # Look for head-to-head data more specifically
            # Try to find the actual head-to-head statistics
            head2head_stats = soup.select('.head-to-head-listing .stats, .head-to-head .stats, .head-to-head-listing [class*="stats"], .head-to-head [class*="stats"]')
            
            if not head2head_stats:
                # Try alternative selectors for head-to-head data
                head2head_stats = soup.select('.head-to-head-listing .score, .head-to-head .score, .head-to-head-listing [class*="score"], .head-to-head [class*="score"]')
            
            if not head2head_stats:
                # Look for any elements that might contain the head-to-head data
                head2head_stats = soup.select('.head-to-head-listing > div, .head-to-head > div')
            
            for element in head2head_stats:
                # Get the full text content
                element_text = element.get_text()
                
                import re
                
                # Look for the pattern where each team has a number followed by "Wins"
                # Format: Team1\n3\nWins\n...Team2\n6\nWins
                team1_wins_match = re.search(rf'{re.escape(team1_name)}\s*(\d+)\s*Wins', element_text, re.IGNORECASE)
                team2_wins_match = re.search(rf'{re.escape(team2_name)}\s*(\d+)\s*Wins', element_text, re.IGNORECASE)
                
                if team1_wins_match and team2_wins_match:
                    team1_head2head_freq = int(team1_wins_match.group(1))
                    team2_head2head_freq = int(team2_wins_match.group(1))
                    break
                elif team1_wins_match:
                    # If only team1 matches, try to find team2 with a different approach
                    team1_head2head_freq = int(team1_wins_match.group(1))
                    
                    # Look for the second team's wins after the first team's data
                    remaining_text = element_text[team1_wins_match.end():]
                    
                    # Try different patterns for team2
                    team2_wins_match = re.search(rf'{re.escape(team2_name)}\s*(\d+)\s*Wins', remaining_text, re.IGNORECASE)
                    if not team2_wins_match:
                        # Try looking for just the number before the team name
                        team2_wins_match = re.search(r'(\d+)\s*Wins\s*{re.escape(team2_name)}', remaining_text, re.IGNORECASE)
                    if not team2_wins_match:
                        # Try looking for the pattern: number, then team name
                        team2_wins_match = re.search(r'(\d+)\s*Wins.*?{re.escape(team2_name)}', remaining_text, re.IGNORECASE)
                    if not team2_wins_match:
                        # Look for any number followed by "Wins" in the remaining text
                        any_wins_match = re.search(r'(\d+)\s*Wins', remaining_text, re.IGNORECASE)
                        if any_wins_match:
                            team2_head2head_freq = int(any_wins_match.group(1))
                            break
                    
                    if team2_wins_match:
                        team2_head2head_freq = int(team2_wins_match.group(1))
                        break
                
                # Fallback: Look for score patterns like "3-6" or "6-3"
                score_match = re.search(r'(\d+)-(\d+)', element_text)
                if score_match:
                    score1 = int(score_match.group(1))
                    score2 = int(score_match.group(2))
                    
                    # Try to determine which team has which score by looking at team names in the element
                    element_html = str(element)
                    
                    # Check if team1 name appears before team2 name in the HTML
                    team1_pos = element_html.lower().find(team1_name.lower())
                    team2_pos = element_html.lower().find(team2_name.lower())
                    
                    if team1_pos != -1 and team2_pos != -1:
                        if team1_pos < team2_pos:
                            # Team1 appears first, so score1 belongs to team1
                            team1_head2head_freq = score1
                            team2_head2head_freq = score2
                        else:
                            # Team2 appears first, so score1 belongs to team2
                            team1_head2head_freq = score2
                            team2_head2head_freq = score1
                    else:
                        # If we can't determine order, assume first score is team1
                        team1_head2head_freq = score1
                        team2_head2head_freq = score2
                    
                    break  # Found the score, no need to continue
            
            # Determine winner and loser head-to-head based on actual winner
            if winner == "team1":
                winner_head2head_freq = team1_head2head_freq
                loser_head2head_freq = team2_head2head_freq
            elif winner == "team2":
                winner_head2head_freq = team2_head2head_freq
                loser_head2head_freq = team1_head2head_freq
            else:
                # If it's a tie or unknown, just use the order
                winner_head2head_freq = team1_head2head_freq
                loser_head2head_freq = team2_head2head_freq
            
            return {
                "winner_head2head_freq": winner_head2head_freq,
                "loser_head2head_freq": loser_head2head_freq
            }
            
        except Exception as e:
            return {"winner_head2head_freq": None, "loser_head2head_freq": None}
    
    def extract_past3_months(self, match_url: str, team1_name: str, team2_name: str, winner: str) -> Dict[str, Optional[float]]:
        """Extract past 3 months win percentage for each team"""
        try:
            response = self.session.get(match_url)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find past matches boxes for both teams
            past_matches_boxes = soup.select('.past-matches-box.text-ellipsis')
            
            # Also try alternative selectors
            if not past_matches_boxes:
                past_matches_boxes = soup.select('.past-matches-box')
            if not past_matches_boxes:
                past_matches_boxes = soup.select('[class*="past-matches"]')
            
            # Find past matches tables which contain the complete match history
            
            team1_wins = 0
            team1_total = 0
            team2_wins = 0
            team2_total = 0
            
            # Let's also try to find the tables which might have more complete data
            past_tables = soup.select('.past-matches-table')
            
            # Since tables 1&3 and 2&4 appear to be duplicates, let's just process the first 2 unique ones
            # Based on the debug output, Table 1 has 19 rows (should be Astralis) and Table 2 has 16 rows (should be GamerLegion)
            
            if len(past_tables) >= 2:
                # Process first table (should be Astralis - 19 rows)
                table1 = past_tables[0]
                rows1 = table1.select('tr')
                
                table1_wins = 0
                table1_total = 0
                
                for row in rows1:
                    score_elem = row.select_one('.past-matches-score')
                    if not score_elem:
                        continue
                    
                    score_text = score_elem.get_text().strip()
                    
                    # Skip if score contains '3' (best of 5)
                    if '3' in score_text:
                        continue
                    
                    # Extract the first number from the score
                    import re
                    score_match = re.search(r'^(\d+)', score_text)
                    if not score_match:
                        continue
                    
                    first_score = int(score_match.group(1))
                    
                    # Count as win if starts with 2, loss if starts with 0 or 1
                    is_win = first_score >= 2
                    
                    table1_total += 1
                    if is_win:
                        table1_wins += 1
                
                team1_total = table1_total
                team1_wins = table1_wins
                
                # Process second table (should be GamerLegion - 16 rows)  
                table2 = past_tables[1]
                rows2 = table2.select('tr')
                
                table2_wins = 0
                table2_total = 0
                
                for row in rows2:
                    score_elem = row.select_one('.past-matches-score')
                    if not score_elem:
                        continue
                    
                    score_text = score_elem.get_text().strip()
                    
                    # Skip if score contains '3' (best of 5)
                    if '3' in score_text:
                        continue
                    
                    # Extract the first number from the score
                    import re
                    score_match = re.search(r'^(\d+)', score_text)
                    if not score_match:
                        continue
                    
                    first_score = int(score_match.group(1))
                    
                    # Count as win if starts with 2, loss if starts with 0 or 1
                    is_win = first_score >= 2
                    
                    table2_total += 1
                    if is_win:
                        table2_wins += 1
                
                team2_total = table2_total
                team2_wins = table2_wins
            
            # Calculate percentages
            team1_percentage = 50.0  # Default if no data
            team2_percentage = 50.0  # Default if no data
            
            if team1_total > 0:
                team1_percentage = round((team1_wins / team1_total) * 100, 2)
            
            if team2_total > 0:
                team2_percentage = round((team2_wins / team2_total) * 100, 2)
            
            # Assign to winner and loser
            if winner == "team1":
                winner_past3 = team1_percentage
                loser_past3 = team2_percentage
            else:  # winner == "team2"
                winner_past3 = team2_percentage
                loser_past3 = team1_percentage
            
            return {
                "winner_past3": winner_past3,
                "loser_past3": loser_past3
            }
            
        except Exception as e:
            return {"winner_past3": 50.0, "loser_past3": 50.0}
    
    def extract_team_ids(self, match_url: str) -> Dict[str, Optional[str]]:
        """Extract team IDs from the match page"""
        try:
            response = self.session.get(match_url)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Look for team links that contain /team/ in the href
            team_links = soup.select('a[href*="/team/"]')
            
            team1_id = None
            team2_id = None
            
            for link in team_links:
                href = link.get('href', '')
                if '/team/' in href:
                    # Extract team ID from URL like /team/4991/astralis
                    import re
                    match = re.search(r'/team/(\d+)/', href)
                    if match:
                        team_id = match.group(1)
                        if team1_id is None:
                            team1_id = team_id
                        elif team2_id is None and team_id != team1_id:
                            team2_id = team_id
                            break
            
            return {"team1_id": team1_id, "team2_id": team2_id}
            
        except Exception as e:
            return {"team1_id": None, "team2_id": None}
    
    def extract_map_winrates(self, team_id: str, team_name: str) -> Dict[str, float]:
        """Extract map win rates for a specific team"""
        try:
            # Format team name for URL (lowercase, replace spaces with dashes)
            team_name_formatted = team_name.lower().replace(' ', '-').replace('.', '')
            stats_url = f"https://www.hltv.org/stats/teams/maps/{team_id}/{team_name_formatted}"
            
            response = self.session.get(stats_url)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find all map pool elements
            map_elements = soup.select('.map-pool-map-name')
            
            # Define all possible maps
            all_maps = ['mirage', 'inferno', 'nuke', 'dust2', 'overpass', 'train', 'ancient', 'cache', 'vertigo', 'anubis', 'cobblestone']
            map_winrates = {}
            
            # Initialize all maps to 50% (default)
            for map_name in all_maps:
                map_winrates[map_name] = 50.0
            
            # Also try alternative selectors
            if not map_elements:
                alt_selectors = ['.map-pool-map', '.map-name', '[class*="map"]']
                for selector in alt_selectors:
                    alt_elements = soup.select(selector)
                    if alt_elements:
                        map_elements = alt_elements[:11]  # Take first 11 as backup
                        break
            
            # Parse the actual map data
            for i, element in enumerate(map_elements):
                # Try to get map name and percentage from the element text
                full_text = element.get_text().strip()
                if not full_text:
                    continue
                
                import re
                # Look for pattern like "mapname - percentage%"
                match = re.match(r'^([a-zA-Z0-9]+)\s*-\s*(\d+(?:\.\d+)?)%', full_text)
                if match:
                    map_name = match.group(1).lower()
                    percentage = float(match.group(2))
                    
                    if map_name in all_maps:
                        map_winrates[map_name] = percentage
                else:
                    # If no percentage in the text, just get the map name
                    map_name = full_text.lower()
                    if map_name in all_maps:
                        pass  # Already initialized to 50%
            
            return map_winrates
            
        except Exception as e:
            # Return default 50% for all maps
            all_maps = ['mirage', 'inferno', 'nuke', 'dust2', 'overpass', 'train', 'ancient', 'cache', 'vertigo', 'anubis', 'cobblestone']
            return {map_name: 50.0 for map_name in all_maps}
    
    def extract_team_map_winrates(self, match_url: str, team1_name: str, team2_name: str, winner: str) -> Dict[str, float]:
        """Extract map win rates for both teams and assign to winner/loser"""
        try:
            # First extract team IDs
            team_ids = self.extract_team_ids(match_url)
            team1_id = team_ids["team1_id"]
            team2_id = team_ids["team2_id"]
            
            if not team1_id or not team2_id:
                # Return default 50% for all maps
                all_maps = ['mirage', 'inferno', 'nuke', 'dust2', 'overpass', 'train', 'ancient', 'cache', 'vertigo', 'anubis', 'cobblestone']
                result = {}
                for map_name in all_maps:
                    result[f"winner_{map_name}"] = 50.0
                    result[f"loser_{map_name}"] = 50.0
                return result
            
            # Extract map winrates for both teams
            team1_winrates = self.extract_map_winrates(team1_id, team1_name)
            team2_winrates = self.extract_map_winrates(team2_id, team2_name)
            
            # Assign to winner/loser based on match result
            result = {}
            all_maps = ['mirage', 'inferno', 'nuke', 'dust2', 'overpass', 'train', 'ancient', 'cache', 'vertigo', 'anubis', 'cobblestone']
            
            for map_name in all_maps:
                if winner == "team1":
                    result[f"winner_{map_name}"] = team1_winrates.get(map_name, 50.0)
                    result[f"loser_{map_name}"] = team2_winrates.get(map_name, 50.0)
                else:  # winner == "team2"
                    result[f"winner_{map_name}"] = team2_winrates.get(map_name, 50.0)
                    result[f"loser_{map_name}"] = team1_winrates.get(map_name, 50.0)
            
            return result
            
        except Exception as e:
            # Return default 50% for all maps
            all_maps = ['mirage', 'inferno', 'nuke', 'dust2', 'overpass', 'train', 'ancient', 'cache', 'vertigo', 'anubis', 'cobblestone']
            result = {}
            for map_name in all_maps:
                result[f"winner_{map_name}"] = 50.0
                result[f"loser_{map_name}"] = 50.0
            return result

    def extract_map_veto(self, match_url: str, team1_name: str, team2_name: str, winner: str) -> Dict[str, Optional[str]]:
        """Extract map veto information from match page"""
        try:
            soup = self.get_page_content(match_url)
            if not soup:
                return {"winner_map": None, "loser_map": None, "decider": None}
            
            # Look for the map veto section in col-6 col-7-small class
            veto_elem = soup.select_one('.col-6.col-7-small')
            if not veto_elem:
                return {"winner_map": None, "loser_map": None, "decider": None}
            
            veto_text = veto_elem.get_text()
            
            # Parse the veto text to extract map picks
            team1_picked_map = None
            team2_picked_map = None
            decider = None
            
            lines = veto_text.strip().split('\n')
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                
                # Look for patterns like "X picked Y"
                if 'picked' in line.lower():
                    # Extract team name and map
                    parts = line.split(' picked ')
                    if len(parts) == 2:
                        team_name = parts[0].strip()
                        map_name = parts[1].strip()
                        
                        # Match team name to determine which team picked which map
                        # Use more flexible matching to handle slight differences
                        if (team_name.lower() == team1_name.lower() or 
                            team_name.lower() in team1_name.lower() or 
                            team1_name.lower() in team_name.lower()):
                            team1_picked_map = map_name
                        elif (team_name.lower() == team2_name.lower() or 
                              team_name.lower() in team2_name.lower() or 
                              team2_name.lower() in team_name.lower()):
                            team2_picked_map = map_name
                
                elif 'was left over' in line.lower():
                    # Extract the decider map
                    parts = line.split(' was left over')
                    if len(parts) == 1:
                        decider = parts[0].strip()
                    else:
                        # Handle cases like "7. Nuke was left over"
                        decider = line.replace(' was left over', '').strip()
                        if decider.startswith('7.'):
                            decider = decider.replace('7.', '').strip()
                
                # Also check for patterns like "7. Nuke" (decider map)
                elif line.startswith('7.') and not decider:
                    # Extract map name after "7. "
                    map_name = line.replace('7.', '').strip()
                    if map_name and not 'was left over' in map_name.lower():
                        decider = map_name
            
            # Determine winner and loser maps based on actual winner
            if winner == "team1":
                winner_map = team1_picked_map
                loser_map = team2_picked_map
            elif winner == "team2":
                winner_map = team2_picked_map
                loser_map = team1_picked_map
            else:
                # If it's a tie or unknown, just use the order
                winner_map = team1_picked_map
                loser_map = team2_picked_map
            
            return {
                "winner_map": winner_map,
                "loser_map": loser_map, 
                "decider": decider
            }
            
        except Exception as e:
            return {"winner_map": None, "loser_map": None, "decider": None}
    
    def is_match_forfeited(self, match_element, match_url: str) -> bool:
        """Check if a match was forfeited"""
        try:
            # Check score for 1-0 or 0-1 patterns
            score_element = match_element.select_one('.result-score')
            if score_element:
                score_text = score_element.get_text().strip()
                if score_text in ['1-0', '0-1']:
                    # Verify on match page
                    soup = self.get_page_content(match_url)
                    if soup:
                        forfeit_text = soup.select_one('.padding.preformatted-text')
                        if forfeit_text and 'forfeit' in forfeit_text.get_text().lower():
                            return True
            return False
        except Exception as e:
            return False
    
    def extract_match_info(self, match_element, match_number: int) -> Optional[Dict[str, Any]]:
        """Extract basic match information from a match element"""
        try:
            # Get match URL
            match_link = match_element.select_one('a')
            if not match_link:
                return None
            
            match_url = self.base_url + match_link.get('href')
            match_id = self.extract_match_id_from_url(match_url)
            
            if not match_id:
                return None
            
            # Check if we've reached our target match ID
            if match_id and match_id <= self.target_match_id:
                return None
            
            # Extract team names
            team1_elem = match_element.select_one('.team1 .team')
            team2_elem = match_element.select_one('.team2 .team')
            
            team1_name = team1_elem.get_text().strip() if team1_elem else "Unknown Team 1"
            team2_name = team2_elem.get_text().strip() if team2_elem else "Unknown Team 2"
            
            # Extract score
            score_elem = match_element.select_one('.result-score')
            if not score_elem:
                return None
            
            score_text = score_elem.get_text().strip()
            try:
                team1_score, team2_score = map(int, score_text.split(' - '))
            except:
                return None
            
            # Determine winner
            if team1_score > team2_score:
                winner = "team1"
            elif team2_score > team1_score:
                winner = "team2"
            else:
                winner = "tie"
            
            return {
                'match_id': match_id,
                'match_url': match_url,
                'team1_name': team1_name,
                'team2_name': team2_name,
                'team1_score': team1_score,
                'team2_score': team2_score,
                'winner': winner,
                'match_number': match_number
            }
            
        except Exception as e:
            return None
    
    def scrape_team_urls(self, match_url: str) -> tuple:
        """Scrape team URLs from match page"""
        try:
            soup = self.get_page_content(match_url)
            if not soup:
                return None, None
            
            team1_elem = soup.select_one('.team1-gradient a')
            team2_elem = soup.select_one('.team2-gradient a')
            
            team1_url = self.base_url + team1_elem.get('href') if team1_elem else None
            team2_url = self.base_url + team2_elem.get('href') if team2_elem else None
            
            return team1_url, team2_url
        except Exception as e:
            return None, None
    
    def scrape_team_players(self, team_url: str, team_name: str) -> List[Dict[str, Any]]:
        """Scrape team players and their stats URLs"""
        try:
            soup = self.get_page_content(team_url)
            if not soup:
                return []
            
            players = []
            player_elements = soup.select('.bodyshot-team a')
            
            for player_elem in player_elements:
                player_url = player_elem.get('href')
                if player_url and '/player/' in player_url:
                    # Extract player ID and name from URL
                    match = re.search(r'/player/(\d+)/([^/]+)', player_url)
                    if match:
                        player_id = match.group(1)
                        player_name = match.group(2)
                        
                        # Construct stats URL
                        stats_url = f"{self.base_url}/stats/players/{player_id}/{player_name}"
                        
                        players.append({
                            'name': player_name,
                            'player_id': player_id,
                            'stats_url': stats_url
                        })
            
            return players
        except Exception as e:
            return []
    
    def scrape_player_stats(self, stats_url: str, player_name: str) -> Dict[str, Optional[float]]:
        """Scrape individual player statistics"""
        try:
            soup = self.get_page_content(stats_url)
            if not soup:
                return {"DPR": None, "KAST": None, "ADR": None, "KPR": None, "RATING": None}
            
            stats = {}
            
            # Look for stats in the specific class
            stat_boxes = soup.find_all('div', class_='player-summary-stat-box-data traditionalData')
            
            if len(stat_boxes) >= 5:
                # Map by position (skip box 2 which is multi-kill)
                if len(stat_boxes) > 0:  # DPR
                    stats["DPR"] = self.safe_float(stat_boxes[0].get_text().strip())
                if len(stat_boxes) > 1:  # KAST
                    kast_text = stat_boxes[1].get_text().strip()
                    if '%' in kast_text:
                        stats["KAST"] = self.safe_float(kast_text.replace('%', ''))
                if len(stat_boxes) > 3:  # ADR
                    stats["ADR"] = self.safe_float(stat_boxes[3].get_text().strip())
                if len(stat_boxes) > 4:  # KPR
                    stats["KPR"] = self.safe_float(stat_boxes[4].get_text().strip())
            
            # Get RATING from separate class
            rating_elem = soup.find('div', class_='player-summary-stat-box-rating-data-text')
            if rating_elem:
                stats["RATING"] = self.safe_float(rating_elem.get_text().strip())
            
            # Ensure all stats are present
            for stat in ["DPR", "KAST", "ADR", "KPR", "RATING"]:
                if stat not in stats:
                    stats[stat] = None
            
            return stats
            
        except Exception as e:
            return {"DPR": None, "KAST": None, "ADR": None, "KPR": None, "RATING": None}
    
    def safe_float(self, value: str) -> Optional[float]:
        """Safely convert string to float"""
        try:
            if value and value != '-' and value.strip():
                return float(value.strip())
        except:
            pass
        return None
    
    def safe_int(self, value: str) -> Optional[int]:
        """Safely convert string to int"""
        try:
            if value is None:
                return None
            cleaned = value.replace(',', '').strip()
            if not cleaned or cleaned == '-':
                return None
            return int(cleaned)
        except:
            return None
    
    def parse_ratio_pair(self, value: str) -> Tuple[int, int]:
        """Parse strings like '17 : 13' into integer pairs"""
        if not value:
            return 0, 0
        cleaned = value.replace(' ', '')
        for sep in [':', '/', '-']:
            if sep in cleaned:
                left, right = cleaned.split(sep, 1)
                return (self.safe_int(left) or 0, self.safe_int(right) or 0)
        parsed = self.safe_int(cleaned)
        return (parsed or 0, 0)
    
    def parse_parenthetical_primary(self, value: str) -> Optional[int]:
        """Parse values like '75(45)' and return the primary number (75)"""
        if not value:
            return None
        cleaned = value.replace(' ', '')
        match = re.match(r'(-?\d+)\(([-\d]+)\)', cleaned)
        if match:
            return self.safe_int(match.group(1))
        return self.safe_int(value)
    
    def parse_percentage_value(self, value: str) -> Optional[float]:
        """Convert percentage strings to floats while preserving sign"""
        if not value:
            return None
        cleaned = value.strip()
        if not cleaned or cleaned == '-':
            return None
        sign = 1
        if cleaned.startswith('+'):
            cleaned = cleaned[1:]
        elif cleaned.startswith('-'):
            cleaned = cleaned[1:]
            sign = -1
        cleaned = cleaned.replace('%', '').strip()
        if not cleaned:
            return None
        try:
            return sign * float(cleaned)
        except:
            return None
    
    def calculate_team_averages(self, players: List[Dict[str, Any]]) -> Dict[str, Optional[float]]:
        """Calculate average statistics for a team's players"""
        stats_keys = ['DPR', 'KAST', 'ADR', 'KPR', 'RATING']
        team_averages = {}
        
        for stat in stats_keys:
            values = []
            for player in players:
                if player.get('statistics') and player['statistics'].get(stat) is not None:
                    values.append(player['statistics'][stat])
            
            if values:
                team_averages[stat] = round(mean(values), 2)
            else:
                team_averages[stat] = None
                
        return team_averages
    
    def scrape_enhanced_matches_from_snapshot(self) -> List[Dict[str, Any]]:
        """Scrape matches using pre-captured snapshot of match IDs"""
        all_matches = []
        matches_found = 0
        
        print(f"📸 Starting snapshot-based scraping")
        print(f"🎯 Target: {self.num_matches} valid matches")
        print(f"📊 Snapshot contains: {len(self.snapshot_data)} match IDs")
        print(f"🔄 Starting from snapshot index: {self.snapshot_index}")
        
        try:
            while matches_found < self.num_matches and self.snapshot_index < len(self.snapshot_data):
                # Check for pause signal before processing each match
                if self.check_pause_signal():
                    if self.handle_pause():
                        return all_matches
                
                # Get match data from snapshot
                snapshot_match = self.snapshot_data[self.snapshot_index]
                match_id = snapshot_match['match_id']
                
                # Increment counters
                self.match_counter += 1
                self.snapshot_index += 1
                current_season = self.get_current_season()
                
                match_number = matches_found + 1
                print(f"Scraping game #{self.match_counter} (Snapshot index: {self.snapshot_index}/{len(self.snapshot_data)}, Valid matches: {matches_found}/{self.num_matches})")
                
                # Save progress every 10 matches
                if self.match_counter % 10 == 0:
                    self.save_progress()
                
                # Save intermediate data every 100 matches
                if self.match_counter % 100 == 0 and matches_found > 0:
                    checkpoint = f"checkpoint_{self.match_counter}"
                    json_file = self.save_intermediate_data(all_matches, checkpoint)
                    csv_file = self.convert_to_csv(all_matches, json_file)
                    print(f"📊 Checkpoint saved: {len(all_matches)} matches at game #{self.match_counter}")
                
                # Save final CSV at 10,000 games
                if matches_found >= 10000:
                    print(f"🎯 Reached 10,000 matches! Saving final CSV...")
                    json_file = self.save_to_json(all_matches)
                    csv_file = self.convert_to_csv(all_matches, json_file)
                    self.save_progress()
                    print(f"\n📊 Final Scraping Summary (10,000 matches):")
                    print(f"  • Matches scraped: {len(all_matches)}")
                    print(f"  • Total matches processed: {self.match_counter}")
                    print(f"  • JSON file: {json_file}")
                    print(f"  • CSV file: {csv_file}")
                    return all_matches
                
                try:
                    # Construct match URL from ID
                    # We need to visit the match page to get the full URL with slug
                    # For now, construct a basic URL and let HLTV redirect us
                    match_url = f"{self.base_url}/matches/{match_id}/-"
                    
                    # Check for forfeit first
                    if snapshot_match['score'] in ['1-0', '0-1']:
                        soup = self.get_page_content(match_url)
                        if soup:
                            forfeit_text = soup.select_one('.padding.preformatted-text')
                            if forfeit_text and 'forfeit' in forfeit_text.get_text().lower():
                                print(f"Skipped game #{self.match_counter} due to forfeit")
                                continue
                    
                    # Get match page for full details
                    soup = self.get_page_content(match_url)
                    if not soup:
                        print(f"Skipped game #{self.match_counter} - couldn't load match page")
                        continue
                    
                    # Extract match info from the match page directly
                    # Get teams from match page
                    team1_elem = soup.select_one('.team1-gradient .teamName')
                    team2_elem = soup.select_one('.team2-gradient .teamName')
                    
                    if not team1_elem or not team2_elem:
                        print(f"Skipped game #{self.match_counter} - couldn't find teams")
                        continue
                    
                    team1_name = team1_elem.get_text().strip()
                    team2_name = team2_elem.get_text().strip()
                    
                    print(f"🎯 Game #{self.match_counter} | Match ID {match_id} | {team1_name} vs {team2_name}")
                    
                    # Get score from snapshot data
                    try:
                        # Handle both "1-2" and "1 - 2" formats
                        score_text = snapshot_match['score'].replace(' ', '')
                        team1_score, team2_score = map(int, score_text.split('-'))
                    except:
                        print(f"Skipped game #{self.match_counter} - invalid score format: {snapshot_match.get('score', 'unknown')}")
                        continue
                    
                    # Determine winner
                    if team1_score > team2_score:
                        winner = "team1"
                    elif team2_score > team1_score:
                        winner = "team2"
                    else:
                        winner = "tie"
                    
                    match_info = {
                        'match_id': match_id,
                        'match_url': match_url,
                        'team1_name': team1_name,
                        'team2_name': team2_name,
                        'team1_score': team1_score,
                        'team2_score': team2_score,
                        'winner': winner,
                        'match_number': match_number
                    }
                    
                    match_metadata = self.extract_enhanced_data_from_soup(soup, match_info)
                    if not match_metadata:
                        print(f"Skipped game #{self.match_counter} due to missing match metadata")
                        continue
                    
                    detailed_stats = self.extract_detailed_stats_from_match_page(soup, match_info)
                    if not detailed_stats:
                        print(f"Skipped game #{self.match_counter} due to missing detailed stats")
                        continue
                    
                    match_data = self.build_match_dataset_entry(match_info, match_metadata, detailed_stats, current_season, match_number)
                    all_matches.append(match_data)
                    matches_found += 1
                    print(f"✅ Finished scraping game #{self.match_counter} (Valid match #{matches_found})")
                    
                    # Add delay after successful match
                    time.sleep(self.match_delay)
                    
                except Exception as e:
                    print(f"⚠️ Skipped game #{self.match_counter} due to error: {e}")
                    continue
            
            print(f"\n🎉 Found {len(all_matches)} enhanced matches from snapshot!")
            return all_matches
            
        except Exception as e:
            print(f"❌ Error during snapshot scraping: {e}")
            import traceback
            traceback.print_exc()
            return all_matches
        finally:
            # Always save final progress
            self.save_progress()
    
    def extract_enhanced_data_from_soup(self, soup: BeautifulSoup, match_info: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Extract enhanced data from an already-loaded match page soup"""
        try:
            match_date = self.extract_match_date_from_soup(soup)
            tournament = self.extract_tournament_from_soup(soup)
            event_type = self.extract_event_type_from_soup(soup)
            canonical_url = self.extract_canonical_url_from_soup(soup)
            
            return {
                'match_date': match_date,
                'tournament': tournament,
                'event_type': event_type,
                'match_url': canonical_url or match_info.get("match_url")
            }
        except Exception as e:
            print(f"⚠️ Error extracting enhanced data: {e}")
            return None
    
    def extract_canonical_url_from_soup(self, soup: BeautifulSoup) -> Optional[str]:
        """Get canonical HLTV URL from match soup"""
        try:
            canonical_elem = soup.select_one('link[rel="canonical"]')
            if canonical_elem and canonical_elem.get('href'):
                href = canonical_elem.get('href')
                if href.startswith('http'):
                    return href
                return f"{self.base_url}{href}"
            return None
        except Exception:
            return None
    
    def extract_detailed_stats_from_match_page(self, match_soup: BeautifulSoup, match_info: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Follow the detailed stats link and aggregate total stats for both teams"""
        try:
            detail_link = None
            for link in match_soup.select('a[href]'):
                href = link.get('href')
                text = link.get_text(strip=True).lower() if link.get_text() else ''
                if href and '/stats/matches/' in href and 'detailed stats' in text:
                    detail_link = href
                    break
            
            if not detail_link:
                # Fallback: grab first stats/matches link
                fallback = match_soup.select_one('a[href*="/stats/matches/"]')
                if fallback:
                    detail_link = fallback.get('href')
            
            if not detail_link:
                print("⚠️ No detailed stats link found on match page")
                return None
            
            if not detail_link.startswith('http'):
                detail_url = f"{self.base_url}{detail_link}"
            else:
                detail_url = detail_link
            
            stats_soup = self.get_page_content(detail_url)
            if not stats_soup:
                print("⚠️ Unable to load detailed stats page")
                return None
            
            tables = stats_soup.select('table.stats-table.totalstats')
            if len(tables) < 2:
                print("⚠️ Detailed stats tables not found or incomplete")
                return None
            
            team_tables = []
            for table in tables[:2]:
                header = table.select_one('thead th')
                team_name = header.get_text(strip=True) if header else None
                aggregated_stats = self.aggregate_totalstats_table(table)
                team_tables.append({
                    "name": team_name,
                    "stats": aggregated_stats
                })
            
            team1_stats = self.match_stats_to_team(team_tables, match_info['team1_name'], fallback_index=0)
            team2_stats = self.match_stats_to_team(team_tables, match_info['team2_name'], fallback_index=1)
            
            return {
                "detailed_stats_url": detail_url,
                "team1_stats": team1_stats,
                "team2_stats": team2_stats
            }
        except Exception as e:
            print(f"⚠️ Error extracting detailed stats: {e}")
            return None
    
    def match_stats_to_team(self, team_tables: List[Dict[str, Any]], team_name: str, fallback_index: int) -> Dict[str, Any]:
        """Align aggregated table stats to the expected team based on name"""
        for entry in team_tables:
            entry_name = entry.get("name")
            if entry_name and entry_name.lower() == team_name.lower():
                return entry["stats"]
        if 0 <= fallback_index < len(team_tables):
            return team_tables[fallback_index]["stats"]
        return {}
    
    def aggregate_totalstats_table(self, table: BeautifulSoup) -> Dict[str, Optional[float]]:
        """Aggregate per-player rows in the total stats table into team-level features"""
        stats = {
            "opening_kills": 0,
            "opening_deaths": 0,
            "multi_kill_rounds": 0,
            "kast_pct": None,
            "clutches_won": 0,
            "kills": 0,
            "assists": 0,
            "deaths": 0,
            "adr": None,
            "swing_pct": None,
            "rating_3": None
        }
        
        kast_values = []
        adr_values = []
        swing_values = []
        rating_values = []
        
        rows = table.select('tbody tr')
        for row in rows:
            cells = [cell.get_text(strip=True) for cell in row.select('td')]
            if len(cells) < 18:
                continue
            
            op_k, op_d = self.parse_ratio_pair(cells[1])
            stats["opening_kills"] += op_k
            stats["opening_deaths"] += op_d
            
            stats["multi_kill_rounds"] += self.safe_int(cells[3]) or 0
            
            kast_value = self.parse_percentage_value(cells[14] if len(cells) > 14 else cells[4])
            if kast_value is not None:
                kast_values.append(kast_value)
            
            stats["clutches_won"] += self.safe_int(cells[6]) or 0
            
            kills = self.parse_parenthetical_primary(cells[7])
            if kills is not None:
                stats["kills"] += kills
            
            assists = self.parse_parenthetical_primary(cells[9])
            if assists is not None:
                stats["assists"] += assists
            
            deaths = self.parse_parenthetical_primary(cells[10])
            if deaths is not None:
                stats["deaths"] += deaths
            
            adr = self.safe_float(cells[12])
            if adr is not None:
                adr_values.append(adr)
            
            swing = self.parse_percentage_value(cells[16])
            if swing is not None:
                swing_values.append(swing)
            
            rating = self.safe_float(cells[17])
            if rating is not None:
                rating_values.append(rating)
        
        if kast_values:
            stats["kast_pct"] = round(sum(kast_values) / len(kast_values), 2)
        if adr_values:
            stats["adr"] = round(sum(adr_values) / len(adr_values), 2)
        if swing_values:
            stats["swing_pct"] = round(sum(swing_values) / len(swing_values), 2)
        if rating_values:
            stats["rating_3"] = round(sum(rating_values) / len(rating_values), 2)
        
        return stats
    
    def build_match_dataset_entry(self, match_info: Dict[str, Any], metadata: Dict[str, Any], detailed_stats: Dict[str, Any], current_season: int, match_number: int) -> Dict[str, Any]:
        """Combine match metadata and detailed stats into a single flat record"""
        winner_side = match_info.get("winner")
        if winner_side == "team1":
            winner_name = match_info["team1_name"]
        elif winner_side == "team2":
            winner_name = match_info["team2_name"]
        else:
            winner_name = None
        
        record = {
            "match_id": f"hltv_match_{match_info['match_id']}",
            "hltv_match_id": match_info["match_id"],
            "match_number": match_number,
            "season": current_season,
            "date": metadata.get("match_date"),
            "event_name": metadata.get("tournament"),
            "event_type": metadata.get("event_type"),
            "winner": winner_name,
            "winner_side": winner_side,
            "team1_name": match_info["team1_name"],
            "team2_name": match_info["team2_name"],
            "team1_score": match_info["team1_score"],
            "team2_score": match_info["team2_score"],
            "hltv_url": metadata.get("match_url") or match_info["match_url"],
            "detailed_stats_url": detailed_stats.get("detailed_stats_url"),
            "scraped_date": datetime.now().isoformat() + "Z"
        }
        
        for prefix, stats in (("team1", detailed_stats.get("team1_stats", {})), ("team2", detailed_stats.get("team2_stats", {}))):
            for key, value in stats.items():
                record[f"{prefix}_{key}"] = value
        
        return record
    
    def extract_match_date_from_soup(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract match date from already-loaded soup"""
        try:
            time_elem = soup.select_one('.time[data-unix]')
            if time_elem:
                unix_timestamp = time_elem.get('data-unix')
                if unix_timestamp:
                    timestamp = int(unix_timestamp) / 1000
                    date_obj = datetime.fromtimestamp(timestamp)
                    return date_obj.isoformat() + "Z"
            return None
        except Exception as e:
            return None
    
    def extract_tournament_from_soup(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract tournament name from already-loaded soup"""
        try:
            tournament_elem = soup.select_one('.event.text-ellipsis')
            if tournament_elem:
                return tournament_elem.get_text().strip()
            return None
        except Exception as e:
            return None
    
    def extract_event_type_from_soup(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract event type (LAN/Online) from already-loaded soup"""
        try:
            event_text_elem = soup.select_one('.padding.preformatted-text')
            if event_text_elem:
                text = event_text_elem.get_text().lower()
                if 'lan' in text:
                    return 'LAN'
                elif 'online' in text:
                    return 'Online'
            return None
        except Exception as e:
            return None
    
    def extract_map_veto_from_soup(self, soup: BeautifulSoup, match_info: Dict[str, Any]) -> Dict[str, Optional[str]]:
        """Extract map veto information from already-loaded soup"""
        try:
            veto_elem = soup.select_one('.col-6.col-7-small')
            if not veto_elem:
                return {"winner_map": None, "loser_map": None, "decider": None}
            
            veto_text = veto_elem.get_text()
            
            team1_name = match_info["team1_name"]
            team2_name = match_info["team2_name"]
            winner = match_info["winner"]
            
            team1_map = None
            team2_map = None
            decider_map = None
            
            for line in veto_text.split('\n'):
                line = line.strip()
                if 'picked' in line.lower():
                    if team1_name in line:
                        parts = line.split('picked')
                        if len(parts) > 1:
                            team1_map = parts[1].strip()
                    elif team2_name in line:
                        parts = line.split('picked')
                        if len(parts) > 1:
                            team2_map = parts[1].strip()
                elif 'was left over' in line.lower():
                    parts = line.split('was left over')
                    if parts:
                        decider_map = parts[0].strip().split()[-1]
            
            if winner == "team1":
                return {"winner_map": team1_map, "loser_map": team2_map, "decider": decider_map}
            elif winner == "team2":
                return {"winner_map": team2_map, "loser_map": team1_map, "decider": decider_map}
            else:
                return {"winner_map": None, "loser_map": None, "decider": decider_map}
                
        except Exception as e:
            return {"winner_map": None, "loser_map": None, "decider": None}
    
    def extract_head_to_head_from_soup(self, soup: BeautifulSoup, match_info: Dict[str, Any]) -> Dict[str, Optional[int]]:
        """Extract head-to-head statistics from already-loaded soup"""
        try:
            h2h_elem = soup.select_one('.head-to-head-listing')
            if not h2h_elem:
                h2h_elem = soup.select_one('.head-to-head')
            
            if not h2h_elem:
                return {"winner_head2head_freq": None, "loser_head2head_freq": None}
            
            h2h_text = h2h_elem.get_text()
            
            winner = match_info["winner"]
            team1_name = match_info["team1_name"]
            team2_name = match_info["team2_name"]
            
            import re
            
            # Try old format first (aggregate wins)
            wins_pattern = r'(\d+)\s*Wins'
            matches = re.findall(wins_pattern, h2h_text)
            
            if len(matches) >= 2:
                team1_wins = int(matches[0])
                team2_wins = int(matches[1])
                
                if winner == "team1":
                    return {"winner_head2head_freq": team1_wins, "loser_head2head_freq": team2_wins}
                elif winner == "team2":
                    return {"winner_head2head_freq": team2_wins, "loser_head2head_freq": team1_wins}
            
            # New format: count map wins from individual results
            # Find all map scores in format "13 - 8"
            map_scores = re.findall(r'(\d+)\s*-\s*(\d+)', h2h_text)
            
            if map_scores:
                team1_map_wins = 0
                team2_map_wins = 0
                
                # Count which team won each map
                # In h2h section, scores are shown from team1's perspective typically
                for score1, score2 in map_scores:
                    s1, s2 = int(score1), int(score2)
                    if s1 > s2:
                        team1_map_wins += 1
                    elif s2 > s1:
                        team2_map_wins += 1
                
                if team1_map_wins + team2_map_wins > 0:
                    if winner == "team1":
                        return {"winner_head2head_freq": team1_map_wins, "loser_head2head_freq": team2_map_wins}
                    elif winner == "team2":
                        return {"winner_head2head_freq": team2_map_wins, "loser_head2head_freq": team1_map_wins}
            
            return {"winner_head2head_freq": None, "loser_head2head_freq": None}
            
        except Exception as e:
            return {"winner_head2head_freq": None, "loser_head2head_freq": None}
    
    def extract_past3_months_from_soup(self, soup: BeautifulSoup, match_info: Dict[str, Any]) -> Dict[str, float]:
        """Extract past 3 months performance from already-loaded soup"""
        try:
            past_tables = soup.select('.past-matches-table')
            if len(past_tables) < 2:
                return {"winner_past3": 50.0, "loser_past3": 50.0}
            
            def calculate_win_percentage(table):
                rows = table.select('tr')
                wins = 0
                total = 0
                
                for row in rows:
                    score_elem = row.select_one('.past-matches-score')
                    if score_elem:
                        score = score_elem.get_text().strip()
                        if '3' in score:
                            continue
                        first_digit = score[0] if score else '0'
                        if first_digit == '2':
                            wins += 1
                        total += 1
                
                if total > 0:
                    return round((wins / total) * 100, 2)
                return 50.0
            
            team1_percentage = calculate_win_percentage(past_tables[0])
            team2_percentage = calculate_win_percentage(past_tables[1])
            
            winner = match_info["winner"]
            if winner == "team1":
                return {"winner_past3": team1_percentage, "loser_past3": team2_percentage}
            elif winner == "team2":
                return {"winner_past3": team2_percentage, "loser_past3": team1_percentage}
            else:
                return {"winner_past3": 50.0, "loser_past3": 50.0}
                
        except Exception as e:
            return {"winner_past3": 50.0, "loser_past3": 50.0}
    
    def extract_team_map_winrates_from_soup(self, soup: BeautifulSoup, match_info: Dict[str, Any]) -> Dict[str, float]:
        """Extract team map win rates from already-loaded soup (needs additional page fetches)"""
        try:
            # This requires visiting team stats pages, so we use the existing method
            return self.extract_team_map_winrates(
                match_info["match_url"],
                match_info["team1_name"],
                match_info["team2_name"],
                match_info["winner"]
            )
        except Exception as e:
            return self.get_default_map_winrates()
    
    def get_default_map_winrates(self) -> Dict[str, float]:
        """Return default 50% win rates for all maps"""
        all_maps = ['mirage', 'inferno', 'nuke', 'dust2', 'overpass', 'train', 'ancient', 'cache', 'vertigo', 'anubis', 'cobblestone']
        result = {}
        for map_name in all_maps:
            result[f'winner_{map_name}'] = 50.0
            result[f'loser_{map_name}'] = 50.0
        return result
    
    def scrape_enhanced_matches(self) -> List[Dict[str, Any]]:
        """Scrape matches with enhanced information"""
        # Use snapshot mode if snapshot data is available
        if self.snapshot_data:
            return self.scrape_enhanced_matches_from_snapshot()
        
        # Otherwise use original pagination-based scraping
        all_matches = []
        page_offset = 0
        page_number = 1
        matches_found = 0
        
        print(f"🔍 Starting scraping: {self.num_matches} matches newer than ID {self.target_match_id}")
        
        try:
            while matches_found < self.num_matches:
                current_page_url = f"{self.results_url}?offset={page_offset}"
                
                soup = self.get_page_content(current_page_url)
                if not soup:
                    break
                
                all_matches_on_page = soup.select('.result-con')
                if not all_matches_on_page:
                    break
                
                for i, match_element in enumerate(all_matches_on_page):
                    # Check for pause signal before processing each match
                    if self.check_pause_signal():
                        if self.handle_pause():
                            return all_matches
                    
                    if matches_found >= self.num_matches:
                        break
                    
                    # Increment match counter and calculate season
                    self.match_counter += 1
                    current_season = self.get_current_season()
                    
                    match_number = matches_found + 1
                    print(f"Scraping game #{self.match_counter}")
                    
                    # Save progress every 10 matches
                    if self.match_counter % 10 == 0:
                        self.save_progress()
                    
                    # Save intermediate data every 100 matches
                    if self.match_counter % 100 == 0 and matches_found > 0:
                        checkpoint = f"checkpoint_{self.match_counter}"
                        json_file = self.save_intermediate_data(all_matches, checkpoint)
                        csv_file = self.convert_to_csv(all_matches, json_file)
                        print(f"📊 Checkpoint saved: {len(all_matches)} matches at game #{self.match_counter}")
                    
                    # Save final CSV at 10,000 games instead of 14,000
                    if matches_found >= 10000:
                        print(f"🎯 Reached 10,000 matches! Saving final CSV...")
                        json_file = self.save_to_json(all_matches)
                        csv_file = self.convert_to_csv(all_matches, json_file)
                        self.save_progress()
                        print(f"\n📊 Final Scraping Summary (10,000 matches):")
                        print(f"  • Matches scraped: {len(all_matches)}")
                        print(f"  • Total matches processed: {self.match_counter}")
                        print(f"  • JSON file: {json_file}")
                        print(f"  • CSV file: {csv_file}")
                        return all_matches
                    
                    try:
                        match_info = self.extract_match_info(match_element, match_number)
                        if not match_info:
                            continue
                        
                        print(f"🎯 Game #{self.match_counter} | Match ID {match_info['match_id']} | {match_info['team1_name']} vs {match_info['team2_name']}")
                        
                        # Check for forfeit
                        if self.is_match_forfeited(match_element, match_info['match_url']):
                            print(f"Skipped game #{self.match_counter} due to forfeit")
                            continue
                        
                        enhanced_data = self.process_match_with_timeout(match_element, match_number, match_info)
                        if not enhanced_data:
                            print(f"Skipped game #{self.match_counter} due to being stuck")
                            continue
                        
                        match_metadata = enhanced_data.get('match_metadata')
                        detailed_stats = enhanced_data.get('detailed_stats')
                        
                        if not match_metadata or not detailed_stats:
                            print(f"Skipped game #{self.match_counter} due to incomplete data")
                            continue
                        
                        match_data = self.build_match_dataset_entry(match_info, match_metadata, detailed_stats, current_season, match_number)
                        
                        all_matches.append(match_data)
                        matches_found += 1
                        print(f"Finished scraping game #{self.match_counter}")
                        
                        # Add delay after successful match
                        time.sleep(self.match_delay)
                        
                    except Exception as e:
                        print(f"Skipped game #{self.match_counter} due to error")
                        continue
                
                if matches_found < self.num_matches:
                    page_offset += 100
                    page_number += 1
                    time.sleep(self.page_delay)
            
            print(f"\n🎉 Found {len(all_matches)} enhanced matches!")
            return all_matches
            
        except Exception as e:
            print(f"Error during scraping: {e}")
            return all_matches
        finally:
            # Always save final progress
            self.save_progress()
    
    def save_to_json(self, matches: List[Dict[str, Any]]) -> str:
        """Save matches to JSON file"""
        output_file = os.path.join(self.output_dir, f"enhanced_matches_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        
        data = {
            "enhanced_scraping_session": {
                "target_match_id": self.target_match_id,
                "num_matches_requested": self.num_matches,
                "matches_found": len(matches),
                "scraped_date": datetime.now().isoformat() + "Z"
            },
            "matches": matches
        }
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        print(f"✅ Enhanced matches saved to {output_file}")
        return output_file
    
    def save_intermediate_data(self, matches: List[Dict[str, Any]], checkpoint: str) -> str:
        """Save intermediate matches to JSON file for checkpoint"""
        output_file = os.path.join(self.output_dir, f"enhanced_matches_{checkpoint}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        
        data = {
            "enhanced_scraping_session": {
                "target_match_id": self.target_match_id,
                "num_matches_requested": self.num_matches,
                "matches_found": len(matches),
                "checkpoint": checkpoint,
                "scraped_date": datetime.now().isoformat() + "Z"
            },
            "matches": matches
        }
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        print(f"✅ Intermediate data saved to {output_file}")
        return output_file
    
    def convert_to_csv(self, matches: List[Dict[str, Any]], json_file: str) -> Optional[str]:
        """Convert the in-memory matches list to CSV"""
        try:
            import pandas as pd
        except ImportError:
            print("❌ pandas is required to export CSV files")
            return None
        
        if not matches:
            print("⚠️ No matches available to convert to CSV")
            return None
        
        csv_file = json_file.replace('.json', '.csv')
        
        try:
            df = pd.DataFrame(matches)
            df.to_csv(csv_file, index=False)
            print(f"✅ CSV file created: {csv_file}")
            return csv_file
        except Exception as e:
            print(f"❌ Error converting to CSV: {e}")
            return None
    
    def run(self) -> None:
        """Main execution method"""
        print("🚀 HLTV Enhanced Scraper")
        print("=" * 50)
        print(f"Target match ID: {self.target_match_id}")
        print(f"Number of matches: {self.num_matches}")
        print(f"Output directory: {self.output_dir}")
        
        # Scrape enhanced matches
        matches = self.scrape_enhanced_matches()
        
        if not matches:
            print("No enhanced matches found!")
            return
        
        # Save to JSON
        json_file = self.save_to_json(matches)
        
        # Convert to CSV
        csv_file = self.convert_to_csv(matches, json_file)
        
        # Save final progress
        self.save_progress()
        
        print(f"\n📊 Enhanced Scraping Summary:")
        print(f"  • Matches scraped: {len(matches)}")
        print(f"  • Total matches processed: {self.match_counter}")
        print(f"  • Final season: {self.get_current_season()}")
        print(f"  • JSON file: {json_file}")
        print(f"  • CSV file: {csv_file}")
        print(f"  • Enhanced data: Date, Tournament, LAN/Online status")

def main():
    """Main function with command line argument handling"""
    parser = argparse.ArgumentParser(description="HLTV Enhanced Scraper - Scrape matches with enhanced information")
    parser.add_argument('--target_match_id', '-t', type=int, default=0,
                       help='Target match ID to scrape backwards from (not used with snapshot mode)')
    parser.add_argument('--num_matches', '-n', type=int, default=3,
                       help='Number of matches to scrape')
    parser.add_argument('--output_dir', '-o', type=str, default='data/enhanced',
                       help='Output directory for enhanced matches')
    parser.add_argument('--snapshot_file', '-s', type=str, default=None,
                       help='Path to snapshot JSON file containing match IDs (enables snapshot mode)')
    parser.add_argument('--pause', action='store_true',
                       help='Create a pause file to stop scraping gracefully')
    
    args = parser.parse_args()
    
    if args.pause:
        # Create pause file
        scraper = HLTVEnhancedScraper(args.target_match_id, args.num_matches, args.output_dir, args.snapshot_file)
        scraper.create_pause_file()
    else:
        scraper = HLTVEnhancedScraper(args.target_match_id, args.num_matches, args.output_dir, args.snapshot_file)
        scraper.run()

if __name__ == "__main__":
    main()
