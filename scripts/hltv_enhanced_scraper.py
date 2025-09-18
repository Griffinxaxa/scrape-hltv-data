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
from datetime import datetime
from typing import Dict, List, Any, Optional
from statistics import mean
import cloudscraper
from bs4 import BeautifulSoup
import re

class HLTVEnhancedScraper:
    def __init__(self, target_match_id: int, num_matches: int = 3, output_dir: str = "data/enhanced"):
        self.target_match_id = target_match_id
        self.num_matches = num_matches
        self.output_dir = output_dir
        self.base_url = "https://www.hltv.org"
        self.results_url = f"{self.base_url}/results"
        
        # Delays for respectful scraping
        self.page_delay = 2
        self.match_delay = 1
        
        # Large-scale scraping features
        self.match_counter = 0
        self.pause_file = os.path.join(output_dir, "scraper_pause.flag")
        self.progress_file = os.path.join(output_dir, "scraper_progress.json")
        self.matches_per_season = 1750
        
        # Load progress if resuming
        self.load_progress()
        self.player_stat_delay = 0.5
        
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
    
    def load_progress(self):
        """Load scraping progress from file if resuming"""
        try:
            if os.path.exists(self.progress_file):
                with open(self.progress_file, 'r') as f:
                    progress_data = json.load(f)
                    self.match_counter = progress_data.get('match_counter', 0)
                    print(f"🔄 Resuming scraping from match #{self.match_counter + 1}")
            else:
                self.match_counter = 0
                print(f"🚀 Starting fresh scraping session")
        except Exception as e:
            print(f"⚠️ Error loading progress: {e}. Starting fresh.")
            self.match_counter = 0
    
    def save_progress(self):
        """Save current scraping progress"""
        try:
            progress_data = {
                'match_counter': self.match_counter,
                'timestamp': datetime.now().isoformat()
            }
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
        
    def get_page_content(self, url: str) -> BeautifulSoup:
        """Get page content using cloudscraper"""
        try:
            response = self.session.get(url)
            response.raise_for_status()
            return BeautifulSoup(response.content, 'html.parser')
        except Exception as e:
            print(f"Error fetching {url}: {e}")
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
            
            print(f"    -> No date found for match {match_url}")
            return None
            
        except Exception as e:
            print(f"    -> Error extracting date: {e}")
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
            
            print(f"    -> No tournament found for match {match_url}")
            return None
            
        except Exception as e:
            print(f"    -> Error extracting tournament: {e}")
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
            
            print(f"    -> No event type found for match {match_url}")
            return "unknown"
            
        except Exception as e:
            print(f"    -> Error extracting event type: {e}")
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
                print(f"    -> No head-to-head sections found for match {match_url}")
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
                    print(f"    -> Found score pattern: {score1}-{score2}")
                    
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
                    
                    print(f"    -> Assigned scores - {team1_name}: {team1_head2head_freq}, {team2_name}: {team2_head2head_freq}")
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
            
            print(f"    -> Head-to-head - {team1_name}: {team1_head2head_freq}, {team2_name}: {team2_head2head_freq}")
            print(f"    -> Winner H2H: {winner_head2head_freq}, Loser H2H: {loser_head2head_freq}")
            return {
                "winner_head2head_freq": winner_head2head_freq,
                "loser_head2head_freq": loser_head2head_freq
            }
            
        except Exception as e:
            print(f"    -> Error extracting head-to-head: {e}")
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
            
            print(f"    -> Found {len(past_matches_boxes)} past-matches-box elements")
            
            # Let's also try to find the tables which might have more complete data
            past_tables = soup.select('.past-matches-table')
            print(f"    -> Found {len(past_tables)} past-matches-table elements")
            
            if past_tables:
                for i, table in enumerate(past_tables):
                    rows = table.select('tr')
                    print(f"    -> Table {i+1}: {len(rows)} rows")
                    for j, row in enumerate(rows[:5]):  # Show first 5 rows
                        team_cell = row.select_one('.past-matches-team')
                        score_cell = row.select_one('.past-matches-score')
                        if team_cell and score_cell:
                            print(f"    -> Table {i+1}, Row {j+1}: Team='{team_cell.get_text().strip()}', Score='{score_cell.get_text().strip()}'")
            
            # Since tables 1&3 and 2&4 appear to be duplicates, let's just process the first 2 unique ones
            # Based on the debug output, Table 1 has 19 rows (should be Astralis) and Table 2 has 16 rows (should be GamerLegion)
            
            if len(past_tables) >= 2:
                # Process first table (should be Astralis - 19 rows)
                table1 = past_tables[0]
                rows1 = table1.select('tr')
                print(f"    -> Processing Table 1 as {team1_name}: {len(rows1)} matches")
                
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
                print(f"    -> Table 1 ({team1_name}) totals: {table1_wins}/{table1_total} wins")
                
                # Process second table (should be GamerLegion - 16 rows)  
                table2 = past_tables[1]
                rows2 = table2.select('tr')
                print(f"    -> Processing Table 2 as {team2_name}: {len(rows2)} matches")
                
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
                print(f"    -> Table 2 ({team2_name}) totals: {table2_wins}/{table2_total} wins")
            
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
            
            print(f"    -> Past 3 months - {team1_name}: {team1_wins}/{team1_total} ({team1_percentage}%), {team2_name}: {team2_wins}/{team2_total} ({team2_percentage}%)")
            print(f"    -> Winner past3: {winner_past3}%, Loser past3: {loser_past3}%")
            
            return {
                "winner_past3": winner_past3,
                "loser_past3": loser_past3
            }
            
        except Exception as e:
            print(f"    -> Error extracting past 3 months: {e}")
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
            
            print(f"    -> Extracted team IDs: team1={team1_id}, team2={team2_id}")
            return {"team1_id": team1_id, "team2_id": team2_id}
            
        except Exception as e:
            print(f"    -> Error extracting team IDs: {e}")
            return {"team1_id": None, "team2_id": None}
    
    def extract_map_winrates(self, team_id: str, team_name: str) -> Dict[str, float]:
        """Extract map win rates for a specific team"""
        try:
            # Format team name for URL (lowercase, replace spaces with dashes)
            team_name_formatted = team_name.lower().replace(' ', '-').replace('.', '')
            stats_url = f"https://www.hltv.org/stats/teams/maps/{team_id}/{team_name_formatted}"
            
            print(f"    -> Fetching map stats from: {stats_url}")
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
            
            # Debug: Print all map-related elements found
            print(f"    -> Found {len(map_elements)} map-pool-map-name elements")
            
            # Also try alternative selectors
            if not map_elements:
                alt_selectors = ['.map-pool-map', '.map-name', '[class*="map"]']
                for selector in alt_selectors:
                    alt_elements = soup.select(selector)
                    if alt_elements:
                        print(f"    -> Alternative selector '{selector}' found {len(alt_elements)} elements")
                        map_elements = alt_elements[:11]  # Take first 11 as backup
                        break
            
            # Parse the actual map data
            for i, element in enumerate(map_elements):
                print(f"    -> Processing map element {i+1}")
                
                # Try to get map name and percentage from the element text
                full_text = element.get_text().strip()
                if not full_text:
                    continue
                
                print(f"    -> Processing element text: '{full_text}'")
                
                import re
                # Look for pattern like "mapname - percentage%"
                match = re.match(r'^([a-zA-Z0-9]+)\s*-\s*(\d+(?:\.\d+)?)%', full_text)
                if match:
                    map_name = match.group(1).lower()
                    percentage = float(match.group(2))
                    
                    print(f"    -> Extracted map: '{map_name}', percentage: {percentage}%")
                    
                    if map_name in all_maps:
                        map_winrates[map_name] = percentage
                        print(f"    -> {team_name} {map_name}: {percentage}%")
                    else:
                        print(f"    -> Map '{map_name}' not in our list of maps")
                else:
                    # If no percentage in the text, just get the map name
                    map_name = full_text.lower()
                    if map_name in all_maps:
                        print(f"    -> Found map '{map_name}' without percentage, using default 50%")
                    else:
                        print(f"    -> Could not parse '{full_text}'")
            
            return map_winrates
            
        except Exception as e:
            print(f"    -> Error extracting map winrates for {team_name}: {e}")
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
                print(f"    -> Could not extract team IDs, using defaults")
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
            print(f"    -> Error extracting team map winrates: {e}")
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
                print(f"    -> No map veto section found for match {match_url}")
                return {"winner_map": None, "loser_map": None, "decider": None}
            
            veto_text = veto_elem.get_text()
            print(f"    -> Map veto text: {veto_text[:300]}...")
            
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
            
            print(f"    -> Team maps - {team1_name}: {team1_picked_map}, {team2_name}: {team2_picked_map}")
            print(f"    -> Winner: {winner}, Winner Map: {winner_map}, Loser Map: {loser_map}, Decider: {decider}")
            return {
                "winner_map": winner_map,
                "loser_map": loser_map, 
                "decider": decider
            }
            
        except Exception as e:
            print(f"    -> Error extracting map veto: {e}")
            return {"winner_map": None, "loser_map": None, "decider": None}
    
    def is_match_forfeited(self, match_element, match_url: str) -> bool:
        """Check if a match was forfeited"""
        try:
            # Check score for 1-0 or 0-1 patterns
            score_element = match_element.select_one('.result-score')
            if score_element:
                score_text = score_element.get_text().strip()
                if score_text in ['1-0', '0-1']:
                    print(f"  -> Potential forfeit detected by score: {score_text}")
                    
                    # Verify on match page
                    soup = self.get_page_content(match_url)
                    if soup:
                        forfeit_text = soup.select_one('.padding.preformatted-text')
                        if forfeit_text and 'forfeit' in forfeit_text.get_text().lower():
                            print(f"  -> Confirmed forfeit on match page")
                            return True
            return False
        except Exception as e:
            print(f"  -> Error checking forfeit status: {e}")
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
            if match_id <= self.target_match_id:
                print(f"  -> Reached target match ID {self.target_match_id} (found {match_id})")
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
            print(f"  -> Error extracting match info: {e}")
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
            print(f"    Error scraping team URLs: {e}")
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
            print(f"    Error scraping team players: {e}")
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
            print(f"    Error scraping stats for {player_name}: {e}")
            return {"DPR": None, "KAST": None, "ADR": None, "KPR": None, "RATING": None}
    
    def safe_float(self, value: str) -> Optional[float]:
        """Safely convert string to float"""
        try:
            if value and value != '-' and value.strip():
                return float(value.strip())
        except:
            pass
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
    
    def scrape_enhanced_matches(self) -> List[Dict[str, Any]]:
        """Scrape matches with enhanced information"""
        all_matches = []
        page_offset = 0
        page_number = 1
        matches_found = 0
        
        print(f"🔍 Searching for {self.num_matches} matches newer than ID {self.target_match_id}...")
        
        try:
            while matches_found < self.num_matches:
                current_page_url = f"{self.results_url}?offset={page_offset}"
                print(f"\n=== Scraping Page {page_number} ===")
                print(f"Fetching: {current_page_url}")
                
                soup = self.get_page_content(current_page_url)
                if not soup:
                    print(f"Failed to load page {page_number}")
                    break
                
                all_matches_on_page = soup.select('.result-con')
                if not all_matches_on_page:
                    print(f"No more matches found on page {page_number}, stopping.")
                    break
                
                print(f"Found {len(all_matches_on_page)} matches on page {page_number}")
                
                for i, match_element in enumerate(all_matches_on_page):
                    # Check for pause signal before processing each match
                    if self.check_pause_signal():
                        if self.handle_pause():
                            return all_matches
                    
                    if matches_found >= self.num_matches:
                        print(f"Reached target of {self.num_matches} matches, stopping...")
                        break
                    
                    # Increment match counter and calculate season
                    self.match_counter += 1
                    current_season = self.get_current_season()
                    
                    match_number = matches_found + 1
                    print(f"\nProcessing match {match_number}/{self.num_matches} (Page {page_number}, Match {i+1})...")
                    print(f"📊 Global Match #{self.match_counter} | Season {current_season}")
                    
                    # Save progress every 10 matches
                    if self.match_counter % 10 == 0:
                        self.save_progress()
                    
                    try:
                        match_info = self.extract_match_info(match_element, match_number)
                        if not match_info:
                            print(f"  -> No match info extracted or reached target ID")
                            continue
                        
                        # Check for forfeit
                        if self.is_match_forfeited(match_element, match_info['match_url']):
                            print(f"  -> Match {match_number} appears to be forfeited, skipping...")
                            continue
                        
                        print(f"  -> Processing: {match_info['team1_name']} vs {match_info['team2_name']}")
                        
                        # Extract enhanced information from match page
                        print(f"    Extracting enhanced information...")
                        match_date = self.extract_match_date(match_info['match_url'])
                        tournament = self.extract_tournament(match_info['match_url'])
                        event_type = self.extract_event_type(match_info['match_url'])
                        map_veto = self.extract_map_veto(match_info['match_url'], match_info['team1_name'], match_info['team2_name'], match_info['winner'])
                        head2head = self.extract_head_to_head(match_info['match_url'], match_info['team1_name'], match_info['team2_name'], match_info['winner'])
                        past3_data = self.extract_past3_months(match_info['match_url'], match_info['team1_name'], match_info['team2_name'], match_info['winner'])
                        map_winrates = self.extract_team_map_winrates(match_info['match_url'], match_info['team1_name'], match_info['team2_name'], match_info['winner'])
                        
                        print(f"    -> Date: {match_date}")
                        print(f"    -> Tournament: {tournament}")
                        print(f"    -> Event Type: {event_type}")
                        print(f"    -> Map Veto: Winner={map_veto['winner_map']}, Loser={map_veto['loser_map']}, Decider={map_veto['decider']}")
                        print(f"    -> Head-to-Head: Winner={head2head['winner_head2head_freq']}, Loser={head2head['loser_head2head_freq']}")
                        print(f"    -> Past 3 Months: Winner={past3_data['winner_past3']}%, Loser={past3_data['loser_past3']}%")
                        
                        # Get team URLs
                        team1_url, team2_url = self.scrape_team_urls(match_info["match_url"])
                        if not team1_url or not team2_url:
                            print(f"  -> Failed to get team URLs, skipping...")
                            continue
                        
                        # Scrape team 1 players
                        print(f"    Scraping team 1 players...")
                        team1_players = self.scrape_team_players(team1_url, match_info["team1_name"])
                        if len(team1_players) < 5:
                            print(f"  -> Only found {len(team1_players)} players for {match_info['team1_name']}, skipping...")
                            continue
                        
                        # Scrape team 2 players
                        print(f"    Scraping team 2 players...")
                        team2_players = self.scrape_team_players(team2_url, match_info["team2_name"])
                        if len(team2_players) < 5:
                            print(f"  -> Only found {len(team2_players)} players for {match_info['team2_name']}, skipping...")
                            continue
                        
                        # Scrape individual player stats
                        print(f"    Scraping player statistics...")
                        for player in team1_players:
                            player["statistics"] = self.scrape_player_stats(player["stats_url"], player["name"])
                            time.sleep(self.player_stat_delay)
                        
                        for player in team2_players:
                            player["statistics"] = self.scrape_player_stats(player["stats_url"], player["name"])
                            time.sleep(self.player_stat_delay)
                        
                        # Calculate team averages
                        team1_averages = self.calculate_team_averages(team1_players)
                        team2_averages = self.calculate_team_averages(team2_players)
                        
                        # Calculate head-to-head percentages
                        winner_h2h_freq = head2head["winner_head2head_freq"]
                        loser_h2h_freq = head2head["loser_head2head_freq"]
                        
                        winner_h2h_percentage = 0.0
                        loser_h2h_percentage = 0.0
                        
                        if winner_h2h_freq is not None and loser_h2h_freq is not None:
                            total_maps = winner_h2h_freq + loser_h2h_freq
                            if total_maps > 0:
                                winner_h2h_percentage = round((winner_h2h_freq / total_maps) * 100, 2)
                                loser_h2h_percentage = round((loser_h2h_freq / total_maps) * 100, 2)
                            elif winner_h2h_freq == 0 and loser_h2h_freq == 0:
                                # If both teams have 0 map wins, set both percentages to 50%
                                winner_h2h_percentage = 50.0
                                loser_h2h_percentage = 50.0
                        else:
                            # If no head-to-head data available, set both percentages to 0
                            winner_h2h_percentage = 0.0
                            loser_h2h_percentage = 0.0
                        
                        # Structure the match data
                        match_data = {
                            "match_id": f"hltv_match_{match_info['match_id']}",
                            "hltv_match_id": match_info['match_id'],
                            "date": match_date,
                            "tournament": tournament,
                            "winner": match_info["winner"],
                            "season": current_season,
                            "score": {
                                "team1": match_info["team1_score"],
                                "team2": match_info["team2_score"]
                            },
                            "map_veto": {
                                "winner_map": map_veto["winner_map"],
                                "loser_map": map_veto["loser_map"],
                                "decider": map_veto["decider"]
                            },
                            "head_to_head": {
                                "winner_head2head_freq": winner_h2h_freq,
                                "loser_head2head_freq": loser_h2h_freq,
                                "winner_head2head_percentage": winner_h2h_percentage,
                                "loser_head2head_percentage": loser_h2h_percentage
                            },
                            "past_3_months": {
                                "winner_past3": past3_data["winner_past3"],
                                "loser_past3": past3_data["loser_past3"]
                            },
                            "map_winrates": map_winrates,
                            "team1": {
                                "name": match_info["team1_name"],
                                "team_averages": team1_averages,
                                "players": [
                                    {
                                        "name": player["name"],
                                        "statistics": player["statistics"]
                                    }
                                    for player in team1_players
                                ]
                            },
                            "team2": {
                                "name": match_info["team2_name"],
                                "team_averages": team2_averages,
                                "players": [
                                    {
                                        "name": player["name"],
                                        "statistics": player["statistics"]
                                    }
                                    for player in team2_players
                                ]
                            },
                            "metadata": {
                                "match_type": "bo3",
                                "event_type": event_type,
                                "scraped_date": datetime.now().isoformat() + "Z",
                                "hltv_url": match_info["match_url"]
                            }
                        }
                        
                        all_matches.append(match_data)
                        matches_found += 1
                        print(f"  ✅ Successfully completed match {match_number}/{self.num_matches}")
                        
                    except Exception as e:
                        print(f"  ❌ Error processing match {match_number}: {e}")
                        continue
                
                if matches_found < self.num_matches:
                    print(f"Page {page_number} completed: {matches_found} matches found so far")
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
        
        # Save final progress
        self.save_progress()
        
        print(f"\n📊 Enhanced Scraping Summary:")
        print(f"  • Matches scraped: {len(matches)}")
        print(f"  • Total matches processed: {self.match_counter}")
        print(f"  • Final season: {self.get_current_season()}")
        print(f"  • JSON file: {json_file}")
        print(f"  • Enhanced data: Date, Tournament, LAN/Online status")

def main():
    """Main function with command line argument handling"""
    parser = argparse.ArgumentParser(description="HLTV Enhanced Scraper - Scrape matches with enhanced information")
    parser.add_argument('--target_match_id', '-t', type=int, required=True,
                       help='Target match ID to scrape backwards from')
    parser.add_argument('--num_matches', '-n', type=int, default=3,
                       help='Number of matches to scrape')
    parser.add_argument('--output_dir', '-o', type=str, default='data/enhanced',
                       help='Output directory for enhanced matches')
    parser.add_argument('--pause', action='store_true',
                       help='Create a pause file to stop scraping gracefully')
    
    args = parser.parse_args()
    
    if args.pause:
        # Create pause file
        scraper = HLTVEnhancedScraper(args.target_match_id, args.num_matches, args.output_dir)
        scraper.create_pause_file()
    else:
        scraper = HLTVEnhancedScraper(args.target_match_id, args.num_matches, args.output_dir)
        scraper.run()

if __name__ == "__main__":
    main()
