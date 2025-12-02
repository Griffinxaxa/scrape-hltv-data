#!/usr/bin/env python3
"""
HLTV Round-by-Round Map Scraper

This script scrapes round-by-round data from map stats pages for each match.
Extracts starting sides, round winners, and handles edge cases.

Usage:
    python hltv_round_by_round_scraper.py --snapshot_file data/match_snapshot.json --num_matches 5
"""

import json
import os
import argparse
import time
from datetime import datetime
from typing import Dict, List, Any, Optional
import cloudscraper
from bs4 import BeautifulSoup
import pandas as pd

class HLTVRoundByRoundScraper:
    def __init__(self, snapshot_file: str, num_matches: int = 5, output_dir: str = "data/round_by_round"):
        self.snapshot_file = snapshot_file
        self.num_matches = num_matches
        self.output_dir = output_dir
        self.snapshot_data = None
        self.snapshot_index = 0
        self.base_url = "https://www.hltv.org"
        
        # Delays for respectful scraping
        self.page_delay = 0.3
        self.match_delay = 0.1
        
        # Create cloudscraper session
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
        
        # Progress tracking
        self.progress_file = os.path.join(self.output_dir, 'scraper_progress.json')
        self.load_progress()
        
        # Load snapshot
        self.load_snapshot()
    
    def load_progress(self):
        """Load progress from previous run"""
        try:
            if os.path.exists(self.progress_file):
                with open(self.progress_file, 'r') as f:
                    progress = json.load(f)
                    self.snapshot_index = progress.get('snapshot_index', 0)
                    print(f"📊 Resuming from match #{self.snapshot_index + 1}")
            else:
                self.snapshot_index = 0
        except Exception as e:
            print(f"⚠️ Error loading progress: {e}")
            self.snapshot_index = 0
    
    def save_progress(self):
        """Save current progress"""
        try:
            progress = {
                'snapshot_index': self.snapshot_index,
                'timestamp': datetime.now().isoformat()
            }
            with open(self.progress_file, 'w') as f:
                json.dump(progress, f)
        except Exception as e:
            print(f"⚠️ Error saving progress: {e}")
    
    def load_snapshot(self):
        """Load match snapshot from JSON file"""
        try:
            if os.path.exists(self.snapshot_file):
                with open(self.snapshot_file, 'r') as f:
                    snapshot = json.load(f)
                    self.snapshot_data = snapshot.get('matches', [])
                    print(f"📸 Snapshot loaded: {len(self.snapshot_data)} match IDs")
            else:
                print(f"⚠️ Snapshot file not found: {self.snapshot_file}")
                self.snapshot_data = None
        except Exception as e:
            print(f"⚠️ Error loading snapshot: {e}")
            self.snapshot_data = None
    
    def get_page_content(self, url: str, max_retries: int = 3) -> Optional[BeautifulSoup]:
        """Get page content using cloudscraper with retry logic"""
        for attempt in range(max_retries):
            try:
                response = self.session.get(url, timeout=30)
                if response.status_code == 429:
                    wait_time = (attempt + 1) * 10
                    print(f"Rate limited, waiting {wait_time} seconds...")
                    time.sleep(wait_time)
                    continue
                response.raise_for_status()
                return BeautifulSoup(response.content, 'html.parser')
            except Exception as e:
                if attempt == max_retries - 1:
                    return None
                time.sleep((attempt + 1) * 5)
        return None
    
    def extract_match_info(self, match_url: str, match_id: int = None) -> Optional[Dict[str, Any]]:
        """Extract basic match info from match page"""
        try:
            soup = self.get_page_content(match_url)
            # If match page doesn't work, try results page
            if not soup and match_id:
                results_url = f"{self.base_url}/results?offset={match_id}"
                soup = self.get_page_content(results_url)
                # If results page works, try to find the match link
                if soup:
                    match_link = soup.select_one(f'a[href*="/matches/{match_id}/"]')
                    if match_link:
                        href = match_link.get('href', '')
                        if not href.startswith('http'):
                            href = self.base_url + href
                        soup = self.get_page_content(href)
            
            if not soup:
                return None
            
            # Get date
            time_elem = soup.select_one('.time[data-unix]')
            date = None
            if time_elem:
                unix_timestamp = time_elem.get('data-unix')
                if unix_timestamp:
                    timestamp = int(unix_timestamp) / 1000
                    date_obj = datetime.fromtimestamp(timestamp)
                    date = date_obj.isoformat() + "Z"
            
            # Get event name
            event_elem = soup.select_one('.event.text-ellipsis')
            event_name = event_elem.get_text().strip() if event_elem else None
            
            # Get teams and score
            team1_elem = soup.select_one('.team1-gradient .teamName')
            team2_elem = soup.select_one('.team2-gradient .teamName')
            team1_name = team1_elem.get_text().strip() if team1_elem else None
            team2_name = team2_elem.get_text().strip() if team2_elem else None
            
            # Get BO3 match score (e.g., "1:2" means team1 won 1 map, team2 won 2 maps)
            score_elem = soup.select_one('.score')
            team1_score = None
            team2_score = None
            winner_side = None
            
            if score_elem:
                score_text = score_elem.get_text().strip()
                # Handle both "1:2" and "1-2" formats
                if ':' in score_text:
                    try:
                        scores = [int(x.strip()) for x in score_text.split(':')]
                        if len(scores) == 2:
                            team1_score = scores[0]
                            team2_score = scores[1]
                            if team1_score > team2_score:
                                winner_side = "team1"
                            elif team2_score > team1_score:
                                winner_side = "team2"
                    except:
                        pass
                elif '-' in score_text:
                    try:
                        scores = [int(x.strip()) for x in score_text.split('-')]
                        if len(scores) == 2:
                            team1_score = scores[0]
                            team2_score = scores[1]
                            if team1_score > team2_score:
                                winner_side = "team1"
                            elif team2_score > team1_score:
                                winner_side = "team2"
                    except:
                        pass
            
            # Find map stats links
            map_stats_links = []
            for link in soup.select('a.results-stats'):
                href = link.get('href', '')
                if '/stats/matches/mapstatsid/' in href:
                    if not href.startswith('http'):
                        href = self.base_url + href
                    map_stats_links.append(href)
            
            return {
                'date': date,
                'event_name': event_name,
                'team1_name': team1_name,
                'team2_name': team2_name,
                'team1_score': team1_score,
                'team2_score': team2_score,
                'winner_side': winner_side,
                'match_url': match_url,
                'map_stats_links': map_stats_links
            }
        except Exception as e:
            print(f"⚠️ Error extracting match info: {e}")
            return None
    
    def extract_map_round_data(self, map_stats_url: str, team1_name: str, team2_name: str) -> Optional[Dict[str, Any]]:
        """Extract round-by-round data from a map stats page"""
        try:
            soup = self.get_page_content(map_stats_url)
            if not soup:
                return None
            
            # Find round history rows (2 rows: one for each team)
            round_rows = soup.select('div.round-history-team-row')
            if len(round_rows) < 2:
                return None
            
            # CT win images
            ct_win_images = [
                '/img/static/scoreboard/bomb_defused.svg',
                '/img/static/scoreboard/ct_win.svg',
                '/img/static/scoreboard/stopwatch.svg'
            ]
            # T win images
            t_win_images = [
                '/img/static/scoreboard/t_win.svg',
                '/img/static/scoreboard/bomb_exploded.svg'
            ]
            empty_image = '/img/static/scoreboard/emptyHistory.svg'
            
            # Get images from both team rows (skip first image which is team logo)
            team1_row = round_rows[0]
            team2_row = round_rows[1]
            
            team1_images = [img.get('src', '') for img in team1_row.select('img')[1:]]  # Skip logo
            team2_images = [img.get('src', '') for img in team2_row.select('img')[1:]]  # Skip logo
            
            # Ensure we have at least 24 rounds
            max_rounds = min(len(team1_images), len(team2_images), 24)
            
            rounds_data = []
            team1_startside = None
            team2_startside = None
            
            # Process rounds
            for round_num in range(1, max_rounds + 1):
                idx = round_num - 1
                team1_img = team1_images[idx] if idx < len(team1_images) else empty_image
                team2_img = team2_images[idx] if idx < len(team2_images) else empty_image
                
                # Determine round winner
                round_winner = None
                # Normalize image paths (handle both full URLs and relative paths)
                team1_img_normalized = team1_img if team1_img else empty_image
                team2_img_normalized = team2_img if team2_img else empty_image
                
                # Check if team1 won (has a win image) - check for win image patterns
                team1_won = (team1_img_normalized != empty_image and 
                            (any(win_img in team1_img_normalized for win_img in ct_win_images) or 
                             any(win_img in team1_img_normalized for win_img in t_win_images) or
                             't_win' in team1_img_normalized or 'ct_win' in team1_img_normalized or
                             'bomb_exploded' in team1_img_normalized or 'bomb_defused' in team1_img_normalized or
                             'stopwatch' in team1_img_normalized))
                # Check if team2 won (has a win image)
                team2_won = (team2_img_normalized != empty_image and 
                            (any(win_img in team2_img_normalized for win_img in ct_win_images) or 
                             any(win_img in team2_img_normalized for win_img in t_win_images) or
                             't_win' in team2_img_normalized or 'ct_win' in team2_img_normalized or
                             'bomb_exploded' in team2_img_normalized or 'bomb_defused' in team2_img_normalized or
                             'stopwatch' in team2_img_normalized))
                
                # Determine winner: if one team has a win image, they won
                # If one has empty and other has win, the one with win won
                # If both have win images, prefer the one that's not empty (shouldn't happen but handle it)
                if team1_won and not team2_won:
                    round_winner = "team1"
                elif team2_won and not team1_won:
                    round_winner = "team2"
                elif team1_won and team2_won:
                    # Both have win images - this is unusual, prefer team1 (first team)
                    # In practice, this might indicate a data issue, but we'll assign to team1
                    round_winner = "team1"
                elif team1_img_normalized == empty_image and team2_img_normalized != empty_image:
                    # Team1 lost (empty), team2 has something (win image)
                    round_winner = "team2"
                elif team2_img_normalized == empty_image and team1_img_normalized != empty_image:
                    # Team2 lost (empty), team1 has something (win image)
                    round_winner = "team1"
                elif team1_img_normalized == empty_image and team2_img_normalized == empty_image:
                    # Both empty - if both images are blank, team2 won the round
                    round_winner = "team2"
                else:
                    # Both have non-empty images but neither matched win patterns
                    # This is unusual, but try to determine based on which has a win image
                    if ('t_win' in team1_img_normalized or 'ct_win' in team1_img_normalized or
                        'bomb_exploded' in team1_img_normalized or 'bomb_defused' in team1_img_normalized or
                        'stopwatch' in team1_img_normalized):
                        round_winner = "team1"
                    elif ('t_win' in team2_img_normalized or 'ct_win' in team2_img_normalized or
                          'bomb_exploded' in team2_img_normalized or 'bomb_defused' in team2_img_normalized or
                          'stopwatch' in team2_img_normalized):
                        round_winner = "team2"
                    else:
                        round_winner = None
                
                # Determine starting side (before round 13)
                if round_num < 13 and team1_startside is None:
                    if team1_won and ('ct_win' in team1_img_normalized or 'bomb_defused' in team1_img_normalized or 'stopwatch' in team1_img_normalized):
                        team1_startside = "ct"
                        team2_startside = "t"
                    elif team2_won and ('ct_win' in team2_img_normalized or 'bomb_defused' in team2_img_normalized or 'stopwatch' in team2_img_normalized):
                        team2_startside = "ct"
                        team1_startside = "t"
                    elif team1_won and ('t_win' in team1_img_normalized or 'bomb_exploded' in team1_img_normalized):
                        team1_startside = "t"
                        team2_startside = "ct"
                    elif team2_won and ('t_win' in team2_img_normalized or 'bomb_exploded' in team2_img_normalized):
                        team2_startside = "t"
                        team1_startside = "ct"
                
                rounds_data.append({
                    'round': round_num,
                    'team1_img': team1_img,
                    'team2_img': team2_img,
                    'winner': round_winner
                })
            
            # Check for 12-0 or 0-12 halves (all empty or all wins)
            if len(rounds_data) >= 12:
                first_half_team1_wins = sum(1 for r in rounds_data[:12] if r['winner'] == 'team1')
                first_half_team2_wins = sum(1 for r in rounds_data[:12] if r['winner'] == 'team2')
                
                if first_half_team1_wins == 12 or first_half_team2_wins == 12:
                    return None  # Drop this map
            
            # Game ends when a team reaches 13
            game_ended_at = len(rounds_data)  # Default to all rounds
            running_team1 = 0
            running_team2 = 0
            for r in rounds_data:
                if r['winner'] == 'team1':
                    running_team1 += 1
                elif r['winner'] == 'team2':
                    running_team2 += 1
                
                if running_team1 >= 13 or running_team2 >= 13:
                    game_ended_at = r['round']
                    break
            
            # Determine end sides (opposite of start)
            team1_endside = "t" if team1_startside == "ct" else "ct" if team1_startside == "t" else None
            team2_endside = "t" if team2_startside == "ct" else "ct" if team2_startside == "t" else None
            
            return {
                'team1_startside': team1_startside,
                'team2_startside': team2_startside,
                'team1_endside': team1_endside,
                'team2_endside': team2_endside,
                'rounds_data': rounds_data,
                'game_ended_at': game_ended_at
            }
        except Exception as e:
            print(f"⚠️ Error extracting map round data: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def scrape_match_rounds(self, match_info: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Scrape round-by-round data for all maps in a match"""
        try:
            map_stats_links = match_info.get('map_stats_links', [])
            
            # Process up to 3 maps
            maps_data = []
            for map_num in range(1, 4):
                if map_num - 1 < len(map_stats_links):
                    map_url = map_stats_links[map_num - 1]
                    print(f"  📊 Processing Map {map_num}...")
                    map_data = self.extract_map_round_data(
                        map_url,
                        match_info['team1_name'],
                        match_info['team2_name']
                    )
                    
                    if map_data is None:
                        # Map was dropped (12-0 half) or doesn't exist
                        if map_num <= len(map_stats_links):
                            # Map exists but was dropped
                            return None
                        # Map doesn't exist, fill with NA
                        maps_data.append(None)
                    else:
                        maps_data.append(map_data)
                    time.sleep(self.page_delay)
                else:
                    # Map doesn't exist
                    maps_data.append(None)
            
            # Build result dictionary
            result = {
                'date': match_info.get('date'),
                'event_name': match_info.get('event_name'),
                'winner_side': match_info.get('winner_side'),
                'team1_name': match_info.get('team1_name'),
                'team2_name': match_info.get('team2_name'),
                'team1_score': match_info.get('team1_score'),
                'team2_score': match_info.get('team2_score'),
                'match_url': match_info.get('match_url'),
            }
            
            # Add map data
            for map_num in range(1, 4):
                map_data = maps_data[map_num - 1] if map_num - 1 < len(maps_data) else None
                
                if map_data:
                    result[f'map{map_num}_team1_startside'] = map_data['team1_startside']
                    result[f'map{map_num}_team2_startside'] = map_data['team2_startside']
                    
                    # Round winners (up to 24 rounds)
                    rounds_data = map_data['rounds_data']
                    game_ended_at = map_data.get('game_ended_at')
                    if game_ended_at is None:
                        game_ended_at = len(rounds_data)
                    
                    for round_num in range(1, 25):
                        if round_num <= len(rounds_data):
                            round_data = rounds_data[round_num - 1]
                            # Always include the winner if it exists in the data
                            # Only set to None if round is clearly after game ended AND has no winner
                            if round_data['winner'] is not None:
                                result[f'map{map_num}_round{round_num}_winner'] = round_data['winner']
                            elif round_num > game_ended_at:
                                # Round is after game ended and has no winner
                                result[f'map{map_num}_round{round_num}_winner'] = None
                            else:
                                # Round is within game but has no winner (shouldn't happen, but include it)
                                result[f'map{map_num}_round{round_num}_winner'] = round_data['winner']  # Will be None
                        else:
                            # Round doesn't exist in data
                            result[f'map{map_num}_round{round_num}_winner'] = None
                else:
                    # Map doesn't exist or was dropped
                    result[f'map{map_num}_team1_startside'] = None
                    result[f'map{map_num}_team2_startside'] = None
                    for round_num in range(1, 25):
                        result[f'map{map_num}_round{round_num}_winner'] = None
            
            return result
        except Exception as e:
            print(f"⚠️ Error scraping match rounds: {e}")
            return None
    
    def scrape_matches(self) -> List[Dict[str, Any]]:
        """Scrape round-by-round data for matches"""
        all_matches = []
        matches_found = 0
        valid_matches = 0
        
        print(f"📸 Starting round-by-round scraping")
        print(f"🎯 Target: {self.num_matches} matches")
        print(f"📊 Snapshot contains: {len(self.snapshot_data)} match IDs")
        print(f"📊 Starting from index: {self.snapshot_index}")
        
        try:
            while matches_found < self.num_matches and self.snapshot_index < len(self.snapshot_data):
                snapshot_match = self.snapshot_data[self.snapshot_index]
                match_id = snapshot_match['match_id']
                match_url = snapshot_match.get('url', f"{self.base_url}/matches/{match_id}/-")
                
                matches_found += 1
                
                print(f"\n🎯 Game #{matches_found} (Index {self.snapshot_index}) | Match ID {match_id} | {snapshot_match.get('team1', 'Unknown')} vs {snapshot_match.get('team2', 'Unknown')}")
                
                # Extract match info
                match_info = self.extract_match_info(match_url, match_id)
                if not match_info:
                    print(f"  ⚠️ Skipped - couldn't extract match info")
                    self.snapshot_index += 1
                    self.save_progress()
                    continue
                
                time.sleep(self.match_delay)
                
                # Scrape round-by-round data
                round_data = self.scrape_match_rounds(match_info)
                if not round_data:
                    print(f"  ⚠️ Skipped - map had 12-0 half or error")
                    self.snapshot_index += 1
                    self.save_progress()
                    continue
                
                all_matches.append(round_data)
                valid_matches += 1
                self.snapshot_index += 1
                print(f"  ✅ Successfully scraped round data (Valid: {valid_matches})")
                
                # Save progress every match
                self.save_progress()
                
                # Save checkpoint every 100 valid matches
                if valid_matches > 0 and valid_matches % 100 == 0:
                    checkpoint_file = os.path.join(self.output_dir, f"round_by_round_checkpoint_{valid_matches}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
                    df = pd.DataFrame(all_matches)
                    df.to_csv(checkpoint_file, index=False)
                    print(f"  💾 Checkpoint saved: {checkpoint_file} ({valid_matches} matches)")
                
                time.sleep(self.match_delay)
            
            return all_matches
        except Exception as e:
            print(f"❌ Error during scraping: {e}")
            import traceback
            traceback.print_exc()
            # Save progress on error
            self.save_progress()
            return all_matches
    
    def save_to_csv(self, matches: List[Dict[str, Any]]) -> str:
        """Save matches to CSV file"""
        if not matches:
            print("⚠️ No matches to save")
            return None
        
        df = pd.DataFrame(matches)
        output_file = os.path.join(self.output_dir, f"round_by_round_matches_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
        df.to_csv(output_file, index=False)
        
        print(f"\n✅ Round-by-round matches saved to {output_file}")
        print(f"📊 Total matches: {len(matches)}")
        return output_file

def main():
    parser = argparse.ArgumentParser(description="HLTV Round-by-Round Scraper")
    parser.add_argument('--snapshot_file', '-s', type=str, required=True,
                       help='Path to snapshot JSON file containing match IDs')
    parser.add_argument('--num_matches', '-n', type=int, default=5,
                       help='Number of matches to scrape (default: 5)')
    parser.add_argument('--output_dir', '-o', type=str, default='data/round_by_round',
                       help='Output directory (default: data/round_by_round)')
    
    args = parser.parse_args()
    
    scraper = HLTVRoundByRoundScraper(args.snapshot_file, args.num_matches, args.output_dir)
    matches = scraper.scrape_matches()
    
    if matches:
        scraper.save_to_csv(matches)
    else:
        print("❌ No matches scraped")

if __name__ == "__main__":
    main()

