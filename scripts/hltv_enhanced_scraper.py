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
            team1_elem = match_element.select_one('.team1 .team-name')
            team2_elem = match_element.select_one('.team2 .team-name')
            
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
                    if matches_found >= self.num_matches:
                        print(f"Reached target of {self.num_matches} matches, stopping...")
                        break
                    
                    match_number = matches_found + 1
                    print(f"\nProcessing match {match_number}/{self.num_matches} (Page {page_number}, Match {i+1})...")
                    
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
                        
                        print(f"    -> Date: {match_date}")
                        print(f"    -> Tournament: {tournament}")
                        print(f"    -> Event Type: {event_type}")
                        
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
                        
                        # Structure the match data
                        match_data = {
                            "match_id": f"hltv_match_{match_info['match_id']}",
                            "hltv_match_id": match_info['match_id'],
                            "date": match_date,
                            "tournament": tournament,
                            "winner": match_info["winner"],
                            "score": {
                                "team1": match_info["team1_score"],
                                "team2": match_info["team2_score"]
                            },
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
        
        print(f"\n📊 Enhanced Scraping Summary:")
        print(f"  • Matches scraped: {len(matches)}")
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
    
    args = parser.parse_args()
    
    scraper = HLTVEnhancedScraper(args.target_match_id, args.num_matches, args.output_dir)
    scraper.run()

if __name__ == "__main__":
    main()
