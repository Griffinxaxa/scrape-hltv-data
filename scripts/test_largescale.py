#!/usr/bin/env python3
"""
Test version of the large scale scraper with only 10 matches
"""

import json
import re
import time
from datetime import datetime
from typing import Dict, List, Optional, Any
import cloudscraper
from bs4 import BeautifulSoup
import requests


class HLTVTestLargeScaleScraper:
    def __init__(self, target_matches: int = 10):
        self.base_url = "https://www.hltv.org"
        self.results_url = f"{self.base_url}/results"
        self.target_matches = target_matches
        self.matches_per_page = 100
        self.total_pages = (target_matches + self.matches_per_page - 1) // self.matches_per_page
        self.match_data = {}
        self.scraped_count = 0
        self.failed_matches = []
        
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
        
    def safe_float(self, text: str) -> Optional[float]:
        """Safely convert text to float, return None if conversion fails"""
        if not text or text.strip() == "":
            return None
        try:
            # Remove any non-numeric characters except decimal point and minus
            cleaned = re.sub(r'[^\d.-]', '', text.strip())
            return float(cleaned) if cleaned else None
        except (ValueError, TypeError):
            return None
    
    def extract_player_id_and_name(self, href: str) -> tuple[Optional[str], Optional[str]]:
        """Extract player ID and name from href like '/player/12345/player-name'"""
        try:
            # Pattern: /player/ID/NAME
            match = re.search(r'/player/(\d+)/([^/]+)', href)
            if match:
                return match.group(1), match.group(2)
        except Exception as e:
            print(f"Error extracting player info from {href}: {e}")
        return None, None
    
    def get_page_content(self, url: str) -> BeautifulSoup:
        """Get page content using cloudscraper to bypass Cloudflare"""
        print(f"Fetching: {url}")
        
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            # Check if we got a Cloudflare challenge page
            if 'challenge' in response.text.lower() or 'cloudflare' in response.text.lower():
                print("⚠️  Cloudflare challenge detected, retrying...")
                time.sleep(5)
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
            
            return BeautifulSoup(response.content, 'html.parser')
            
        except Exception as e:
            print(f"Error fetching {url}: {e}")
            raise

    def get_page_url(self, page: int) -> str:
        """Get the URL for a specific page with offset"""
        if page == 0:
            return self.results_url
        else:
            offset = page * self.matches_per_page
            return f"{self.results_url}?offset={offset}"

    def scrape_page_matches(self, page: int) -> List[Dict[str, Any]]:
        """Scrape all matches from a specific page"""
        print(f"\n=== Scraping Page {page + 1}/{self.total_pages} ===")
        
        page_url = self.get_page_url(page)
        soup = self.get_page_content(page_url)
        
        # Find all matches on this page
        all_matches = soup.select('.result-con')
        if not all_matches:
            print(f"No matches found on page {page + 1}")
            return []
        
        print(f"Found {len(all_matches)} matches on page {page + 1}")
        
        matches_data = []
        matches_processed = 0
        
        for i, match in enumerate(all_matches):
            # Stop if we've reached our target
            if self.scraped_count >= self.target_matches:
                print(f"Reached target of {self.target_matches} matches, stopping...")
                break
                
            print(f"Processing match {self.scraped_count + 1}/{self.target_matches} (Page {page + 1}, Match {i + 1})...")
            
            try:
                match_info = self.extract_match_info(match, self.scraped_count + 1)
                if match_info:
                    matches_data.append(match_info)
                    self.scraped_count += 1
                    matches_processed += 1
                    print(f"  -> Successfully processed: {match_info['team1_name']} vs {match_info['team2_name']}")
                else:
                    print(f"  -> Failed to extract match info for match {self.scraped_count + 1}")
                    self.failed_matches.append(self.scraped_count + 1)
            except Exception as e:
                print(f"  -> Error processing match {self.scraped_count + 1}: {e}")
                self.failed_matches.append(self.scraped_count + 1)
                continue
        
        print(f"Page {page + 1} completed: {matches_processed} matches processed")
        return matches_data

    def extract_match_info(self, match_element, match_number: int) -> Optional[Dict[str, Any]]:
        """Extract basic match information from a single match element"""
        try:
            # Extract team names - try multiple selectors
            team1_name = ""
            team2_name = ""
            
            # Try different selectors for team names
            team1_selectors = ['.team1 .team-name', '.team1 .team', '.team1 a', '.team1']
            team2_selectors = ['.team2 .team-name', '.team2 .team', '.team2 a', '.team2']
            
            for selector in team1_selectors:
                elem = match_element.select_one(selector)
                if elem and elem.get_text().strip():
                    team1_name = elem.get_text().strip()
                    break
            
            for selector in team2_selectors:
                elem = match_element.select_one(selector)
                if elem and elem.get_text().strip():
                    team2_name = elem.get_text().strip()
                    break
            
            # If still empty, try to extract from the match URL
            if not team1_name or not team2_name:
                match_url = match_element.select_one('a')
                if match_url:
                    href = match_url.get('href', '')
                    # Extract team names from URL like /matches/123/team1-vs-team2
                    if 'vs' in href:
                        url_parts = href.split('/')[-1].split('-vs-')
                        if len(url_parts) == 2:
                            team1_name = team1_name or url_parts[0].replace('-', ' ').title()
                            team2_name = team2_name or url_parts[1].replace('-', ' ').title()
            
            # Extract score
            score_elem = match_element.select_one('.result-score')
            score_text = score_elem.get_text().strip() if score_elem else "0 - 0"
            
            # Parse score
            score_match = re.search(r'(\d+)\s*-\s*(\d+)', score_text)
            if score_match:
                team1_score = int(score_match.group(1))
                team2_score = int(score_match.group(2))
            else:
                team1_score = team2_score = 0
            
            # Determine winner
            winner = "team1" if team1_score > team2_score else "team2" if team2_score > team1_score else "draw"
            
            # Get match page URL
            match_link = match_element.select_one('a')
            match_href = match_link.get('href') if match_link else ""
            match_url = f"{self.base_url}{match_href}" if match_href else ""
            
            return {
                "match_number": match_number,
                "team1_name": team1_name,
                "team2_name": team2_name,
                "team1_score": team1_score,
                "team2_score": team2_score,
                "winner": winner,
                "match_url": match_url
            }
            
        except Exception as e:
            print(f"Error extracting match info for match {match_number}: {e}")
            return None

    def scrape_team_urls(self, match_url: str) -> tuple[str, str]:
        """Scrape team page URLs from match page"""
        soup = self.get_page_content(match_url)
        
        team1_link = soup.select_one('.team1-gradient a')
        team2_link = soup.select_one('.team2-gradient a')
        
        team1_url = f"{self.base_url}{team1_link.get('href')}" if team1_link else ""
        team2_url = f"{self.base_url}{team2_link.get('href')}" if team2_link else ""
        
        return team1_url, team2_url
    
    def scrape_team_players(self, team_url: str, team_name: str) -> List[Dict[str, Any]]:
        """Scrape player information from team page"""
        soup = self.get_page_content(team_url)
        
        players = []
        player_links = soup.select('.bodyshot-team a')
        
        for link in player_links:
            href = link.get('href')
            if href and '/player/' in href:
                player_id, player_name = self.extract_player_id_and_name(href)
                if player_id and player_name:
                    players.append({
                        "id": player_id,
                        "name": player_name,
                        "stats_url": f"{self.base_url}/stats/players/{player_id}/{player_name}"
                    })
        
        return players

    def scrape_player_stats(self, stats_url: str, player_name: str) -> Dict[str, Optional[float]]:
        """Scrape individual player statistics"""
        try:
            soup = self.get_page_content(stats_url)
            
            stats = {}
            
            # Primary method: Look for the specific class you mentioned
            stat_boxes = soup.find_all('div', class_='player-summary-stat-box-data traditionalData')
            
            if len(stat_boxes) >= 5:
                # Map boxes correctly: 0=DPR, 1=KAST, 3=ADR, 4=KPR (skip box 2 which is multi-kill)
                for i, box in enumerate(stat_boxes):
                    box_text = box.get_text().strip()
                    
                    # Map by position and content (skip box 2 which is multi-kill)
                    if i == 0:  # DPR (Damage per Round)
                        if box_text != '-' and box_text:
                            stats["DPR"] = self.safe_float(box_text)
                    elif i == 1:  # KAST (percentage)
                        if box_text != '-' and box_text and '%' in box_text:
                            # Remove % and convert to decimal
                            kast_value = box_text.replace('%', '')
                            stats["KAST"] = self.safe_float(kast_value)
                    elif i == 3:  # ADR (Average Damage per Round)
                        if box_text != '-' and box_text:
                            stats["ADR"] = self.safe_float(box_text)
                    elif i == 4:  # KPR (Kills per Round)
                        if box_text != '-' and box_text:
                            stats["KPR"] = self.safe_float(box_text)
                    # Skip i == 2 (multi-kill data)
            
            # Get RATING from the separate class
            rating_elem = soup.find('div', class_='player-summary-stat-box-rating-data-text')
            if rating_elem:
                rating_text = rating_elem.get_text().strip()
                if rating_text != '-' and rating_text:
                    stats["RATING"] = self.safe_float(rating_text)
            
            # Ensure all stats are present
            for stat in ["DPR", "KAST", "ADR", "KPR", "RATING"]:
                if stat not in stats:
                    stats[stat] = None
            
            return stats
            
        except Exception as e:
            print(f"Error scraping stats for {player_name}: {e}")
            return {"DPR": None, "KAST": None, "ADR": None, "KPR": None, "RATING": None}

    def process_match_detailed_data(self, match_info: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Process detailed data for a single match (teams, players, stats)"""
        try:
            print(f"  Processing detailed data for: {match_info['team1_name']} vs {match_info['team2_name']}")
            
            # Get team URLs
            team1_url, team2_url = self.scrape_team_urls(match_info["match_url"])
            
            # Scrape team 1 players
            team1_players = self.scrape_team_players(team1_url, match_info["team1_name"])
            
            # Scrape team 2 players
            team2_players = self.scrape_team_players(team2_url, match_info["team2_name"])
            
            # Scrape individual player stats
            print(f"    Scraping stats for {len(team1_players)} players from {match_info['team1_name']}...")
            for player in team1_players:
                player["statistics"] = self.scrape_player_stats(player["stats_url"], player["name"])
                time.sleep(0.5)  # Reduced delay for large scale
            
            print(f"    Scraping stats for {len(team2_players)} players from {match_info['team2_name']}...")
            for player in team2_players:
                player["statistics"] = self.scrape_player_stats(player["stats_url"], player["name"])
                time.sleep(0.5)  # Reduced delay for large scale
            
            # Structure the match data
            match_data = {
                "match_id": f"hltv_match_{match_info['match_number']}",
                "date": "",
                "tournament": "",
                "winner": match_info["winner"],
                "score": {
                    "team1": match_info["team1_score"],
                    "team2": match_info["team2_score"]
                },
                "team1": {
                    "name": match_info["team1_name"],
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
                    "players": [
                        {
                            "name": player["name"],
                            "statistics": player["statistics"]
                        }
                        for player in team2_players
                    ]
                },
                "metadata": {
                    "match_type": "bo1",
                    "event_type": "lan",
                    "scraped_date": datetime.now().isoformat() + "Z",
                    "hltv_url": match_info["match_url"],
                    "match_number": match_info["match_number"]
                }
            }
            
            return match_data
            
        except Exception as e:
            print(f"    Error processing detailed data for match {match_info['match_number']}: {e}")
            return None

    def scrape_test_data(self) -> Dict[str, Any]:
        """Test method to scrape a small number of matches"""
        try:
            print(f"Starting test scraping for {self.target_matches} matches...")
            
            all_matches_data = []
            
            # Process each page
            for page in range(self.total_pages):
                if self.scraped_count >= self.target_matches:
                    break
                    
                # Scrape basic match info from this page
                page_matches = self.scrape_page_matches(page)
                
                # Process detailed data for each match
                for match_info in page_matches:
                    if self.scraped_count > self.target_matches:
                        break
                        
                    detailed_match = self.process_match_detailed_data(match_info)
                    if detailed_match:
                        all_matches_data.append(detailed_match)
                        print(f"  ✅ Successfully completed match {len(all_matches_data)}/{self.target_matches}")
                    else:
                        print(f"  ❌ Failed to process detailed data for match {match_info['match_number']}")
                
                # Add delay between pages to be respectful
                if page < self.total_pages - 1:
                    print(f"Waiting 2 seconds before next page...")
                    time.sleep(2)
            
            self.match_data = {
                "scraping_session": {
                    "target_matches": self.target_matches,
                    "total_matches_scraped": len(all_matches_data),
                    "failed_matches": len(self.failed_matches),
                    "failed_match_numbers": self.failed_matches,
                    "scraped_date": datetime.now().isoformat() + "Z"
                },
                "matches": all_matches_data
            }
            
            return self.match_data
            
        except Exception as e:
            print(f"Error during test scraping: {e}")
            raise
    
    def save_to_file(self, filename: str = "data/test/hltv_test_data.json"):
        """Save scraped data to JSON file"""
        if not self.match_data:
            print("No data to save. Run scrape_test_data() first.")
            return
        
        # Ensure directory exists
        import os
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(self.match_data, f, indent=2, ensure_ascii=False)
        
        print(f"Data saved to {filename}")
        print(f"Total matches scraped: {self.match_data['scraping_session']['total_matches_scraped']}")
        print(f"Failed matches: {self.match_data['scraping_session']['failed_matches']}")


def main():
    """Main function to run the test scraper"""
    target_matches = 10
    
    scraper = HLTVTestLargeScaleScraper(target_matches=target_matches)
    
    try:
        print(f"Starting HLTV test scraping for {target_matches} matches...")
        
        match_data = scraper.scrape_test_data()
        scraper.save_to_file()
        print("Test scraping completed successfully!")
        
    except Exception as e:
        print(f"Test scraping failed: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())
