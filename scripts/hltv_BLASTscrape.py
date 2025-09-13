#!/usr/bin/env python3
"""
HLTV Match Data Scraper using cfscrape to bypass Cloudflare
"""

import json
import re
import time
from datetime import datetime
from typing import Dict, List, Optional, Any
import cloudscraper
from bs4 import BeautifulSoup
import requests


class HLTVCFScraper:
    def __init__(self):
        self.base_url = "https://www.hltv.org"
        self.results_url = f"{self.base_url}/results?event=7912"
        self.match_data = {}
        # No forfeit detection needed for T1 event
        
        # Create cloudscraper session to handle Cloudflare
        self.session = cloudscraper.create_scraper()
        self.session.headers.update({ #YOU WILL AMOST CERTAINLY NEED TO UPDATE THIS TO WHATEVERY BROWSER YOU ARE USING
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
        #WE CAN IGNORE FORFEITS FOR THIS EVENT BECAUSE IT WAS TIER 1 AND OBVIOUSLY NO FORFEITS SO JUST COMMENT OUT THE FUNCTION
    
    # def is_match_forfeited(self, match_element) -> bool:
    #     """Check if a match was forfeited (1-0 score with no maps played)"""
    #     try:
    #         # Check the score
    #         score_elem = match_element.select_one('.result-score')
    #         if not score_elem:
    #             return True
            
    #         score_text = score_elem.get_text().strip()
            
    #         # Parse score
    #         score_match = re.search(r'(\d+)\s*-\s*(\d+)', score_text)
    #         if not score_match:
    #             return True
            
    #         team1_score = int(score_match.group(1))
    #         team2_score = int(score_match.group(2))
            
            
    #             # Look for explicit forfeit indicators in the match text
    #         match_text = match_element.get_text().lower()
    #         if 'forfeit' in match_text or 'ff' in match_text or 'wo' in match_text or 'walkover' in match_text:
    #             print(f"  -> Forfeit detected in text: {match_text[:100]}...")
    #             return True
                
    #             # For now, let's be less strict and only flag obvious forfeits
    #             # We'll rely more on the match page verification
    #             print(f"  -> 1-0 score detected, will verify on match page")
    #             return False
            
    #         # If score is not 1-0 or 0-1, it's definitely not a forfeit
    #         return False
            
    #     except Exception as e:
    #         print(f"Error checking if match is forfeited: {e}")
    #         return True  # Assume forfeited if we can't determine

    def scrape_all_matches_basic_info(self) -> List[Dict[str, Any]]:
        """Scrape basic match information for all matches in the event"""
        print("Scraping basic match information for all matches...")
        
        soup = self.get_page_content(self.results_url)
        
        # Find all matches
        all_matches = soup.select('.result-con')
        if not all_matches:
            raise Exception("No matches found on results page")
        
        print(f"Found {len(all_matches)} matches on results page")
        
        matches_data = []
        
        # Process all matches (no forfeit detection for T1 event)
        for i, match in enumerate(all_matches):
            print(f"Processing match {i+1}/{len(all_matches)}...")
            
            try:
                match_info = self.extract_match_info(match, i+1)
                if match_info:
                    matches_data.append(match_info)
                    print(f"  -> Successfully processed: {match_info['team1_name']} vs {match_info['team2_name']}")
                else:
                    print(f"  -> Failed to extract match info for match {i+1}")
            except Exception as e:
                print(f"  -> Error processing match {i+1}: {e}")
                continue
        
        print(f"Successfully processed {len(matches_data)} matches")
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
        print("Scraping team URLs from match page...")
        
        soup = self.get_page_content(match_url)
        
        team1_link = soup.select_one('.team1-gradient a')
        team2_link = soup.select_one('.team2-gradient a')
        
        team1_url = f"{self.base_url}{team1_link.get('href')}" if team1_link else ""
        team2_url = f"{self.base_url}{team2_link.get('href')}" if team2_link else ""
        
        print(f"Team URLs: {team1_url}, {team2_url}")
        return team1_url, team2_url
    
    def scrape_team_players(self, team_url: str, team_name: str) -> List[Dict[str, Any]]:
        """Scrape player information from team page"""
        print(f"Scraping players for {team_name}...")
        
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
        
        print(f"Found {len(players)} players for {team_name}")
        return players
    
    def scrape_player_stats(self, stats_url: str, player_name: str) -> Dict[str, Optional[float]]:
        """Scrape individual player statistics"""
        print(f"Scraping stats for {player_name}...")
        
        try:
            soup = self.get_page_content(stats_url)
            
            stats = {}
            
            # Debug: Print page content to understand structure
            print(f"Debug: Looking for stats on {stats_url}")
            
            # Approach 1: Look for stats in the specific class you mentioned
            stat_boxes = soup.find_all('div', class_='player-summary-stat-box-data traditionalData')
            print(f"Debug: Found {len(stat_boxes)} stat boxes with class 'player-summary-stat-box-data traditionalData'")
            
            # The stat boxes appear to be in a specific order based on the debug output
            # Let's map them by position and content patterns
            if len(stat_boxes) >= 5:
                # Correct order based on your clarification:
                # Box 0: DPR (Damage per Round)
                # Box 1: KAST (percentage like 74.0%)
                # Box 2: Multi-kill (SKIP THIS)
                # Box 3: ADR (Average Damage per Round)
                # Box 4: KPR (Kills per Round)
                # RATING is in separate class: player-summary-stat-box-rating-data-text
                
                for i, box in enumerate(stat_boxes):
                    box_text = box.get_text().strip()
                    print(f"Debug: Stat box {i} content: {box_text}")
                    
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
                print(f"Debug: Rating element content: {rating_text}")
                if rating_text != '-' and rating_text:
                    stats["RATING"] = self.safe_float(rating_text)
            
            # Fallback: Look for specific stats in any box
            for box in stat_boxes:
                box_text = box.get_text().strip()
                
                # Look for specific patterns
                if 'Rating 2.0' in box_text:
                    import re
                    rating_match = re.search(r'Rating 2\.0\s*([\d.]+)', box_text)
                    if rating_match:
                        stats["RATING"] = self.safe_float(rating_match.group(1))
                
                if 'Average damage' in box_text or 'ADR' in box_text:
                    adr_match = re.search(r'(?:Average damage|ADR)[\s/]*:?\s*([\d.]+)', box_text)
                    if adr_match:
                        stats["ADR"] = self.safe_float(adr_match.group(1))
                
                if 'K/D' in box_text or 'KDR' in box_text:
                    kd_match = re.search(r'K/D[\s:]*([\d.]+)', box_text)
                    if kd_match:
                        stats["KDR"] = self.safe_float(kd_match.group(1))
                
                if 'KAST' in box_text:
                    kast_match = re.search(r'KAST[\s:]*([\d.]+)', box_text)
                    if kast_match:
                        stats["KAST"] = self.safe_float(kast_match.group(1))
            
            # If we didn't find stats in the specific boxes, try looking for individual stat elements
            if not stats or not any(stats.values()):
                print("Debug: Looking for individual stat elements...")
                
                # Look for elements with specific text patterns
                all_elements = soup.find_all(['div', 'span', 'td', 'th'])
                for element in all_elements:
                    text = element.get_text().strip()
                    
                    # Look for Rating 2.0
                    if 'Rating 2.0' in text and not stats.get("RATING"):
                        # Try to find the value in the same element or nearby
                        import re
                        rating_match = re.search(r'Rating 2\.0\s*([\d.]+)', text)
                        if rating_match:
                            stats["RATING"] = self.safe_float(rating_match.group(1))
                        else:
                            # Look in parent or next sibling
                            parent = element.parent
                            if parent:
                                parent_text = parent.get_text()
                                rating_match = re.search(r'Rating 2\.0\s*([\d.]+)', parent_text)
                                if rating_match:
                                    stats["RATING"] = self.safe_float(rating_match.group(1))
                    
                    # Look for ADR
                    if ('Average damage' in text or 'ADR' in text) and not stats.get("ADR"):
                        import re
                        adr_match = re.search(r'(?:Average damage|ADR)[\s/]*:?\s*([\d.]+)', text)
                        if adr_match:
                            stats["ADR"] = self.safe_float(adr_match.group(1))
                        else:
                            parent = element.parent
                            if parent:
                                parent_text = parent.get_text()
                                adr_match = re.search(r'(?:Average damage|ADR)[\s/]*:?\s*([\d.]+)', parent_text)
                                if adr_match:
                                    stats["ADR"] = self.safe_float(adr_match.group(1))
                    
                    # Look for K/D
                    if ('K/D' in text or 'KDR' in text) and not stats.get("KDR"):
                        import re
                        kd_match = re.search(r'K/D[\s:]*([\d.]+)', text)
                        if kd_match:
                            stats["KDR"] = self.safe_float(kd_match.group(1))
                        else:
                            parent = element.parent
                            if parent:
                                parent_text = parent.get_text()
                                kd_match = re.search(r'K/D[\s:]*([\d.]+)', parent_text)
                                if kd_match:
                                    stats["KDR"] = self.safe_float(kd_match.group(1))
                    
                    # Look for KAST
                    if 'KAST' in text and not stats.get("KAST"):
                        import re
                        kast_match = re.search(r'KAST[\s:]*([\d.]+)', text)
                        if kast_match:
                            stats["KAST"] = self.safe_float(kast_match.group(1))
                        else:
                            parent = element.parent
                            if parent:
                                parent_text = parent.get_text()
                                kast_match = re.search(r'KAST[\s:]*([\d.]+)', parent_text)
                                if kast_match:
                                    stats["KAST"] = self.safe_float(kast_match.group(1))
            
            # Try multiple approaches to find stats
            # Approach 2: Look for stats in table format
            tables = soup.find_all('table')
            for table in tables:
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    if len(cells) >= 2:
                        label = cells[0].get_text().strip()
                        value = cells[1].get_text().strip()
                        
                        if 'Rating 2.0' in label or 'Rating' in label:
                            stats["RATING"] = self.safe_float(value)
                        elif 'Average damage' in label or 'ADR' in label:
                            stats["ADR"] = self.safe_float(value)
                        elif 'K/D' in label or 'KDR' in label:
                            stats["KDR"] = self.safe_float(value)
                        elif 'KAST' in label:
                            stats["KAST"] = self.safe_float(value)
            
            # Approach 2: Look for stats in div format - parent contains both label and value
            if not stats:
                all_divs = soup.find_all('div')
                for div in all_divs:
                    text = div.get_text().strip()
                    
                    # Look for specific stat labels in the div itself
                    if 'Rating 2.0' in text:
                        # The value should be in the same div or parent
                        parent = div.parent
                        if parent:
                            parent_text = parent.get_text().strip()
                            # Extract number after "Rating 2.0"
                            import re
                            match = re.search(r'Rating 2\.0\s*([\d.]+)', parent_text)
                            if match:
                                stats["RATING"] = self.safe_float(match.group(1))
                    
                    elif 'Average damage' in text or 'ADR' in text:
                        parent = div.parent
                        if parent:
                            parent_text = parent.get_text().strip()
                            # Look for number after the label
                            import re
                            match = re.search(r'(?:Average damage|ADR)[\s/]*:?\s*([\d.]+)', parent_text)
                            if match:
                                stats["ADR"] = self.safe_float(match.group(1))
                    
                    elif 'K/D' in text or 'KDR' in text:
                        parent = div.parent
                        if parent:
                            parent_text = parent.get_text().strip()
                            # Look for number after K/D
                            import re
                            match = re.search(r'K/D[\s:]*([\d.]+)', parent_text)
                            if match:
                                stats["KDR"] = self.safe_float(match.group(1))
                    
                    elif 'KAST' in text:
                        parent = div.parent
                        if parent:
                            parent_text = parent.get_text().strip()
                            # Look for number after KAST
                            import re
                            match = re.search(r'KAST[\s:]*([\d.]+)', parent_text)
                            if match:
                                stats["KAST"] = self.safe_float(match.group(1))
            
            # Approach 3: Look for specific patterns in the page content
            if not stats or not any(stats.values()):
                page_text = soup.get_text()
                
                # Look for Rating 2.0 pattern
                import re
                rating_match = re.search(r'Rating 2\.0\s*([\d.]+)', page_text)
                if rating_match:
                    stats["RATING"] = self.safe_float(rating_match.group(1))
                
                # Look for ADR pattern
                adr_match = re.search(r'(?:Average damage|ADR)[\s/]*:?\s*([\d.]+)', page_text)
                if adr_match:
                    stats["ADR"] = self.safe_float(adr_match.group(1))
                
                # Look for K/D pattern
                kd_match = re.search(r'K/D[\s:]*([\d.]+)', page_text)
                if kd_match:
                    stats["KDR"] = self.safe_float(kd_match.group(1))
                
                # Look for KAST pattern
                kast_match = re.search(r'KAST[\s:]*([\d.]+)', page_text)
                if kast_match:
                    stats["KAST"] = self.safe_float(kast_match.group(1))
            
            # Approach 4: Look for specific class patterns
            if not stats or not any(stats.values()):
                # Try to find elements with specific classes
                rating_elem = soup.find('div', class_=lambda x: x and 'rating' in x.lower())
                if rating_elem:
                    stats["RATING"] = self.safe_float(rating_elem.get_text())
                
                adr_elem = soup.find('div', class_=lambda x: x and 'adr' in x.lower())
                if adr_elem:
                    stats["ADR"] = self.safe_float(adr_elem.get_text())
                
                kd_elem = soup.find('div', class_=lambda x: x and 'kd' in x.lower())
                if kd_elem:
                    stats["KDR"] = self.safe_float(kd_elem.get_text())
                
                kast_elem = soup.find('div', class_=lambda x: x and 'kast' in x.lower())
                if kast_elem:
                    stats["KAST"] = self.safe_float(kast_elem.get_text())
            
            # Ensure we have all required stats in the correct order
            if "DPR" not in stats:
                stats["DPR"] = None
            if "KAST" not in stats:
                stats["KAST"] = None
            if "ADR" not in stats:
                stats["ADR"] = None
            if "KPR" not in stats:
                stats["KPR"] = None
            if "RATING" not in stats:
                stats["RATING"] = None
            
            print(f"Stats for {player_name}: {stats}")
            return stats
            
        except Exception as e:
            print(f"Error scraping stats for {player_name}: {e}")
            return {"DPR": None, "KAST": None, "ADR": None, "KPR": None, "RATING": None}
    
    def scrape_all_matches_data(self) -> List[Dict[str, Any]]:
        """Main method to scrape complete data for all matches in the event"""
        try:
            # Step 1: Scrape basic match info for all matches
            all_matches_info = self.scrape_all_matches_basic_info()
            
            all_matches_data = []
            
            # Step 2: Process each match
            for i, match_info in enumerate(all_matches_info):
                print(f"\n=== Processing Match {i+1}/{len(all_matches_info)}: {match_info['team1_name']} vs {match_info['team2_name']} ===")
                
                try:
                    # Get team URLs
                    team1_url, team2_url = self.scrape_team_urls(match_info["match_url"])
                    
                    # Scrape team 1 players
                    team1_players = self.scrape_team_players(team1_url, match_info["team1_name"])
                    
                    # Scrape team 2 players
                    team2_players = self.scrape_team_players(team2_url, match_info["team2_name"])
                    
                    # Scrape individual player stats
                    print(f"  Scraping stats for {len(team1_players)} players from {match_info['team1_name']}...")
                    for player in team1_players:
                        player["statistics"] = self.scrape_player_stats(player["stats_url"], player["name"])
                        time.sleep(1)  # Be respectful to the server
                    
                    print(f"  Scraping stats for {len(team2_players)} players from {match_info['team2_name']}...")
                    for player in team2_players:
                        player["statistics"] = self.scrape_player_stats(player["stats_url"], player["name"])
                        time.sleep(1)  # Be respectful to the server
                    
                    # Structure the match data
                    match_data = {
                        "match_id": f"blast_london_2025_{match_info['match_number']}",
                        "date": "",
                        "tournament": "BLAST Premier: London 2025",
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
                            "match_type": "bo3",
                            "event_type": "lan",
                            "scraped_date": datetime.now().isoformat() + "Z",
                            "hltv_url": match_info["match_url"],
                            "match_number": match_info["match_number"]
                        }
                    }
                    
                    all_matches_data.append(match_data)
                    print(f"  ✅ Successfully completed match {i+1}")
                    
                except Exception as e:
                    print(f"  ❌ Error processing match {i+1}: {e}")
                    # Continue with next match instead of failing completely
                    continue
            
            self.match_data = {
                "event": "BLAST Premier: London 2025",
                "total_matches": len(all_matches_data),
                "scraped_date": datetime.now().isoformat() + "Z",
                "matches": all_matches_data
            }
            
            return self.match_data
            
        except Exception as e:
            print(f"Error during scraping: {e}")
            raise
    
    def save_to_file(self, filename: str = "data/blast/BlastLondon_data.json"):
        """Save scraped data to JSON file"""
        if not self.match_data:
            print("No data to save. Run scrape_all_matches_data() first.")
            return
        
        # Ensure directory exists
        import os
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(self.match_data, f, indent=2, ensure_ascii=False)
        
        print(f"Data saved to {filename}")
        print(f"Total matches scraped: {self.match_data.get('total_matches', 0)}")


def main():
    """Main function to run the scraper"""
    scraper = HLTVCFScraper()
    
    try:
        print("Starting HLTV BLAST London 2025 event scraping with cloudscraper...")
        print("This will scrape ALL matches in the event...")
        match_data = scraper.scrape_all_matches_data()
        scraper.save_to_file()
        print("Scraping completed successfully!")
        
    except Exception as e:
        print(f"Scraping failed: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())
