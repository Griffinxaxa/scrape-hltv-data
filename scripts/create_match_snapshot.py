#!/usr/bin/env python3
"""
HLTV Match Snapshot Creator

This script quickly scrapes match IDs from HLTV results pages to create
a snapshot of matches in chronological order. This snapshot can then be
used for consistent scraping that won't be affected by new matches being added.

Usage:
    python create_match_snapshot.py --num_ids 15000 --output data/match_snapshot.json
    python create_match_snapshot.py --num_ids 100 --output data/test_snapshot.json  # For testing
"""

import json
import argparse
import time
import os
from datetime import datetime
from typing import List, Dict, Any
import cloudscraper
from bs4 import BeautifulSoup

class MatchSnapshotCreator:
    def __init__(self, num_ids: int = 15000, output_file: str = "data/match_snapshot.json"):
        self.num_ids = num_ids
        self.output_file = output_file
        self.base_url = "https://www.hltv.org"
        self.results_url = f"{self.base_url}/results"
        
        # Respectful scraping delay
        self.page_delay = 2  # 2 seconds between pages
        
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
        
        # Ensure output directory exists
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    def get_page_content(self, url: str) -> BeautifulSoup:
        """Fetch and parse a page"""
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            return BeautifulSoup(response.text, 'html.parser')
        except Exception as e:
            print(f"⚠️ Error fetching {url}: {e}")
            return None
    
    def extract_match_id_from_element(self, match_element) -> Dict[str, Any]:
        """Extract match ID and basic info from a result-con element"""
        try:
            # Get match link
            match_link = match_element.select_one('a.a-reset')
            if not match_link or 'href' not in match_link.attrs:
                return None
            
            # Extract match ID from URL
            # URL format: /matches/2386572/vitality-vs-g2-blast-premier-world-final-2024
            href = match_link['href']
            parts = href.split('/')
            if len(parts) < 3:
                return None
            
            match_id = parts[2]
            
            # Try to get match ID as integer to validate
            try:
                match_id_int = int(match_id)
            except ValueError:
                return None
            
            # Get team names for verification
            team_elements = match_element.select('.team')
            team1_name = team_elements[0].get_text(strip=True) if len(team_elements) > 0 else "Unknown"
            team2_name = team_elements[1].get_text(strip=True) if len(team_elements) > 1 else "Unknown"
            
            # Get score for forfeit detection
            result_score = match_element.select_one('.result-score')
            score = result_score.get_text(strip=True) if result_score else "0-0"
            
            return {
                'match_id': match_id_int,
                'team1': team1_name,
                'team2': team2_name,
                'score': score,
                'url': f"{self.base_url}{href}"
            }
            
        except Exception as e:
            print(f"⚠️ Error extracting match ID: {e}")
            return None
    
    def create_snapshot(self) -> List[Dict[str, Any]]:
        """Create a snapshot of match IDs from results pages"""
        all_match_data = []
        page_offset = 0
        matches_per_page = 100  # HLTV shows ~100 matches per page
        
        print(f"🔍 Creating snapshot of {self.num_ids} match IDs...")
        print(f"📄 Estimated pages to scrape: ~{self.num_ids // matches_per_page}")
        print(f"⏱️  Estimated time: ~{(self.num_ids // matches_per_page * self.page_delay / 60):.1f} minutes")
        print("")
        
        start_time = time.time()
        pages_scraped = 0
        
        try:
            while len(all_match_data) < self.num_ids:
                current_page_url = f"{self.results_url}?offset={page_offset}"
                
                print(f"📄 Page {pages_scraped + 1} (offset={page_offset}) - {len(all_match_data)} IDs collected...", end='\r')
                
                soup = self.get_page_content(current_page_url)
                if not soup:
                    print(f"\n⚠️ Failed to fetch page at offset {page_offset}")
                    break
                
                all_matches_on_page = soup.select('.result-con')
                if not all_matches_on_page:
                    print(f"\n⚠️ No matches found on page at offset {page_offset}")
                    break
                
                # Extract match IDs from this page
                for match_element in all_matches_on_page:
                    if len(all_match_data) >= self.num_ids:
                        break
                    
                    match_data = self.extract_match_id_from_element(match_element)
                    if match_data:
                        all_match_data.append(match_data)
                
                pages_scraped += 1
                page_offset += matches_per_page
                
                # Respectful delay between pages
                if len(all_match_data) < self.num_ids:
                    time.sleep(self.page_delay)
            
            elapsed_time = time.time() - start_time
            print(f"\n\n✅ Snapshot creation complete!")
            print(f"📊 Total match IDs collected: {len(all_match_data)}")
            print(f"📄 Pages scraped: {pages_scraped}")
            print(f"⏱️  Time elapsed: {elapsed_time / 60:.1f} minutes")
            
            return all_match_data
            
        except KeyboardInterrupt:
            print(f"\n\n⚠️ Snapshot creation interrupted!")
            print(f"📊 Match IDs collected so far: {len(all_match_data)}")
            return all_match_data
        
        except Exception as e:
            print(f"\n❌ Error during snapshot creation: {e}")
            return all_match_data
    
    def save_snapshot(self, match_data: List[Dict[str, Any]]):
        """Save snapshot to JSON file"""
        try:
            snapshot = {
                "metadata": {
                    "created_at": datetime.now().isoformat(),
                    "total_matches": len(match_data),
                    "num_ids_requested": self.num_ids,
                    "first_match_id": match_data[0]['match_id'] if match_data else None,
                    "last_match_id": match_data[-1]['match_id'] if match_data else None,
                },
                "matches": match_data
            }
            
            with open(self.output_file, 'w', encoding='utf-8') as f:
                json.dump(snapshot, f, indent=2, ensure_ascii=False)
            
            print(f"\n💾 Snapshot saved to: {self.output_file}")
            print(f"📁 File size: {os.path.getsize(self.output_file) / 1024:.1f} KB")
            
        except Exception as e:
            print(f"❌ Error saving snapshot: {e}")
    
    def run(self):
        """Main execution method"""
        print("=" * 60)
        print("🎯 HLTV Match Snapshot Creator")
        print("=" * 60)
        print(f"Target IDs: {self.num_ids}")
        print(f"Output file: {self.output_file}")
        print("=" * 60)
        print("")
        
        match_data = self.create_snapshot()
        
        if match_data:
            self.save_snapshot(match_data)
            print("\n✅ Snapshot creation successful!")
            print(f"🔢 First match: {match_data[0]['team1']} vs {match_data[0]['team2']} (ID: {match_data[0]['match_id']})")
            print(f"🔢 Last match: {match_data[-1]['team1']} vs {match_data[-1]['team2']} (ID: {match_data[-1]['match_id']})")
        else:
            print("\n❌ Snapshot creation failed - no data collected")

def main():
    """Main function with command line argument handling"""
    parser = argparse.ArgumentParser(description="HLTV Match Snapshot Creator - Quickly scrape match IDs")
    parser.add_argument('--num_ids', '-n', type=int, default=15000,
                       help='Number of match IDs to collect (default: 15000)')
    parser.add_argument('--output', '-o', type=str, default='data/match_snapshot.json',
                       help='Output file path (default: data/match_snapshot.json)')
    
    args = parser.parse_args()
    
    creator = MatchSnapshotCreator(args.num_ids, args.output)
    creator.run()

if __name__ == "__main__":
    main()







