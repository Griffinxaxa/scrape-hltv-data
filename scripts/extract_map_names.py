#!/usr/bin/env python3
"""
Extract map names from match pages

This script reads URLs from the combined round-by-round CSV and extracts
map1_name, map2_name, map3_name for each match.

Usage:
    python scripts/extract_map_names.py --input data/round_by_round/combined_round_by_round_all.csv --output data/round_by_round/map_names.csv
"""

import pandas as pd
import argparse
import time
from typing import Dict, List, Optional
import cloudscraper
from bs4 import BeautifulSoup
import os

class MapNameExtractor:
    def __init__(self, input_file: str, output_file: str, limit: Optional[int] = None):
        self.input_file = input_file
        self.output_file = output_file
        self.limit = limit
        self.base_url = "https://www.hltv.org"
        
        # Delays for respectful scraping
        self.page_delay = 0.3
        
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
    
    def get_page_content(self, url: str, max_retries: int = 3) -> Optional[BeautifulSoup]:
        """Get page content using cloudscraper with retry logic"""
        for attempt in range(max_retries):
            try:
                response = self.session.get(url, timeout=30)
                if response.status_code == 429:
                    wait_time = (attempt + 1) * 10
                    print(f"  ⚠️ Rate limited, waiting {wait_time} seconds...")
                    time.sleep(wait_time)
                    continue
                response.raise_for_status()
                return BeautifulSoup(response.content, 'html.parser')
            except Exception as e:
                if attempt == max_retries - 1:
                    return None
                time.sleep((attempt + 1) * 5)
        return None
    
    def extract_map_names(self, match_url: str) -> Dict[str, Optional[str]]:
        """Extract map names from a match page"""
        try:
            soup = self.get_page_content(match_url)
            if not soup:
                return {
                    'map1_name': None,
                    'map2_name': None,
                    'map3_name': None
                }
            
            # Find all mapholder elements
            mapholders = soup.select('.mapholder')
            map_names = []
            
            for holder in mapholders:
                # Get map name from .map element or first significant line
                map_elem = holder.select_one('.map')
                if map_elem:
                    map_name = map_elem.get_text().strip()
                    if map_name:
                        map_names.append(map_name)
                        continue
                
                # Fallback: get first significant line
                text = holder.get_text()
                lines = [line.strip() for line in text.split('\n') if line.strip()]
                if lines:
                    # Common map names
                    maps = ['Overpass', 'Inferno', 'Train', 'Dust2', 'Mirage', 'Nuke', 'Ancient', 'Vertigo', 'Anubis', 'Cache', 'Cobblestone']
                    for line in lines:
                        for m in maps:
                            if m.lower() in line.lower():
                                map_names.append(m)
                                break
                        if map_names and len(map_names) > len(map_names) - 1:
                            break
            
            # Return map names (up to 3 maps)
            result = {
                'map1_name': map_names[0] if len(map_names) > 0 else None,
                'map2_name': map_names[1] if len(map_names) > 1 else None,
                'map3_name': map_names[2] if len(map_names) > 2 else None
            }
            
            return result
            
        except Exception as e:
            print(f"  ⚠️ Error extracting map names: {e}")
            return {
                'map1_name': None,
                'map2_name': None,
                'map3_name': None
            }
    
    def process_all_matches(self):
        """Process all matches from the input CSV"""
        print(f"📖 Reading input file: {self.input_file}")
        df = pd.read_csv(self.input_file)
        print(f"   Found {len(df)} matches")
        
        # Check if match_url column exists
        if 'match_url' not in df.columns:
            print("❌ Error: 'match_url' column not found in input file")
            return
        
        # Create output dataframe
        output_data = []
        
        total = len(df) if self.limit is None else min(self.limit, len(df))
        df_subset = df.head(total) if self.limit else df
        
        for idx, row in df_subset.iterrows():
            match_url = row['match_url']
            print(f"\n[{idx + 1}/{total}] Processing: {match_url}")
            
            # Extract map names
            map_names = self.extract_map_names(match_url)
            
            # Add match_url for joining later
            result = {
                'match_url': match_url,
                **map_names
            }
            output_data.append(result)
            
            print(f"  ✅ Maps: {map_names['map1_name']}, {map_names['map2_name']}, {map_names['map3_name']}")
            
            # Delay between requests
            time.sleep(self.page_delay)
            
            # Save checkpoint every 100 matches
            if (idx + 1) % 100 == 0:
                checkpoint_df = pd.DataFrame(output_data)
                checkpoint_file = self.output_file.replace('.csv', f'_checkpoint_{idx + 1}.csv')
                checkpoint_df.to_csv(checkpoint_file, index=False)
                print(f"  💾 Checkpoint saved: {checkpoint_file}")
        
        # Save final output
        output_df = pd.DataFrame(output_data)
        output_df.to_csv(self.output_file, index=False)
        print(f"\n✅ Saved map names to: {self.output_file}")
        print(f"   Total matches processed: {len(output_data)}")
        
        # Show summary
        print(f"\n📊 Summary:")
        print(f"   Map 1 names: {output_df['map1_name'].notna().sum()} matches")
        print(f"   Map 2 names: {output_df['map2_name'].notna().sum()} matches")
        print(f"   Map 3 names: {output_df['map3_name'].notna().sum()} matches")

def main():
    parser = argparse.ArgumentParser(description='Extract map names from match pages')
    parser.add_argument('--input', '-i', type=str, 
                       default='data/round_by_round/combined_round_by_round_all.csv',
                       help='Input CSV file with match URLs')
    parser.add_argument('--output', '-o', type=str,
                       default='data/round_by_round/map_names.csv',
                       help='Output CSV file for map names')
    parser.add_argument('--limit', '-l', type=int, default=None,
                       help='Limit number of matches to process (for testing)')
    
    args = parser.parse_args()
    
    # Create output directory if needed
    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    
    extractor = MapNameExtractor(args.input, args.output, limit=args.limit)
    extractor.process_all_matches()

if __name__ == '__main__':
    main()

