#!/usr/bin/env python3
"""
Find Latest Match ID Script

This script finds the latest match ID from the HLTV results page.
Use this to get the current match ID for daily updates.

Usage:
    python find_latest_match_id.py
"""

import cloudscraper
from bs4 import BeautifulSoup
import re

def find_latest_match_id():
    """Find the latest match ID from HLTV results page"""
    try:
        # Create cloudscraper session
        session = cloudscraper.create_scraper()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        })
        
        print("🔍 Finding latest match ID from HLTV results page...")
        
        # Get the first page of results
        response = session.get("https://www.hltv.org/results")
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        match_elements = soup.select('.result-con')
        
        if not match_elements:
            print("❌ No matches found on results page")
            return None
        
        # Get the first match (most recent)
        first_match = match_elements[0]
        match_link = first_match.select_one('a')
        
        if not match_link:
            print("❌ No match link found")
            return None
        
        match_url = "https://www.hltv.org" + match_link.get('href')
        
        # Extract match ID from URL
        match = re.search(r'/matches/(\d+)/', match_url)
        if match:
            match_id = int(match.group(1))
            print(f"✅ Latest match ID: {match_id}")
            print(f"🔗 Match URL: {match_url}")
            print(f"\n💡 Use this ID in your daily_update.py script:")
            print(f"   TARGET_MATCH_ID = {match_id}")
            return match_id
        else:
            print("❌ Could not extract match ID from URL")
            return None
            
    except Exception as e:
        print(f"❌ Error finding latest match ID: {e}")
        return None

if __name__ == "__main__":
    find_latest_match_id()
