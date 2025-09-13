#!/usr/bin/env python3
"""
Daily HLTV Update Script

Simple script to run daily updates. Just change the TARGET_MATCH_ID 
and run this script to get all new matches since that ID.

Usage:
    python daily_update.py
"""

import sys
import os
from datetime import datetime

# Add scripts directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'scripts'))

def main():
    """Main function for daily updates"""
    
    # ========================================
    # CONFIGURATION - UPDATE THIS AS NEEDED
    # ========================================
    
    # Change this to the match ID you want to scrape backwards from
    # This should be the most recent match ID from your previous scraping session
    # Run 'python scripts/find_latest_match_id.py' to get the current latest match ID
    TARGET_MATCH_ID = 2385589
    
    # Optional: Change output directory (default: data/updates)
    OUTPUT_DIR = "data/updates"
    
    # ========================================
    
    print("🔄 HLTV Daily Update")
    print("=" * 50)
    print(f"Target Match ID: {TARGET_MATCH_ID}")
    print(f"Output Directory: {OUTPUT_DIR}")
    print(f"Update Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    try:
        from hltv_update_scraper import HLTVUpdateScraper
        
        # Create scraper instance
        scraper = HLTVUpdateScraper(TARGET_MATCH_ID, OUTPUT_DIR)
        
        # Run the update
        scraper.run()
        
        print("\n✅ Daily update completed successfully!")
        print(f"📁 Check {OUTPUT_DIR}/ for new match files")
        print(f"📊 Check data/largescale/hltv_matches.csv for updated CSV")
        
    except KeyboardInterrupt:
        print("\n⚠️  Update interrupted by user")
        return 1
    except Exception as e:
        print(f"\n❌ Error during update: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
