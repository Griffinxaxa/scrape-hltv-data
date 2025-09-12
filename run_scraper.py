#!/usr/bin/env python3
"""
HLTV Scraper Runner
Main script to run different HLTV scrapers
"""

import sys
import os
import argparse
from pathlib import Path

# Add scripts directory to path
sys.path.append(str(Path(__file__).parent / "scripts"))

def run_blast_scraper():
    """Run the BLAST London 2025 scraper"""
    print("🚀 Starting BLAST London 2025 scraper...")
    from hltv_BLASTscrape import main
    return main()

def run_largescale_scraper(matches: int = 500):
    """Run the large-scale scraper"""
    print(f"🚀 Starting large-scale scraper for {matches} matches...")
    from hltv_largescale_scraper import HLTVLargeScaleScraper
    
    scraper = HLTVLargeScaleScraper(target_matches=matches)
    try:
        match_data = scraper.scrape_large_scale_data()
        scraper.save_to_file()
        print("Large scale scraping completed successfully!")
        return 0
    except KeyboardInterrupt:
        print("\nScraping interrupted by user. Saving current progress...")
        scraper.save_to_file("data/largescale/hltv_largescale_data_partial.json")
        print("Partial data saved.")
        return 1
    except Exception as e:
        print(f"Scraping failed: {e}")
        return 1

def run_test_scraper(matches: int = 10):
    """Run the test scraper"""
    print(f"🧪 Starting test scraper for {matches} matches...")
    from test_largescale import HLTVTestLargeScaleScraper
    
    scraper = HLTVTestLargeScaleScraper(target_matches=matches)
    try:
        match_data = scraper.scrape_test_data()
        scraper.save_to_file()
        print("Test scraping completed successfully!")
        return 0
    except Exception as e:
        print(f"Test scraping failed: {e}")
        return 1

def run_results_scraper():
    """Run the results scraper (single match with forfeit detection)"""
    print("🎯 Starting results scraper...")
    from hltv_resultsscrape import main
    return main()

def main():
    parser = argparse.ArgumentParser(description="HLTV Scraper Runner")
    parser.add_argument("scraper", choices=["blast", "largescale", "test", "results"], 
                       help="Which scraper to run")
    parser.add_argument("--matches", "-m", type=int, default=500,
                       help="Number of matches to scrape (for largescale and test)")
    
    args = parser.parse_args()
    
    # Ensure data directories exist
    os.makedirs("data/blast", exist_ok=True)
    os.makedirs("data/largescale", exist_ok=True)
    os.makedirs("data/test", exist_ok=True)
    os.makedirs("data/results", exist_ok=True)
    
    if args.scraper == "blast":
        return run_blast_scraper()
    elif args.scraper == "largescale":
        return run_largescale_scraper(args.matches)
    elif args.scraper == "test":
        return run_test_scraper(args.matches)
    elif args.scraper == "results":
        return run_results_scraper()

if __name__ == "__main__":
    exit(main())
