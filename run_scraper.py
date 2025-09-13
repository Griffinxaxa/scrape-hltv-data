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

def run_aggregator(input_file: str, output_file: str = None):
    """Run the team statistics aggregator"""
    print("📊 Starting team statistics aggregator...")
    from aggregate_team_stats import TeamStatsAggregator
    
    if not output_file:
        output_file = input_file.replace('.json', '_team_averages.json')
    
    aggregator = TeamStatsAggregator(input_file, output_file)
    aggregator.run()
    return 0

def run_csv_converter(input_file: str, output_file: str = None):
    """Run the JSON to CSV converter"""
    print("📊 Starting JSON to CSV converter...")
    from json_to_csv_converter import JSONToCSVConverter
    
    if not output_file:
        output_file = input_file.replace('.json', '.csv')
    
    converter = JSONToCSVConverter(input_file, output_file)
    converter.run()
    return 0

def run_update_scraper(target_match_id: int, output_dir: str = "data/updates"):
    """Run the update scraper"""
    print(f"🔄 Starting update scraper for matches newer than ID {target_match_id}...")
    from hltv_update_scraper import HLTVUpdateScraper
    
    scraper = HLTVUpdateScraper(target_match_id, output_dir)
    scraper.run()
    return 0

def main():
    parser = argparse.ArgumentParser(description="HLTV Scraper Runner")
    parser.add_argument("scraper", choices=["blast", "largescale", "test", "results", "aggregate", "csv", "update"], 
                       help="Which scraper to run")
    parser.add_argument("--matches", "-m", type=int, default=500,
                       help="Number of matches to scrape (for largescale and test)")
    parser.add_argument("--input", "-i", type=str,
                       help="Input file for aggregation")
    parser.add_argument("--output", "-o", type=str,
                       help="Output file for aggregation")
    parser.add_argument("--target_match_id", "-t", type=int,
                       help="Target match ID for update scraper")
    parser.add_argument("--output_dir", type=str, default="data/updates",
                       help="Output directory for update scraper")
    
    args = parser.parse_args()
    
    # Ensure data directories exist
    os.makedirs("data/blast", exist_ok=True)
    os.makedirs("data/largescale", exist_ok=True)
    os.makedirs("data/test", exist_ok=True)
    os.makedirs("data/results", exist_ok=True)
    os.makedirs("data/updates", exist_ok=True)
    
    if args.scraper == "blast":
        return run_blast_scraper()
    elif args.scraper == "largescale":
        return run_largescale_scraper(args.matches)
    elif args.scraper == "test":
        return run_test_scraper(args.matches)
    elif args.scraper == "results":
        return run_results_scraper()
    elif args.scraper == "aggregate":
        if not args.input:
            print("Error: --input file required for aggregation")
            return 1
        return run_aggregator(args.input, args.output)
    elif args.scraper == "csv":
        if not args.input:
            print("Error: --input file required for CSV conversion")
            return 1
        return run_csv_converter(args.input, args.output)
    elif args.scraper == "update":
        if not args.target_match_id:
            print("Error: --target_match_id required for update scraper")
            return 1
        return run_update_scraper(args.target_match_id, args.output_dir)

if __name__ == "__main__":
    exit(main())
