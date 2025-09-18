#!/usr/bin/env python3
"""
HLTV Large-Scale Scraper

This script is designed for scraping 14,000 matches divided into 8 seasons of 1,750 matches each.
Features pause/resume functionality for long-running scraping sessions.

Usage:
    python hltv_largescale_scraper.py --start_match_id 2385000 --total_matches 14000
    python hltv_largescale_scraper.py --pause  # To pause gracefully
    python hltv_largescale_scraper.py --resume  # To resume from last position
"""

import json
import sys
import argparse
import os
from datetime import datetime
from hltv_enhanced_scraper import HLTVEnhancedScraper

class HLTVLargeScaleScraper:
    def __init__(self, output_dir: str = "data/largescale"):
        self.output_dir = output_dir
        self.matches_per_season = 1750
        self.total_seasons = 8
        self.total_matches = self.matches_per_season * self.total_seasons  # 14,000
        self.batch_size = 100  # Process in batches for better memory management
        
        # Progress tracking files
        self.progress_file = os.path.join(output_dir, "largescale_progress.json")
        self.pause_file = os.path.join(output_dir, "largescale_pause.flag")
        
        os.makedirs(output_dir, exist_ok=True)
    
    def load_progress(self):
        """Load large-scale scraping progress"""
        try:
            if os.path.exists(self.progress_file):
                with open(self.progress_file, 'r') as f:
                    return json.load(f)
            return {
                'matches_completed': 0,
                'current_season': 1,
                'last_match_id': None,
                'start_time': datetime.now().isoformat(),
                'batches_completed': 0
            }
        except Exception as e:
            print(f"⚠️ Error loading progress: {e}")
            return {
                'matches_completed': 0,
                'current_season': 1,
                'last_match_id': None,
                'start_time': datetime.now().isoformat(),
                'batches_completed': 0
            }
    
    def save_progress(self, progress_data):
        """Save large-scale scraping progress"""
        try:
            progress_data['last_updated'] = datetime.now().isoformat()
            with open(self.progress_file, 'w') as f:
                json.dump(progress_data, f, indent=2)
        except Exception as e:
            print(f"⚠️ Error saving progress: {e}")
    
    def check_pause_signal(self):
        """Check if pause signal exists"""
        return os.path.exists(self.pause_file)
    
    def create_pause_file(self):
        """Create pause signal file"""
        try:
            with open(self.pause_file, 'w') as f:
                f.write(f"Large-scale pause requested at {datetime.now().isoformat()}")
            print(f"✅ Large-scale pause file created: {self.pause_file}")
            print(f"🔄 The scraper will pause gracefully after completing the current batch")
        except Exception as e:
            print(f"❌ Error creating pause file: {e}")
    
    def remove_pause_file(self):
        """Remove pause signal file"""
        try:
            if os.path.exists(self.pause_file):
                os.remove(self.pause_file)
                print(f"🗑️ Pause file removed")
        except Exception as e:
            print(f"⚠️ Error removing pause file: {e}")
    
    def calculate_season(self, match_number):
        """Calculate which season a match belongs to"""
        if match_number <= 0:
            return 1
        return min(8, ((match_number - 1) // self.matches_per_season) + 1)
    
    def run_largescale_scraping(self, start_match_id: int, resume: bool = False):
        """Run large-scale scraping with pause/resume functionality"""
        
        # Load or initialize progress
        progress = self.load_progress()
        
        if resume:
            print(f"🔄 Resuming large-scale scraping...")
            matches_completed = progress['matches_completed']
            current_match_id = progress.get('last_match_id', start_match_id)
            batches_completed = progress['batches_completed']
        else:
            print(f"🚀 Starting large-scale scraping of {self.total_matches} matches...")
            matches_completed = 0
            current_match_id = start_match_id
            batches_completed = 0
            # Remove any existing pause file
            self.remove_pause_file()
        
        print(f"📊 Target: {self.total_matches} matches across {self.total_seasons} seasons")
        print(f"📦 Batch size: {self.batch_size} matches per batch")
        print(f"🎯 Starting from match ID: {current_match_id}")
        print(f"📈 Progress: {matches_completed}/{self.total_matches} matches completed")
        
        try:
            while matches_completed < self.total_matches:
                # Check for pause signal
                if self.check_pause_signal():
                    print(f"\n⏸️ Pause signal detected!")
                    print(f"💾 Saving progress at {matches_completed} matches...")
                    progress['matches_completed'] = matches_completed
                    progress['last_match_id'] = current_match_id
                    progress['batches_completed'] = batches_completed
                    self.save_progress(progress)
                    print(f"✅ Progress saved. To resume, run with --resume flag")
                    return
                
                # Calculate remaining matches for this batch
                remaining_matches = self.total_matches - matches_completed
                batch_matches = min(self.batch_size, remaining_matches)
                
                current_season = self.calculate_season(matches_completed + 1)
                
                print(f"\n🔄 Batch {batches_completed + 1} | Season {current_season}")
                print(f"📦 Processing {batch_matches} matches starting from ID {current_match_id}")
                
                # Run the enhanced scraper for this batch
                scraper = HLTVEnhancedScraper(
                    target_match_id=current_match_id,
                    num_matches=batch_matches,
                    output_dir=self.output_dir
                )
                
                # Set the match counter to continue from where we left off
                scraper.match_counter = matches_completed
                
                batch_matches_data = scraper.scrape_enhanced_matches()
                
                if not batch_matches_data:
                    print(f"⚠️ No matches found in batch, stopping...")
                    break
                    
                # Update progress
                matches_completed += len(batch_matches_data)
                batches_completed += 1
                
                # Update current_match_id for next batch (get the lowest ID from this batch)
                if batch_matches_data:
                    batch_ids = [int(match.get('hltv_match_id', 0)) for match in batch_matches_data]
                    current_match_id = min(batch_ids) - 1  # Start next batch from before the lowest ID
                
                # Save progress after each batch
                progress['matches_completed'] = matches_completed
                progress['last_match_id'] = current_match_id
                progress['batches_completed'] = batches_completed
                progress['current_season'] = current_season
                self.save_progress(progress)
                
                print(f"✅ Batch {batches_completed} completed!")
                print(f"📊 Progress: {matches_completed}/{self.total_matches} matches ({(matches_completed/self.total_matches)*100:.1f}%)")
                print(f"🏆 Season: {current_season}/8")
                
                # Brief pause between batches
                import time
                time.sleep(5)
        
        except KeyboardInterrupt:
            print(f"\n⚠️ Scraping interrupted by user!")
            progress['matches_completed'] = matches_completed
            progress['last_match_id'] = current_match_id
            progress['batches_completed'] = batches_completed
            self.save_progress(progress)
            print(f"💾 Progress saved at {matches_completed} matches")
            
        except Exception as e:
            print(f"❌ Error during large-scale scraping: {e}")
            progress['matches_completed'] = matches_completed
            progress['last_match_id'] = current_match_id
            progress['batches_completed'] = batches_completed
            self.save_progress(progress)
        
        print(f"\n🎉 Large-scale scraping completed!")
        print(f"📊 Final stats: {matches_completed}/{self.total_matches} matches")
        print(f"🏆 Seasons completed: {self.calculate_season(matches_completed)}/8")

def main():
    """Main function for large-scale scraping"""
    parser = argparse.ArgumentParser(description="HLTV Large-Scale Scraper - 14,000 matches across 8 seasons")
    parser.add_argument('--start_match_id', type=int, help='Starting match ID for scraping')
    parser.add_argument('--total_matches', type=int, default=14000, help='Total matches to scrape')
    parser.add_argument('--output_dir', type=str, default='data/largescale', help='Output directory')
    parser.add_argument('--pause', action='store_true', help='Create pause signal file')
    parser.add_argument('--resume', action='store_true', help='Resume from last saved progress')
    
    args = parser.parse_args()
    
    scraper = HLTVLargeScaleScraper(args.output_dir)
    
    if args.pause:
        scraper.create_pause_file()
    elif args.resume:
        scraper.run_largescale_scraping(start_match_id=0, resume=True)
    else:
        if not args.start_match_id:
            print("❌ --start_match_id is required for new scraping sessions")
            sys.exit(1)
        scraper.run_largescale_scraping(args.start_match_id, resume=False)

if __name__ == "__main__":
    main()