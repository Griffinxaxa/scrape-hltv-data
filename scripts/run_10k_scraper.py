#!/usr/bin/env python3
"""
HLTV 10K Match Scraper Launcher

This script orchestrates the complete process of scraping 10,000 CS2 matches:
1. Create a snapshot of match IDs (if not exists)
2. Run the enhanced scraper with the snapshot

Usage:
    python run_10k_scraper.py                    # Fresh start
    python run_10k_scraper.py --resume           # Resume from progress
    python run_10k_scraper.py --snapshot-only    # Only create snapshot
    python run_10k_scraper.py --scrape-only      # Only run scraper (snapshot must exist)
"""

import os
import sys
import argparse
import json
import subprocess
from datetime import datetime

class Scraper10KLauncher:
    def __init__(self, 
                 snapshot_file: str = "data/match_snapshot.json",
                 output_dir: str = "data/enhanced",
                 num_snapshot_ids: int = 15000,
                 num_matches_target: int = 10000):
        self.snapshot_file = snapshot_file
        self.output_dir = output_dir
        self.num_snapshot_ids = num_snapshot_ids
        self.num_matches_target = num_matches_target
        self.progress_file = os.path.join(output_dir, "scraper_progress.json")
        
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
        os.makedirs("data", exist_ok=True)
    
    def snapshot_exists(self) -> bool:
        """Check if snapshot file exists"""
        return os.path.exists(self.snapshot_file)
    
    def check_snapshot_validity(self) -> bool:
        """Check if snapshot file is valid and has enough match IDs"""
        try:
            if not self.snapshot_exists():
                return False
            
            with open(self.snapshot_file, 'r') as f:
                snapshot = json.load(f)
                matches = snapshot.get('matches', [])
                if len(matches) >= self.num_snapshot_ids * 0.8:  # At least 80% of target
                    return True
                else:
                    print(f"⚠️ Snapshot has only {len(matches)} IDs (need ~{self.num_snapshot_ids})")
                    return False
        except Exception as e:
            print(f"⚠️ Error validating snapshot: {e}")
            return False
    
    def create_snapshot(self):
        """Create match snapshot using create_match_snapshot.py"""
        print("=" * 80)
        print("PHASE 1: CREATING MATCH SNAPSHOT")
        print("=" * 80)
        print(f"📸 Creating snapshot of {self.num_snapshot_ids} match IDs...")
        print(f"📁 Output: {self.snapshot_file}")
        print(f"⏱️  Estimated time: ~{(self.num_snapshot_ids // 100 * 2 / 60):.1f} minutes")
        print("")
        
        try:
            # Run create_match_snapshot.py
            cmd = [
                sys.executable,
                "scripts/create_match_snapshot.py",
                "--num_ids", str(self.num_snapshot_ids),
                "--output", self.snapshot_file
            ]
            
            result = subprocess.run(cmd, check=True)
            
            if result.returncode == 0:
                print("\n✅ Snapshot creation completed successfully!")
                return True
            else:
                print(f"\n❌ Snapshot creation failed with return code {result.returncode}")
                return False
                
        except subprocess.CalledProcessError as e:
            print(f"\n❌ Snapshot creation failed: {e}")
            return False
        except KeyboardInterrupt:
            print("\n⚠️ Snapshot creation interrupted by user")
            return False
    
    def run_scraper(self, resume: bool = False):
        """Run the enhanced scraper with snapshot"""
        print("\n")
        print("=" * 80)
        print("PHASE 2: SCRAPING MATCH DETAILS")
        print("=" * 80)
        print(f"🎯 Target: {self.num_matches_target} valid matches")
        print(f"📸 Snapshot: {self.snapshot_file}")
        print(f"📁 Output: {self.output_dir}")
        
        if resume:
            print("🔄 Resuming from saved progress...")
        else:
            print("🚀 Starting fresh scraping session...")
        
        print(f"⏱️  Estimated time: 30-50 hours")
        print("")
        
        try:
            # Run hltv_enhanced_scraper.py with snapshot mode
            cmd = [
                sys.executable,
                "scripts/hltv_enhanced_scraper.py",
                "--snapshot_file", self.snapshot_file,
                "--num_matches", str(self.num_matches_target),
                "--output_dir", self.output_dir
            ]
            
            result = subprocess.run(cmd, check=True)
            
            if result.returncode == 0:
                print("\n✅ Scraping completed successfully!")
                return True
            else:
                print(f"\n❌ Scraping failed with return code {result.returncode}")
                return False
                
        except subprocess.CalledProcessError as e:
            print(f"\n❌ Scraping failed: {e}")
            return False
        except KeyboardInterrupt:
            print("\n⚠️ Scraping interrupted by user")
            print("💾 Progress has been saved. Resume with: python run_10k_scraper.py --resume")
            return False
    
    def get_progress_status(self):
        """Get current scraping progress"""
        try:
            if os.path.exists(self.progress_file):
                with open(self.progress_file, 'r') as f:
                    progress = json.load(f)
                    return progress
            return None
        except Exception as e:
            print(f"⚠️ Error reading progress: {e}")
            return None
    
    def print_status(self):
        """Print current scraping status"""
        print("=" * 80)
        print("SCRAPING STATUS")
        print("=" * 80)
        
        # Check snapshot
        if self.snapshot_exists():
            try:
                with open(self.snapshot_file, 'r') as f:
                    snapshot = json.load(f)
                    metadata = snapshot.get('metadata', {})
                    matches = snapshot.get('matches', [])
                    print(f"✅ Snapshot exists: {len(matches)} match IDs")
                    print(f"   Created: {metadata.get('created_at', 'Unknown')}")
            except:
                print(f"⚠️ Snapshot file exists but cannot be read")
        else:
            print(f"❌ Snapshot does not exist")
        
        # Check progress
        progress = self.get_progress_status()
        if progress:
            match_counter = progress.get('match_counter', 0)
            snapshot_index = progress.get('snapshot_index', 0)
            timestamp = progress.get('timestamp', 'Unknown')
            print(f"\n📊 Scraping Progress:")
            print(f"   Matches processed: {match_counter}")
            print(f"   Snapshot index: {snapshot_index}")
            print(f"   Last updated: {timestamp}")
        else:
            print(f"\n📊 No scraping progress found (fresh start)")
        
        print("=" * 80)
    
    def run(self, resume: bool = False, snapshot_only: bool = False, scrape_only: bool = False):
        """Main execution flow"""
        print("\n")
        print("🎮 " + "=" * 74)
        print("🎮 HLTV 10K MATCH SCRAPER - SNAPSHOT-BASED APPROACH")
        print("🎮 " + "=" * 74)
        print(f"📅 Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("")
        
        # Phase 1: Snapshot creation
        if not scrape_only:
            if resume or self.check_snapshot_validity():
                print("✅ Valid snapshot already exists - skipping Phase 1")
                print(f"📸 Using: {self.snapshot_file}")
            else:
                print("📸 Snapshot not found or invalid - creating new snapshot...")
                success = self.create_snapshot()
                if not success:
                    print("\n❌ Failed to create snapshot. Exiting.")
                    return False
        
        if snapshot_only:
            print("\n✅ Snapshot-only mode complete!")
            return True
        
        # Phase 2: Scraping
        if not self.snapshot_exists():
            print("\n❌ Cannot run scraper - snapshot does not exist!")
            print("💡 Run without --scrape-only to create snapshot first")
            return False
        
        success = self.run_scraper(resume)
        
        if success:
            print("\n")
            print("🎉 " + "=" * 74)
            print("🎉 SCRAPING COMPLETE!")
            print("🎉 " + "=" * 74)
            print(f"📁 Output directory: {self.output_dir}")
            print(f"📄 Check for enhanced_matches_*.json and *.csv files")
        
        return success

def main():
    """Main function with command line argument handling"""
    parser = argparse.ArgumentParser(
        description="HLTV 10K Match Scraper - Orchestrates snapshot creation and scraping",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_10k_scraper.py                    # Fresh start (creates snapshot + scrapes)
  python run_10k_scraper.py --resume           # Resume from saved progress
  python run_10k_scraper.py --snapshot-only    # Only create snapshot
  python run_10k_scraper.py --scrape-only      # Only scrape (snapshot must exist)
  python run_10k_scraper.py --status           # Show current status
        """
    )
    
    parser.add_argument('--resume', '-r', action='store_true',
                       help='Resume scraping from saved progress')
    parser.add_argument('--snapshot-only', action='store_true',
                       help='Only create snapshot, do not start scraping')
    parser.add_argument('--scrape-only', action='store_true',
                       help='Only run scraper (snapshot must already exist)')
    parser.add_argument('--status', action='store_true',
                       help='Show current scraping status and exit')
    parser.add_argument('--snapshot-file', '-s', type=str, default='data/match_snapshot.json',
                       help='Path to snapshot file (default: data/match_snapshot.json)')
    parser.add_argument('--output-dir', '-o', type=str, default='data/enhanced',
                       help='Output directory (default: data/enhanced)')
    parser.add_argument('--num-ids', type=int, default=15000,
                       help='Number of match IDs to collect in snapshot (default: 15000)')
    parser.add_argument('--num-matches', type=int, default=10000,
                       help='Target number of valid matches to scrape (default: 10000)')
    
    args = parser.parse_args()
    
    launcher = Scraper10KLauncher(
        snapshot_file=args.snapshot_file,
        output_dir=args.output_dir,
        num_snapshot_ids=args.num_ids,
        num_matches_target=args.num_matches
    )
    
    if args.status:
        launcher.print_status()
        return
    
    launcher.run(
        resume=args.resume,
        snapshot_only=args.snapshot_only,
        scrape_only=args.scrape_only
    )

if __name__ == "__main__":
    main()







