#!/usr/bin/env python3
"""
Dataset Status Script

This script shows the current status of your HLTV dataset including:
- Total matches scraped
- Date range of matches
- Latest match ID
- File sizes and locations

Usage:
    python dataset_status.py
"""

import json
import csv
import os
from datetime import datetime

def get_file_size(file_path):
    """Get human-readable file size"""
    if not os.path.exists(file_path):
        return "Not found"
    
    size = os.path.getsize(file_path)
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size < 1024.0:
            return f"{size:.1f} {unit}"
        size /= 1024.0
    return f"{size:.1f} TB"

def analyze_csv(csv_path):
    """Analyze CSV file"""
    if not os.path.exists(csv_path):
        return None
    
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    
    if not rows:
        return {"total_matches": 0, "latest_match_id": None, "oldest_match_id": None}
    
    # Get match IDs
    match_ids = []
    for row in rows:
        if row.get('hltv_match_id'):
            try:
                match_ids.append(int(row['hltv_match_id']))
            except:
                pass
    
    match_ids.sort()
    
    return {
        "total_matches": len(rows),
        "latest_match_id": max(match_ids) if match_ids else None,
        "oldest_match_id": min(match_ids) if match_ids else None,
        "columns": len(reader.fieldnames) if reader.fieldnames else 0
    }

def analyze_json(json_path):
    """Analyze JSON file"""
    if not os.path.exists(json_path):
        return None
    
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    if 'matches' in data:
        matches = data['matches']
        match_ids = []
        for match in matches:
            if 'hltv_match_id' in match:
                try:
                    match_ids.append(int(match['hltv_match_id']))
                except:
                    pass
        
        match_ids.sort()
        
        return {
            "total_matches": len(matches),
            "latest_match_id": max(match_ids) if match_ids else None,
            "oldest_match_id": min(match_ids) if match_ids else None
        }
    else:
        return {"total_matches": 1, "latest_match_id": None, "oldest_match_id": None}

def main():
    """Main function"""
    print("📊 HLTV Dataset Status")
    print("=" * 50)
    
    # Check main CSV file
    csv_path = "data/largescale/hltv_matches.csv"
    print(f"\n📈 Main Dataset (CSV):")
    print(f"   File: {csv_path}")
    print(f"   Size: {get_file_size(csv_path)}")
    
    csv_data = analyze_csv(csv_path)
    if csv_data:
        print(f"   Total Matches: {csv_data['total_matches']}")
        print(f"   Columns: {csv_data['columns']}")
        if csv_data['latest_match_id']:
            print(f"   Latest Match ID: {csv_data['latest_match_id']}")
        if csv_data['oldest_match_id']:
            print(f"   Oldest Match ID: {csv_data['oldest_match_id']}")
    else:
        print("   ❌ File not found or empty")
    
    # Check team averages JSON
    json_path = "data/largescale/hltv_team_averages.json"
    print(f"\n📊 Team Averages (JSON):")
    print(f"   File: {json_path}")
    print(f"   Size: {get_file_size(json_path)}")
    
    json_data = analyze_json(json_path)
    if json_data:
        print(f"   Total Matches: {json_data['total_matches']}")
        if json_data['latest_match_id']:
            print(f"   Latest Match ID: {json_data['latest_match_id']}")
        if json_data['oldest_match_id']:
            print(f"   Oldest Match ID: {json_data['oldest_match_id']}")
    else:
        print("   ❌ File not found or empty")
    
    # Check updates directory
    updates_dir = "data/updates"
    print(f"\n🔄 Updates Directory:")
    print(f"   Directory: {updates_dir}")
    
    if os.path.exists(updates_dir):
        update_files = [f for f in os.listdir(updates_dir) if f.endswith('.json')]
        print(f"   Update Files: {len(update_files)}")
        for file in sorted(update_files)[-3:]:  # Show last 3 files
            file_path = os.path.join(updates_dir, file)
            print(f"     - {file} ({get_file_size(file_path)})")
    else:
        print("   ❌ Directory not found")
    
    # Check other data files
    print(f"\n📁 Other Data Files:")
    other_files = [
        "data/largescale/hltv_largescale_data.json",
        "data/blast/BlastLondon_data.json",
        "data/results/match_data.json",
        "data/test/hltv_test_data.json"
    ]
    
    for file_path in other_files:
        if os.path.exists(file_path):
            print(f"   ✅ {file_path} ({get_file_size(file_path)})")
        else:
            print(f"   ❌ {file_path} (not found)")
    
    # Recommendations
    print(f"\n💡 Recommendations:")
    if csv_data and csv_data['latest_match_id']:
        print(f"   • Your latest match ID is {csv_data['latest_match_id']}")
        print(f"   • Update daily_update.py with this ID for daily updates")
        print(f"   • Run 'python daily_update.py' to get new matches")
    else:
        print(f"   • Run 'python scripts/find_latest_match_id.py' to get current latest match ID")
        print(f"   • Update daily_update.py with the latest match ID")
    
    print(f"   • Run 'python daily_update.py' for daily updates")
    print(f"   • Check 'data/updates/' for new match files")

if __name__ == "__main__":
    main()
