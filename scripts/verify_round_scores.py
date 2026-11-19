#!/usr/bin/env python3
"""
Verify Round-by-Round Scores

This script helps identify rows with impossible half scores in the round-by-round dataset.
It checks if first halves (rounds 1-12) sum to exactly 12 wins.

Usage:
    python scripts/verify_round_scores.py --csv_file data/round_by_round/combined_round_by_round_with_map_names.csv
    python scripts/verify_round_scores.py --csv_file data/round_by_round/combined_round_by_round_with_map_names.csv --row 100
"""

import pandas as pd
import argparse
from collections import Counter

def verify_row(df, row_idx, map_num):
    """Verify a specific row and map"""
    row = df.iloc[row_idx]
    
    startside_col = f'map{map_num}_team1_startside'
    if startside_col not in df.columns or pd.isna(row[startside_col]):
        return None
    
    # Count first half wins
    team1_wins = 0
    team2_wins = 0
    missing = 0
    invalid = []
    
    for i in range(1, 13):
        col = f'map{map_num}_round{i}_winner'
        if col in df.columns:
            winner = row[col]
            if pd.isna(winner) or winner == 'NA' or winner == '':
                missing += 1
            elif winner == 'team1':
                team1_wins += 1
            elif winner == 'team2':
                team2_wins += 1
            else:
                invalid.append((i, winner))
    
    total = team1_wins + team2_wins
    
    return {
        'row': row_idx,
        'map': map_num,
        'team1': row.get('team1_name', 'N/A'),
        'team2': row.get('team2_name', 'N/A'),
        'match_url': row.get('match_url', 'N/A'),
        'first_half': f"{team1_wins}-{team2_wins}",
        'total': total,
        'missing': missing,
        'invalid': invalid,
        'is_valid': (missing == 0 and total == 12) or (missing > 0 and total <= 12)
    }

def main():
    parser = argparse.ArgumentParser(description='Verify round-by-round scores')
    parser.add_argument('--csv_file', type=str, 
                       default='data/round_by_round/combined_round_by_round_with_map_names.csv',
                       help='Path to CSV file')
    parser.add_argument('--row', type=int, default=None,
                       help='Check specific row number')
    parser.add_argument('--map', type=int, default=None, choices=[1, 2, 3],
                       help='Check specific map (1, 2, or 3)')
    
    args = parser.parse_args()
    
    print("🔍 Loading dataset...")
    df = pd.read_csv(args.csv_file)
    print(f"   Loaded {len(df)} rows\n")
    
    if args.row is not None:
        # Check specific row
        row_idx = args.row
        if row_idx >= len(df):
            print(f"❌ Row {row_idx} is out of range (max: {len(df)-1})")
            return
        
        maps_to_check = [args.map] if args.map else [1, 2, 3]
        
        print(f"📋 Checking row {row_idx}:")
        row = df.iloc[row_idx]
        print(f"   Match: {row.get('team1_name', 'N/A')} vs {row.get('team2_name', 'N/A')}")
        print(f"   URL: {row.get('match_url', 'N/A')}\n")
        
        for map_num in maps_to_check:
            result = verify_row(df, row_idx, map_num)
            if result:
                print(f"   Map {map_num}:")
                print(f"      First half: {result['first_half']} (total: {result['total']})")
                if result['missing'] > 0:
                    print(f"      ⚠️  Missing rounds: {result['missing']}")
                if result['invalid']:
                    print(f"      ❌ Invalid values: {result['invalid']}")
                if result['total'] != 12 and result['missing'] == 0:
                    print(f"      ❌ IMPOSSIBLE: First half totals {result['total']}, should be 12!")
                elif result['is_valid']:
                    print(f"      ✅ Valid")
                print()
    else:
        # Check all rows
        print("🔍 Checking all rows for impossible first half scores...\n")
        
        issues = []
        
        for idx in range(len(df)):
            for map_num in [1, 2, 3]:
                result = verify_row(df, idx, map_num)
                if result and not result['is_valid'] and result['missing'] == 0:
                    issues.append(result)
        
        if issues:
            print(f"❌ Found {len(issues)} rows with impossible first half scores\n")
            
            # Group by issue type
            total_dist = Counter([i['total'] for i in issues])
            print("Distribution of impossible totals:")
            for tot, count in sorted(total_dist.items()):
                print(f"   {tot} total wins: {count} matches")
            
            print(f"\n📋 Sample issues:")
            for issue in issues[:10]:
                print(f"   Row {issue['row']}, Map {issue['map']}: {issue['team1']} vs {issue['team2']}")
                print(f"      First half: {issue['first_half']} (total: {issue['total']}, should be 12)")
                print(f"      URL: {issue['match_url']}\n")
            
            # Save to CSV
            issues_df = pd.DataFrame(issues)
            output_file = 'data/round_by_round/impossible_scores_found.csv'
            issues_df.to_csv(output_file, index=False)
            print(f"💾 Saved to: {output_file}")
        else:
            print("✅ No impossible first half scores found!")
            print("   All first halves correctly sum to 12 rounds\n")
            
            # Show distribution
            print("📊 First half score distribution:")
            all_scores = []
            for idx in range(len(df)):
                for map_num in [1, 2, 3]:
                    result = verify_row(df, idx, map_num)
                    if result and result['missing'] == 0:
                        all_scores.append(result['first_half'])
            
            score_dist = Counter(all_scores)
            print("   Most common scores:")
            for score, count in score_dist.most_common(10):
                print(f"      {score}: {count} matches")

if __name__ == '__main__':
    main()

