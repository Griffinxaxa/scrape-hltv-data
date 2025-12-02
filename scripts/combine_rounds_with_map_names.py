#!/usr/bin/env python3
"""
Combine round-by-round dataset with map names.

Usage:
    python scripts/combine_rounds_with_map_names.py \
        --rounds data/round_by_round/combined_round_by_round_all.csv \
        --maps data/round_by_round/map_names.csv \
        --output data/round_by_round/combined_with_map_names.csv
"""

import argparse
import os
from typing import List

import pandas as pd


def load_csv(path: str, required_columns: List[str]) -> pd.DataFrame:
    """Load CSV and ensure required columns exist."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"File not found: {path}")
    df = pd.read_csv(path)
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"Missing columns in {path}: {', '.join(missing)}")
    return df


def combine_datasets(rounds_file: str, maps_file: str, output_file: str) -> None:
    """Merge datasets and drop rows without map names."""
    print(f"📖 Loading round-by-round data: {rounds_file}")
    rounds_df = load_csv(rounds_file, ['match_url'])
    print(f"   Rows: {len(rounds_df)}")

    print(f"📖 Loading map names: {maps_file}")
    maps_df = load_csv(maps_file, ['match_url', 'map1_name', 'map2_name', 'map3_name'])
    print(f"   Rows: {len(maps_df)}")

    # Deduplicate map names by match_url, keeping first occurrence
    before = len(maps_df)
    maps_df = maps_df.drop_duplicates(subset='match_url', keep='first')
    after = len(maps_df)
    if after != before:
        print(f"   Removed {before - after} duplicate map entries")

    # Merge datasets
    merged_df = rounds_df.merge(maps_df, on='match_url', how='left')
    print(f"🔗 Merged rows: {len(merged_df)}")

    # Drop rows where critical map names are missing
    before_drop = len(merged_df)
    merged_df = merged_df.dropna(subset=['map1_name', 'map2_name'])
    dropped = before_drop - len(merged_df)
    print(f"🧹 Dropped {dropped} rows missing map1/map2 names")

    # Replace NaN map3_name with "NA"
    merged_df['map3_name'] = merged_df['map3_name'].fillna('NA')

    # Save output
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    merged_df.to_csv(output_file, index=False)
    print(f"✅ Saved combined dataset: {output_file}")
    print(f"   Final rows: {len(merged_df)}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Combine round data with map names")
    parser.add_argument('--rounds', '-r', type=str,
                        default='data/round_by_round/combined_round_by_round_all.csv',
                        help='Round-by-round CSV file')
    parser.add_argument('--maps', '-m', type=str,
                        default='data/round_by_round/map_names.csv',
                        help='Map names CSV file')
    parser.add_argument('--output', '-o', type=str,
                        default='data/round_by_round/combined_round_by_round_with_map_names.csv',
                        help='Output CSV file')
    return parser.parse_args()


def main():
    args = parse_args()
    combine_datasets(args.rounds, args.maps, args.output)


if __name__ == '__main__':
    main()


