#!/usr/bin/env python3
"""
Combine all checkpoint CSV files into one comprehensive dataset
"""

import pandas as pd
import glob
import os
from datetime import datetime

def combine_checkpoints():
    """Combine all checkpoint CSV files into one dataset"""
    
    # Get all checkpoint CSV files
    checkpoint_dir = "data/enhanced"
    pattern = os.path.join(checkpoint_dir, "enhanced_matches_checkpoint_*.csv")
    checkpoint_files = sorted(glob.glob(pattern))
    
    print(f"Found {len(checkpoint_files)} checkpoint files")
    
    if not checkpoint_files:
        print("No checkpoint files found!")
        return
    
    # Read and combine all CSV files
    all_dataframes = []
    
    for i, file_path in enumerate(checkpoint_files):
        try:
            print(f"Reading {i+1}/{len(checkpoint_files)}: {os.path.basename(file_path)}")
            df = pd.read_csv(file_path)
            
            # Skip header for all files except the first
            if i > 0:
                df = df.iloc[1:]  # Skip header row
            
            all_dataframes.append(df)
            
        except Exception as e:
            print(f"Error reading {file_path}: {e}")
            continue
    
    if not all_dataframes:
        print("No valid checkpoint files could be read!")
        return
    
    # Combine all dataframes
    print("Combining all dataframes...")
    combined_df = pd.concat(all_dataframes, ignore_index=True)
    
    # Remove duplicates based on match_id
    print("Removing duplicates...")
    initial_count = len(combined_df)
    combined_df = combined_df.drop_duplicates(subset=['match_id'], keep='first')
    final_count = len(combined_df)
    duplicates_removed = initial_count - final_count
    
    print(f"Removed {duplicates_removed} duplicate matches")
    print(f"Final dataset contains {final_count} unique matches")
    
    # Generate output filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"data/combined/combined_matches_{timestamp}.csv"
    
    # Create combined directory if it doesn't exist
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    # Save combined dataset
    print(f"Saving combined dataset to: {output_file}")
    combined_df.to_csv(output_file, index=False)
    
    # Also save as JSON for easier analysis
    json_file = output_file.replace('.csv', '.json')
    print(f"Saving JSON version to: {json_file}")
    combined_df.to_json(json_file, orient='records', indent=2)
    
    # Print summary statistics
    print("\n" + "="*50)
    print("COMBINED DATASET SUMMARY")
    print("="*50)
    print(f"Total matches: {len(combined_df)}")
    print(f"Date range: {combined_df['date'].min()} to {combined_df['date'].max()}")
    print(f"Tournaments: {combined_df['tournament'].nunique()}")
    print(f"Teams: {len(set(combined_df['team1_name'].unique()) | set(combined_df['team2_name'].unique()))}")
    print(f"Output files:")
    print(f"  CSV: {output_file}")
    print(f"  JSON: {json_file}")
    print("="*50)
    
    return output_file, json_file

if __name__ == "__main__":
    combine_checkpoints()
