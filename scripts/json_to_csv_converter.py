#!/usr/bin/env python3
"""
HLTV JSON to CSV Converter

This script converts the aggregated HLTV match data into a comprehensive CSV file
where each row represents a match with all team and player statistics flattened.

Usage:
    python json_to_csv_converter.py [input_file] [output_file]
"""

import json
import csv
import sys
import os
from typing import Dict, List, Any, Optional

class JSONToCSVConverter:
    def __init__(self, input_file: str, output_file: str):
        self.input_file = input_file
        self.output_file = output_file
        
    def load_data(self) -> Dict[str, Any]:
        """Load the input JSON data"""
        try:
            with open(self.input_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            print(f"Error: Input file '{self.input_file}' not found.")
            sys.exit(1)
        except json.JSONDecodeError as e:
            print(f"Error: Invalid JSON in input file: {e}")
            sys.exit(1)
    
    def get_player_stats(self, players: List[Dict[str, Any]], team_prefix: str) -> Dict[str, Any]:
        """Extract individual player statistics for a team"""
        stats = {}
        
        # Ensure we have exactly 5 players, pad with empty stats if needed
        for i in range(5):
            player_num = i + 1
            if i < len(players):
                player = players[i]
                player_name = player.get('name', f'Player_{player_num}')
                player_stats = player.get('statistics', {})
            else:
                player_name = f'Player_{player_num}'
                player_stats = {}
            
            # Add player name
            stats[f'{team_prefix}_player_{player_num}_name'] = player_name
            
            # Add individual statistics
            for stat in ['DPR', 'KAST', 'ADR', 'KPR', 'RATING']:
                stats[f'{team_prefix}_player_{player_num}_{stat}'] = player_stats.get(stat, None)
        
        return stats
    
    def get_team_stats(self, team_data: Dict[str, Any], team_prefix: str) -> Dict[str, Any]:
        """Extract team-level statistics"""
        stats = {}
        
        # Team name
        stats[f'{team_prefix}_name'] = team_data.get('name', '')
        
        # Team averages
        team_averages = team_data.get('team_averages', {})
        for stat in ['DPR', 'KAST', 'ADR', 'KPR', 'RATING']:
            stats[f'{team_prefix}_avg_{stat}'] = team_averages.get(stat, None)
        
        return stats
    
    def convert_match_to_row(self, match: Dict[str, Any]) -> Dict[str, Any]:
        """Convert a single match to a flat dictionary for CSV"""
        row = {}
        
        # Basic match information
        row['match_id'] = match.get('match_id', '')
        row['hltv_match_id'] = match.get('hltv_match_id', '')
        row['date'] = match.get('date', '')
        row['tournament'] = match.get('tournament', '')
        row['winner'] = match.get('winner', '')
        row['score_team1'] = match.get('score', {}).get('team1', None)
        row['score_team2'] = match.get('score', {}).get('team2', None)
        
        # Metadata
        metadata = match.get('metadata', {})
        row['match_type'] = metadata.get('match_type', '')
        row['event_type'] = metadata.get('event_type', '')
        row['scraped_date'] = metadata.get('scraped_date', '')
        row['hltv_url'] = metadata.get('hltv_url', '')
        row['match_number'] = metadata.get('match_number', None)
        
        # Team 1 data
        team1_data = match.get('team1', {})
        team1_stats = self.get_team_stats(team1_data, 'team1')
        row.update(team1_stats)
        
        # Team 1 players
        team1_players = team1_data.get('players', [])
        team1_player_stats = self.get_player_stats(team1_players, 'team1')
        row.update(team1_player_stats)
        
        # Team 2 data
        team2_data = match.get('team2', {})
        team2_stats = self.get_team_stats(team2_data, 'team2')
        row.update(team2_stats)
        
        # Team 2 players
        team2_players = team2_data.get('players', [])
        team2_player_stats = self.get_player_stats(team2_players, 'team2')
        row.update(team2_player_stats)
        
        return row
    
    def get_csv_headers(self) -> List[str]:
        """Generate CSV headers in a logical order"""
        headers = []
        
        # Basic match info
        headers.extend([
            'match_id', 'hltv_match_id', 'date', 'tournament', 'winner', 'score_team1', 'score_team2',
            'match_type', 'event_type', 'scraped_date', 'hltv_url', 'match_number'
        ])
        
        # Team 1 info
        headers.extend([
            'team1_name',
            'team1_avg_DPR', 'team1_avg_KAST', 'team1_avg_ADR', 'team1_avg_KPR', 'team1_avg_RATING'
        ])
        
        # Team 1 players
        for player_num in range(1, 6):
            headers.extend([
                f'team1_player_{player_num}_name',
                f'team1_player_{player_num}_DPR', f'team1_player_{player_num}_KAST',
                f'team1_player_{player_num}_ADR', f'team1_player_{player_num}_KPR',
                f'team1_player_{player_num}_RATING'
            ])
        
        # Team 2 info
        headers.extend([
            'team2_name',
            'team2_avg_DPR', 'team2_avg_KAST', 'team2_avg_ADR', 'team2_avg_KPR', 'team2_avg_RATING'
        ])
        
        # Team 2 players
        for player_num in range(1, 6):
            headers.extend([
                f'team2_player_{player_num}_name',
                f'team2_player_{player_num}_DPR', f'team2_player_{player_num}_KAST',
                f'team2_player_{player_num}_ADR', f'team2_player_{player_num}_KPR',
                f'team2_player_{player_num}_RATING'
            ])
        
        return headers
    
    def convert_to_csv(self) -> None:
        """Convert JSON data to CSV format"""
        print(f"Loading data from {self.input_file}...")
        data = self.load_data()
        
        # Get matches
        if 'matches' in data:
            matches = data['matches']
            print(f"Processing {len(matches)} matches...")
        else:
            # Single match format
            matches = [data]
            print("Processing single match...")
        
        # Get CSV headers
        headers = self.get_csv_headers()
        
        # Create output directory if it doesn't exist
        os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
        
        # Write CSV file
        try:
            with open(self.output_file, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=headers)
                writer.writeheader()
                
                for i, match in enumerate(matches):
                    if i % 50 == 0:
                        print(f"  Processed {i}/{len(matches)} matches...")
                    
                    row = self.convert_match_to_row(match)
                    writer.writerow(row)
            
            print(f"✅ CSV file saved to {self.output_file}")
            print(f"📊 Total rows: {len(matches)}")
            print(f"📋 Total columns: {len(headers)}")
            
        except Exception as e:
            print(f"Error saving CSV file: {e}")
            sys.exit(1)
    
    def run(self) -> None:
        """Main execution method"""
        print("📊 HLTV JSON to CSV Converter")
        print("=" * 50)
        
        self.convert_to_csv()
        
        print("\n📈 Conversion Summary:")
        print(f"  • Input file: {self.input_file}")
        print(f"  • Output file: {self.output_file}")
        print(f"  • Format: Each row = one match")
        print(f"  • Columns: Match info + Team stats + Individual player stats")

def main():
    """Main function with command line argument handling"""
    if len(sys.argv) < 2:
        print("Usage: python json_to_csv_converter.py [input_file] [output_file]")
        print("Example: python json_to_csv_converter.py data/largescale/hltv_team_averages.json data/largescale/hltv_matches.csv")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else input_file.replace('.json', '.csv')
    
    converter = JSONToCSVConverter(input_file, output_file)
    converter.run()

if __name__ == "__main__":
    main()
