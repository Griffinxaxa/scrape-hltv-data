#!/usr/bin/env python3
"""
HLTV Team Statistics Aggregator

This script takes the scraped match data and aggregates individual player statistics
into team averages for each match. It creates a new JSON file with team-level statistics.

Usage:
    python aggregate_team_stats.py [input_file] [output_file]
"""

import json
import sys
import os
from typing import Dict, List, Any, Optional
from statistics import mean

class TeamStatsAggregator:
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
    
    def calculate_team_averages(self, players: List[Dict[str, Any]]) -> Dict[str, Optional[float]]:
        """Calculate average statistics for a team's players"""
        stats_keys = ['DPR', 'KAST', 'ADR', 'KPR', 'RATING']
        team_averages = {}
        
        for stat in stats_keys:
            values = []
            for player in players:
                if player.get('statistics') and player['statistics'].get(stat) is not None:
                    values.append(player['statistics'][stat])
            
            if values:
                team_averages[stat] = round(mean(values), 2)
            else:
                team_averages[stat] = None
                
        return team_averages
    
    def aggregate_match_data(self, match: Dict[str, Any]) -> Dict[str, Any]:
        """Aggregate a single match's data with team averages"""
        # Create a copy of the match data
        aggregated_match = match.copy()
        
        # Calculate team averages for team1
        if 'team1' in match and 'players' in match['team1']:
            team1_averages = self.calculate_team_averages(match['team1']['players'])
            aggregated_match['team1']['team_averages'] = team1_averages
        
        # Calculate team averages for team2
        if 'team2' in match and 'players' in match['team2']:
            team2_averages = self.calculate_team_averages(match['team2']['players'])
            aggregated_match['team2']['team_averages'] = team2_averages
        
        return aggregated_match
    
    def process_data(self) -> Dict[str, Any]:
        """Process all matches and add team averages"""
        print(f"Loading data from {self.input_file}...")
        data = self.load_data()
        
        # Check if this is a large-scale scraping result or single match
        if 'matches' in data:
            # Large-scale scraping format
            print(f"Processing {len(data['matches'])} matches...")
            aggregated_matches = []
            
            for i, match in enumerate(data['matches']):
                if i % 50 == 0:
                    print(f"  Processed {i}/{len(data['matches'])} matches...")
                
                aggregated_match = self.aggregate_match_data(match)
                aggregated_matches.append(aggregated_match)
            
            # Update the data structure
            data['matches'] = aggregated_matches
            data['aggregation_info'] = {
                'aggregated_date': data.get('scraping_session', {}).get('scraped_date', 'unknown'),
                'total_matches_aggregated': len(aggregated_matches),
                'statistics_included': ['DPR', 'KAST', 'ADR', 'KPR', 'RATING']
            }
            
        else:
            # Single match format
            print("Processing single match...")
            data = self.aggregate_match_data(data)
            data['aggregation_info'] = {
                'aggregated_date': data.get('metadata', {}).get('scraped_date', 'unknown'),
                'statistics_included': ['DPR', 'KAST', 'ADR', 'KPR', 'RATING']
            }
        
        return data
    
    def save_data(self, data: Dict[str, Any]) -> None:
        """Save the aggregated data to output file"""
        # Create output directory if it doesn't exist
        os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
        
        try:
            with open(self.output_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            print(f"✅ Aggregated data saved to {self.output_file}")
        except Exception as e:
            print(f"Error saving data: {e}")
            sys.exit(1)
    
    def run(self) -> None:
        """Main execution method"""
        print("🏆 HLTV Team Statistics Aggregator")
        print("=" * 50)
        
        # Process the data
        aggregated_data = self.process_data()
        
        # Save the results
        self.save_data(aggregated_data)
        
        print("\n📊 Aggregation Summary:")
        if 'matches' in aggregated_data:
            print(f"  • Total matches processed: {len(aggregated_data['matches'])}")
            print(f"  • Statistics added: DPR, KAST, ADR, KPR, RATING")
            print(f"  • Output file: {self.output_file}")
        else:
            print(f"  • Single match processed")
            print(f"  • Statistics added: DPR, KAST, ADR, KPR, RATING")
            print(f"  • Output file: {self.output_file}")

def main():
    """Main function with command line argument handling"""
    if len(sys.argv) < 2:
        print("Usage: python aggregate_team_stats.py [input_file] [output_file]")
        print("Example: python aggregate_team_stats.py data/largescale/hltv_largescale_data.json data/largescale/hltv_team_averages.json")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else input_file.replace('.json', '_team_averages.json')
    
    aggregator = TeamStatsAggregator(input_file, output_file)
    aggregator.run()

if __name__ == "__main__":
    main()
