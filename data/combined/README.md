# Combined HLTV Match Dataset

This folder contains the combined dataset from all checkpoint files created during the 10K match scraping process.

## Files

- `combined_matches_20251028_210538.csv` (2.6 MB) - Main dataset in CSV format
- `combined_matches_20251028_210538.json` (13 MB) - Same dataset in JSON format

## Dataset Summary

- **Total matches:** 3,912 unique matches
- **Date range:** March 5, 2025 to October 16, 2025
- **Tournaments:** 322 different tournaments
- **Teams:** 308 unique teams
- **Columns:** 116 columns with complete match statistics

## Data Sources

This dataset was created by combining 98 checkpoint files from the HLTV enhanced scraper, removing 26,385 duplicate matches to create a clean, unique dataset.

## Columns Include

- Match metadata (ID, date, tournament, winner, score)
- Map veto information
- Head-to-head statistics
- Past 3 months performance
- Map win rates for all maps
- Individual player statistics (DPR, KAST, ADR, KPR, RATING)
- Team averages
- Event type (LAN/Online)

## Usage

The CSV file is ready for analysis in Excel, Python pandas, R, or any data analysis tool. The JSON file provides the same data in a structured format for programmatic access.

## Last Updated

October 28, 2025 - 21:05 EDT

## Note

This dataset represents approximately 8.4% of the target 10,000 matches. The scraper is still running and will continue to collect more matches.






