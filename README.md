# HLTV Match Data Scraper

A comprehensive Python scraper for extracting match data from HLTV.org, including team information, player statistics, and match details.

## 📁 Project Structure

```
hltv-scraping/
├── data/                          # All scraped data organized by type
│   ├── blast/                     # BLAST Premier event data
│   ├── largescale/                # Large-scale scraping results (500+ matches)
│   ├── test/                      # Test scraping results
│   ├── results/                   # Single match results
│   └── sample.json               # Sample data format
├── scripts/                       # All scraper scripts
│   ├── hltv_BLASTscrape.py       # BLAST London 2025 event scraper
│   ├── hltv_largescale_scraper.py # Large-scale scraper (500+ matches)
│   ├── hltv_resultsscrape.py     # Single match scraper with forfeit detection
│   └── test_largescale.py        # Test scraper for small batches
├── run_scraper.py                # Main runner script
├── requirements.txt              # Python dependencies
└── README.md                     # This file
```

## 🚀 Quick Start

### 1. Setup Environment

```bash
# Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Run Scrapers

#### Option A: Using the Runner Script (Recommended)

```bash
# Run BLAST London 2025 scraper (5 matches)
python run_scraper.py blast

# Run large-scale scraper (500 matches)
python run_scraper.py largescale

# Run test scraper (10 matches)
python run_scraper.py test --matches 10

# Run single match scraper with forfeit detection
python run_scraper.py results

# Aggregate team statistics from scraped data
python run_scraper.py aggregate --input data/largescale/hltv_largescale_data.json --output data/largescale/hltv_team_averages.json

# Convert JSON data to CSV format
python run_scraper.py csv --input data/largescale/hltv_team_averages.json --output data/largescale/hltv_matches.csv

# Update with new matches since a specific match ID
python run_scraper.py update --target_match_id 2385148 --output_dir data/updates

# Run daily update (simple script)
python daily_update.py
```

#### Option B: Direct Script Execution

```bash
# BLAST scraper
python scripts/hltv_BLASTscrape.py

# Large-scale scraper
python scripts/hltv_largescale_scraper.py

# Test scraper
python scripts/test_largescale.py

# Results scraper
python scripts/hltv_resultsscrape.py
```

## 📊 Scraper Types

### 1. BLAST Scraper (`blast`)
- **Purpose**: Scrape all matches from BLAST Premier: London 2025
- **Matches**: 5 matches
- **Features**: No forfeit detection (T1 event), complete player stats
- **Output**: `data/blast/BlastLondon_data.json`

### 2. Large-Scale Scraper (`largescale`)
- **Purpose**: Scrape 500+ matches from HLTV results page
- **Matches**: 500 (configurable)
- **Features**: Pagination support, forfeit detection, progress tracking
- **Output**: `data/largescale/hltv_largescale_data.json`
- **Time Estimate**: 3.5-4.5 hours

### 3. Test Scraper (`test`)
- **Purpose**: Test scraper for small batches
- **Matches**: 10 (configurable)
- **Features**: Same as large-scale but smaller dataset
- **Output**: `data/test/hltv_test_data.json`

### 4. Results Scraper (`results`)
- **Purpose**: Single match scraper with forfeit detection
- **Matches**: 1 match (first non-forfeited)
- **Features**: Forfeit detection, team/player stats
- **Output**: `data/results/match_data.json`

## 📈 Data Structure

All scrapers output JSON data with the following structure:

### Team Statistics Aggregation

The `aggregate` scraper processes scraped match data and adds team-level statistics:

- **Team Averages**: Calculates average DPR, KAST, ADR, KPR, and RATING for each team
- **Data Structure**: Adds `team_averages` object to each team in every match
- **Usage**: Perfect for team performance analysis and comparison

Example team averages structure:
```json
"team1": {
  "name": "Team Name",
  "players": [...],
  "team_averages": {
    "DPR": 0.72,
    "KAST": 68.5,
    "ADR": 75.3,
    "KPR": 0.69,
    "RATING": 1.05
  }
}
```

### CSV Export

The `csv` converter transforms JSON data into a flat CSV structure perfect for data analysis:

- **Format**: Each row = one match
- **Columns**: 83 total columns including:
  - Match information (ID, date, tournament, winner, scores)
  - Team-level statistics (averages for DPR, KAST, ADR, KPR, RATING)
  - Individual player statistics (all 5 players from each team)
- **Usage**: Ideal for Excel, pandas, R, or any data analysis tool

CSV Structure:
- `match_id`, `date`, `tournament`, `winner`, `score_team1`, `score_team2`
- `team1_name`, `team1_avg_DPR`, `team1_avg_KAST`, etc.
- `team1_player_1_name`, `team1_player_1_DPR`, `team1_player_1_KAST`, etc.
- `team2_name`, `team2_avg_DPR`, `team2_avg_KAST`, etc.
- `team2_player_1_name`, `team2_player_1_DPR`, `team2_player_1_KAST`, etc.

### Daily Updates

The `update` scraper allows you to keep your dataset current by scraping new matches:

- **Backwards Scraping**: Scrapes matches newer than a specified match ID
- **Automatic Integration**: Appends new matches to existing CSV file
- **Smart Filtering**: Skips forfeited matches and incomplete teams
- **Progress Tracking**: Shows real-time progress during scraping

**Daily Update Workflow:**
1. **Set Target ID**: Update `TARGET_MATCH_ID` in `daily_update.py`
2. **Run Update**: `python daily_update.py`
3. **Check Results**: New matches appear at the top of your CSV

**Manual Update:**
```bash
# Scrape matches newer than match ID 2385148
python run_scraper.py update --target_match_id 2385148

# Custom output directory
python run_scraper.py update --target_match_id 2385148 --output_dir data/updates
```

### Raw Data Structure

All scrapers output JSON data with the following structure:

```json
{
  "scraping_session": {
    "target_matches": 500,
    "total_matches_scraped": 500,
    "failed_matches": 0,
    "failed_match_numbers": [],
    "scraped_date": "2025-09-11T21:27:35.958425Z"
  },
  "matches": [
    {
      "match_id": "hltv_match_1",
      "date": "",
      "tournament": "",
      "winner": "team2",
      "score": {
        "team1": 1,
        "team2": 2
      },
      "team1": {
        "name": "Team Name",
        "players": [
          {
            "name": "player_name",
            "statistics": {
              "DPR": 0.69,
              "KAST": 69.3,
              "ADR": 73.1,
              "KPR": 0.65,
              "RATING": 1.02
            }
          }
        ]
      },
      "team2": {
        "name": "Team Name",
        "players": [...]
      },
      "metadata": {
        "match_type": "bo1",
        "event_type": "lan",
        "scraped_date": "2025-09-11T21:27:35.958425Z",
        "hltv_url": "https://www.hltv.org/matches/...",
        "match_number": 1
      }
    }
  ]
}
```

## 🎯 Player Statistics

Each player includes 5 key statistics:
- **DPR**: Damage per Round
- **KAST**: Kill/Assist/Survive/Trade percentage
- **ADR**: Average Damage per Round
- **KPR**: Kills per Round
- **RATING**: HLTV Rating 2.0

## ⚙️ Configuration

### Large-Scale Scraper Settings

```python
# In hltv_largescale_scraper.py
target_matches = 500        # Number of matches to scrape
matches_per_page = 100      # Matches per page (HLTV limit)
```

### Rate Limiting

- **Player stats delay**: 0.5 seconds between players
- **Page delay**: 3 seconds between pages
- **Cloudflare retry**: 5 seconds on challenge detection

## 🛠️ Troubleshooting

### Common Issues

1. **Cloudflare Challenges**: The scraper automatically handles these with retries
2. **Network Timeouts**: Increase timeout values in the scraper
3. **Memory Usage**: Large datasets may require significant RAM
4. **Interrupted Scraping**: Use Ctrl+C to stop gracefully (saves partial data)

### Performance Tips

1. **Run during off-peak hours** to minimize Cloudflare challenges
2. **Monitor progress** - the scraper provides detailed status updates
3. **Use test scraper first** to verify everything works
4. **Check available disk space** for large datasets

## 📝 Dependencies

- `cloudscraper==1.2.71` - Cloudflare bypass
- `beautifulsoup4==4.12.2` - HTML parsing
- `requests==2.31.0` - HTTP requests
- `playwright==1.40.0` - Browser automation (optional)

## 🔧 Development

### Adding New Scrapers

1. Create new script in `scripts/` directory
2. Follow existing naming convention: `hltv_[name]_scraper.py`
3. Update `run_scraper.py` to include new scraper
4. Add appropriate data directory in `data/`

### Modifying Data Structure

1. Update the JSON structure in scraper files
2. Update this README with new structure
3. Test with small datasets first

## 📊 Expected Performance

| Scraper Type | Matches | Estimated Time | Output Size |
|-------------|---------|----------------|-------------|
| BLAST       | 5       | 5-10 minutes   | ~50KB       |
| Test        | 10      | 3-5 minutes    | ~100KB      |
| Large-Scale | 500     | 3.5-4.5 hours  | ~25MB       |
| Results     | 1       | 1-2 minutes    | ~10KB       |

## 🎉 Success!

The scraper successfully extracts comprehensive match data including:
- ✅ Team names and scores
- ✅ Player rosters and statistics
- ✅ Match metadata and URLs
- ✅ Forfeit detection (where applicable)
- ✅ Pagination support for large datasets
- ✅ Error handling and progress tracking

Happy scraping! 🚀