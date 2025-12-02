# HLTV Enhanced Match Scraper

A comprehensive, snapshot-based web scraper for collecting CS2 match statistics straight from HLTV.org's immutable **Detailed stats** pages. Every feature in the dataset is derived from what happened inside that match—no team-page aggregates, no leakage, just match-locked performance numbers.

## Features

- **Leak-free stats**: Aggregates `Op.K-D`, `MKs`, `KAST`, clutch wins, kills, assists, deaths, ADR, swing %, and Rating 3.0 for both teams directly from `/stats/matches/*`.
- **Cloudflare Bypass**: Uses `cloudscraper` to solve Cloudflare challenges automatically.
- **Respectful Scraping**: Built-in delays, exponential backoff, and pause/resume controls.
- **Snapshot-Based Scraping**: Freeze 15k+ match IDs so new HLTV results never reshuffle your queue.
- **Progress Tracking**: Checkpoints every 100 matches plus JSON/CSV exports at each save.
- **Large-Scale Support**: Battle-tested for 10,000 match runs with `nohup` + `caffeinate` helpers.

## Cloudflare Protection

This scraper uses the **`cloudscraper`** library (version 1.2.71) to automatically bypass Cloudflare challenges. The library:

- Mimics browser behavior to solve Cloudflare JavaScript challenges
- Maintains session cookies and headers
- Handles TLS fingerprinting
- Automatically retries when rate-limited

**No additional configuration needed** - just install the dependencies and the scraper handles Cloudflare automatically.

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd "NEW hltv scraping"
```

2. Create and activate a virtual environment:
```bash
python3 -m venv venv
source venv/bin/activate  # On macOS/Linux
# or
venv\Scripts\activate  # On Windows
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

### Quick Start - Scrape Recent Matches

```bash
python scripts/hltv_enhanced_scraper.py --target_match_id 0 --num_matches 10
```

### Large-Scale Scraping (10,000 matches)

#### Step 1: Create Match Snapshot
```bash
python scripts/create_match_snapshot.py --num_ids 15000 --output_file data/match_snapshot.json
```

#### Step 2: Start Scraping
```bash
python scripts/hltv_enhanced_scraper.py \
    --snapshot_file data/match_snapshot.json \
    --num_matches 10000 \
    --output_dir data/enhanced
```

#### Step 3: Keep Mac Awake (macOS only)
```bash
./run_scraper_awake.sh
```

### Combine Checkpoint Files

After scraping, combine all checkpoint files into one dataset:
```bash
python scripts/combine_checkpoints.py
```

Output will be saved to `data/combined/combined_matches_latest.csv`

## Data Structure

Every row in `combined_matches_latest.csv` contains:

### Match Metadata
- `hltv_match_id`, sequential `match_number`, `season`
- ISO timestamp, tournament name, LAN/Online label
- Team names, BO3 scoreline, canonical match URL, detailed-stats URL
- Snapshot-friendly fields: `scraped_date`, `winner`, `winner_side`

### Team Detailed Stats (per team)
- `opening_kills`, `opening_deaths`
- `multi_kill_rounds`
- `kast_pct` (average Kill/Assist/Survive/Trade)
- `clutches_won`
- `kills`, `assists`, `deaths`
- `adr`
- `swing_pct` (average round-to-round swing)
- `rating_3`

Team fields are prefixed with `team1_` and `team2_`, making feature engineering
(e.g., differences, ratios) straightforward in downstream notebooks or Kaggle
kernels.

## Output Files

### Checkpoint Files
- Saved every 100 matches: `data/enhanced/enhanced_matches_checkpoint_N_*.csv`
- Includes both CSV and JSON formats

### Progress File
- `data/enhanced/scraper_progress.json`: Tracks current progress for resume capability

### Combined Dataset
- `data/combined/combined_matches_latest.csv`: All matches combined with duplicates removed
- `data/combined/combined_matches_latest.json`: JSON version

## Architecture

See `SCRAPING_ARCHITECTURE.txt` for detailed information about:
- Data extraction methods
- Column groups and descriptions
- Scraping pipeline flow
- Technical implementation details

## Command Line Arguments

### Main Scraper (`hltv_enhanced_scraper.py`)
- `--target_match_id`: Starting match ID (default: 0)
- `--num_matches`: Number of valid matches to scrape (default: 3)
- `--snapshot_file`: Path to snapshot JSON file (optional)
- `--output_dir`: Output directory (default: data/enhanced)

### Snapshot Creator (`create_match_snapshot.py`)
- `--num_ids`: Number of match IDs to collect (default: 15000)
- `--output_file`: Output JSON file path (default: data/match_snapshot.json)

### Checkpoint Combiner (`combine_checkpoints.py`)
- Automatically finds all checkpoint files and combines them
- Removes duplicates based on match_id
- Outputs to `data/combined/`

## Performance

- **Average time per match**: ~12-15 seconds (with optimized delays)
- **Rate limiting**: Automatic retry with exponential backoff
- **Checkpoint frequency**: Every 100 matches
- **Progress save frequency**: Every 10 matches

## Error Handling

- Automatic retry on rate limiting (3 attempts with increasing delays)
- Graceful handling of missing data (defaults to safe values)
- Progress saving prevents data loss on interruption
- Checkpoint files allow resuming from any point

## Requirements

- Python 3.8+
- `cloudscraper==1.2.71` (Cloudflare bypass)
- `beautifulsoup4==4.12.2` (HTML parsing)
- `requests==2.31.0` (HTTP requests)
- `pandas` (data manipulation, installed automatically)

## Notes

- The scraper is respectful of HLTV servers with built-in delays
- Avoid browsing HLTV.org while scraping to prevent rate limiting
- On macOS, use `run_scraper_awake.sh` to prevent system sleep
- Large-scale scraping may take 2-3 days for 10,000 matches

## License

[Add your license here]

## Contributing

[Add contribution guidelines here]

## Acknowledgments

- Uses `cloudscraper` by VeNoMouS for Cloudflare bypass
- Data source: HLTV.org
