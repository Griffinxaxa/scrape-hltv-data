# 🚀 HLTV Large-Scale Scraping Guide

## Overview
This guide covers scraping **14,000 HLTV matches** divided into **8 seasons** of 1,750 matches each for cross-validation and machine learning purposes.

## Features Included
- ✅ **Map Veto Analysis** (winner_map, loser_map, decider)
- ✅ **Head-to-Head Statistics** (frequencies and percentages)
- ✅ **Past 3 Months Performance** (win percentages)
- ✅ **Map Win Rates** (22 columns covering all 11 maps)
- ✅ **Season Tracking** (1-8 based on match counter)
- ✅ **Pause/Resume Functionality** (graceful interruption)

## Quick Start

### 1. Start Large-Scale Scraping
```bash
cd "/Users/griffindesroches/Desktop/NEW hltv scraping"
source venv/bin/activate
python scripts/hltv_largescale_scraper.py --start_match_id 2385000 --total_matches 14000
```

### 2. Pause Scraping (Graceful Stop)
```bash
# In another terminal window:
python scripts/hltv_largescale_scraper.py --pause
```

### 3. Resume Scraping
```bash
python scripts/hltv_largescale_scraper.py --resume
```

### 4. Convert to CSV (After Completion)
```bash
python run_scraper.py csv --input data/largescale/enhanced_matches_YYYYMMDD_HHMMSS.json --output data/largescale/hltv_complete_dataset.csv
```

## Season Structure

| Season | Match Range | Description |
|--------|-------------|-------------|
| 1 | 1 - 1,750 | Training Set 1 |
| 2 | 1,751 - 3,500 | Training Set 2 |
| 3 | 3,501 - 5,250 | Training Set 3 |
| 4 | 5,251 - 7,000 | Training Set 4 |
| 5 | 7,001 - 8,750 | Training Set 5 |
| 6 | 8,751 - 10,500 | Training Set 6 |
| 7 | 10,501 - 12,250 | Training Set 7 |
| 8 | 12,251 - 14,000 | Training Set 8 |

## Data Structure

### CSV Columns (116 total)
- **Basic Info** (8): match_id, hltv_match_id, date, tournament, winner, season, score_team1, score_team2
- **Map Veto** (3): winner_map, loser_map, decider_map
- **Head-to-Head** (4): winner_head2head_freq, loser_head2head_freq, winner_head2head_percentage, loser_head2head_percentage
- **Past 3 Months** (2): winner_past3, loser_past3
- **Map Win Rates** (22): winner_*map*, loser_*map* for all 11 maps
- **Metadata** (5): match_type, event_type, scraped_date, hltv_url, match_number
- **Team Stats** (72): Team averages + 5 players × 6 stats × 2 teams

## Progress Tracking

### Files Created
- `data/largescale/largescale_progress.json` - Overall progress tracking
- `data/largescale/largescale_pause.flag` - Pause signal file
- `data/largescale/enhanced_matches_*.json` - Match data files

### Progress Information
```json
{
  "matches_completed": 1750,
  "current_season": 1,
  "last_match_id": 2383250,
  "start_time": "2025-09-18T12:00:00",
  "batches_completed": 18,
  "last_updated": "2025-09-18T14:30:00"
}
```

## Performance Considerations

### Batch Processing
- **Batch Size**: 100 matches per batch
- **Memory Management**: Each batch is processed independently
- **Progress Saves**: Every 10 matches + end of each batch

### Rate Limiting
- **Page Delay**: 2 seconds between pages
- **Match Delay**: 1 second between matches
- **Player Delay**: 0.5 seconds between players
- **Batch Delay**: 5 seconds between batches

### Estimated Time
- **Per Match**: ~15-30 seconds (including all features)
- **Per Batch (100 matches)**: ~25-50 minutes
- **Total (14,000 matches)**: ~60-120 hours

## Error Handling

### Automatic Recovery
- Progress saved every 10 matches
- Graceful pause handling
- Resume from exact position
- Default values for missing data

### Manual Intervention
- Create pause file: `--pause`
- Resume scraping: `--resume` 
- Check progress: View `largescale_progress.json`

## Data Quality

### Validation
- All matches include complete feature set
- Season assignment verified (1-8)
- Map win rates from official team stats
- Head-to-head from match history

### Cross-Validation Ready
- 8 equal seasons for k-fold validation
- Chronological ordering maintained
- No data leakage between seasons

## Ready for Production! 🎯

The enhanced HLTV scraper is now ready for massive-scale data collection with:
- **Complete feature extraction**
- **Robust pause/resume system**
- **Season-based organization**
- **Progress tracking**
- **Error recovery**

Execute the large-scale scraping when ready!
