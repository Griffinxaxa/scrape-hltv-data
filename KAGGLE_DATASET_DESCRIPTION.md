# CS2 Professional Match Statistics Dataset

## Overview

A comprehensive dataset of **7,033 professional Counter-Strike 2 (CS2) matches** scraped from HLTV.org, featuring detailed statistics for match analysis, player performance evaluation, and predictive modeling.

## Dataset Details

- **Total Matches**: 7,033
- **Time Period**: May 2024 - October 2025
- **Tournaments**: 648 different professional tournaments
- **Teams**: 331 unique professional teams
- **Columns**: 116 features per match

## What's Included

### Match Information
- Match date, tournament name, event type (LAN/Online)
- Final score and winner
- Map veto information (which maps each team picked/banned)

### Team Performance Statistics
- **Past Performance**: Win rates over the last 3 months
- **Head-to-Head Records**: Historical match-up statistics between teams
- **Map Win Rates**: Each team's win percentage on specific maps (Mirage, Inferno, Nuke, Dust2, Overpass, Train, Ancient, Cache, Vertigo, Anubis, Cobblestone)

### Player Statistics (All 10 Players Per Match)
For each of the 10 players in every match:
- **DPR** (Deaths Per Round): Average deaths per round
- **KAST** (Kill/Assist/Survive/Trade %): Percentage of rounds with a meaningful contribution
- **ADR** (Average Damage Per Round): Average damage dealt
- **KPR** (Kills Per Round): Average kills per round
- **Rating**: Overall performance rating

### Team Averages
Calculated averages across all 5 players for:
- Average DPR, KAST, ADR, KPR, and Rating

## Use Cases

- **Predictive Modeling**: Predict match outcomes using historical performance
- **Player Analysis**: Identify top performers and analyze player trends
- **Team Strategy**: Study map preferences and veto patterns
- **Betting Models**: Build data-driven betting prediction systems
- **Academic Research**: Statistical analysis of esports performance

## Data Quality

- All matches are Best-of-3 format (ensuring consistency)
- Excludes forfeited matches and incomplete games
- Data validated at time of collection
- Regular updates from ongoing professional matches

## Collection Method

Data collected using a respectful web scraping approach with:
- Automatic Cloudflare bypass
- Rate limiting to avoid server overload
- Checkpoint saving for data integrity
- Duplicate removal and validation

## Column Groups

1. **Basic Match Info** (9 columns): IDs, dates, tournament, winner, scores
2. **Map Information** (3 columns): Winner pick, loser pick, decider map
3. **Head-to-Head Stats** (4 columns): Historical team matchup data
4. **Past Performance** (2 columns): Recent win rates for both teams
5. **Map Win Rates** (22 columns): Team win percentages across 11 maps
6. **Team Averages** (10 columns): Average stats for both teams
7. **Player Statistics** (50 columns): Individual stats for all 10 players
8. **Metadata** (4 columns): Match type, event type, scrape date, URL

## File Format

- **CSV**: Easy to import into Excel, Python, R, or any analysis tool
- **JSON**: Structured format for programmatic access

## Updates

This dataset represents matches collected through October 2025. The collection process is ongoing, with new matches added regularly.

## Notes

- All matches are professional-level (no amateur/casual games)
- Statistics are based on official HLTV.org data
- Missing data points are represented with default values (e.g., 50% for unavailable map win rates)
- Match dates use UTC timezone

## Source

Data collected from [HLTV.org](https://www.hltv.org), the premier source for CS2 match statistics and news.

## License

[Specify your license - e.g., CC0: Public Domain, or CC BY 4.0]

---

**Perfect for**: Data scientists, esports analysts, betting model developers, researchers, and anyone interested in professional CS2 statistics!







