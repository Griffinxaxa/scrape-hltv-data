#!/bin/bash
# Run the 10K scraper while keeping Mac awake
# This prevents sleep during the long scraping session

cd "/Users/griffindesroches/Desktop/NEW hltv scraping"

echo "🎮 ======================================================================="
echo "🎮 HLTV 10K Match Scraper - With Sleep Prevention"
echo "🎮 ======================================================================="
echo ""
echo "This will:"
echo "  1. Keep your Mac awake (prevent sleep)"
echo "  2. Run the scraper in the background"
echo "  3. Log all output to scraping_10k.log"
echo ""
echo "Press Ctrl+C to stop (will gracefully save progress)"
echo "======================================================================="
echo ""

# Activate virtual environment
source venv/bin/activate

# Kill any existing scraper process
EXISTING_PID=$(ps aux | grep '[h]ltv_enhanced_scraper.py' | awk '{print $2}')
if [ ! -z "$EXISTING_PID" ]; then
    echo "⚠️  Found existing scraper (PID: $EXISTING_PID)"
    echo "   Stopping it first..."
    kill $EXISTING_PID
    sleep 3
fi

# Start scraper in background
echo "🚀 Starting scraper..."
nohup python -u scripts/hltv_enhanced_scraper.py \
    --snapshot_file data/match_snapshot.json \
    --num_matches 10000 \
    --output_dir data/enhanced \
    >> scraping_10k.log 2>&1 &

SCRAPER_PID=$!
echo "✅ Scraper started with PID: $SCRAPER_PID"
echo ""

# Save PID for later reference
echo $SCRAPER_PID > scraper_pid.txt

echo "💤 Preventing Mac from sleeping..."
echo "   (Display will be kept awake, hard disk won't sleep)"
echo ""
echo "📊 Monitor with:"
echo "   tail -f scraping_10k.log"
echo ""
echo "⏹️  To stop everything, press Ctrl+C"
echo ""
echo "======================================================================="
echo "🔄 Scraper is running... keeping Mac awake..."
echo "======================================================================="
echo ""

# Use caffeinate to keep the Mac awake
# -d: prevent display from sleeping
# -i: prevent system from idle sleeping  
# -w $SCRAPER_PID: keep awake while this process runs
caffeinate -d -i -w $SCRAPER_PID

echo ""
echo "✅ Scraper finished or was stopped!"
echo "📊 Check progress: cat data/enhanced/scraper_progress.json"
echo "📁 Check checkpoints: ls -lht data/enhanced/enhanced_matches_checkpoint_*.json"







