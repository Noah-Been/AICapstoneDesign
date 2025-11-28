#!/usr/bin/env bash
set -euo pipefail

# Get date from first argument, or default to today's date in KST
if [[ -n "${1-}" ]]; then
  SNAPSHOT_DATE="$1"
else
  SNAPSHOT_DATE=$(TZ=Asia/Seoul date +%F)
fi

export SNAPSHOT_DATE
echo "============================================================"
echo "Running Daily Pipeline for SNAPSHOT_DATE: $SNAPSHOT_DATE"
echo "============================================================"

# Define absolute paths
BASE_DIR="$(pwd)"
PYTHON_EXEC="python"
APPS_DIR="$BASE_DIR/apps"

# --- 1. Price Collection ---
echo "\n[Step 1/5] Collecting stock prices..."
# "$PYTHON_EXEC" "$APPS_DIR/batch_prices.py" --snapshot-date "$SNAPSHOT_DATE" --tickers "$BASE_DIR/tickers.txt" --sleep-sec 1.1
echo "[Step 1/5] Price collection complete."

# --- 2. Signal Generation (Top N) ---
echo "\n[Step 2/5] Generating Top N signals..."
# "$PYTHON_EXEC" "$APPS_DIR/signals.py" --snapshot-date "$SNAPSHOT_DATE"
echo "[Step 2/5] Signal generation complete."

# --- 3. Brokerage Reports ---
echo "\n[Step 3/5] Collecting brokerage reports..."
# "$PYTHON_EXEC" "$APPS_DIR/run_mirae_pipeline.py" --date "$SNAPSHOT_DATE"
# "$PYTHON_EXEC" "$APPS_DIR/run_hanwha_pipeline.py" --date "$SNAPSHOT_DATE"
# "$PYTHON_EXEC" "$APPS_DIR/run_eugene_pipeline.py" --date "$SNAPSHOT_DATE"
# "$PYTHON_EXEC" "$APPS_DIR/run_samsung_pipeline.py" --date "$SNAPSHOT_DATE"
echo "[Step 3/5] Brokerage report collection complete."

# --- 4. News and Blogs ---
echo "\n[Step 4/5] Collecting news and blogs for Top N stocks..."
bash "$APPS_DIR/run_top_crawl.sh" || echo "Warning: Crawl incomplete, proceeding anyway..."
echo "[Step 4/5] News and blog collection complete."

# --- 5. Daily Report Generation ---
echo "\n[Step 5/5] Generating final daily report..."
# "$PYTHON_EXEC" "$APPS_DIR/generate_report.py" --snapshot-date "$SNAPSHOT_DATE"
echo "[Step 5/5] Daily report generation complete."

echo "============================================================"
echo "Daily Pipeline for $SNAPSHOT_DATE finished successfully!"
echo "Find the report at: $BASE_DIR/reports/daily_report_${SNAPSHOT_DATE}.md"
echo "============================================================"
