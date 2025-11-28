#!/usr/bin/env bash
set -euo pipefail

# 1. Set Snapshot Date
# Use provided argument or default to today's date in KST
if [[ -n "${1-}" ]]; then
  SNAPSHOT_DATE="$1"
else
  SNAPSHOT_DATE=$(TZ=Asia/Seoul date +%F)
fi

echo "============================================================"
echo "Running Daily Crawling for SNAPSHOT_DATE: $SNAPSHOT_DATE"
echo "============================================================"

# Ensure we are in the project root
# If the script is run from modifications/, move up
if [[ -d "modifications" ]]; then
    BASE_DIR="$(pwd)"
elif [[ -f "run_daily_crawling.sh" ]]; then
    cd ..
    BASE_DIR="$(pwd)"
else
    # Fallback or assume running from root
    BASE_DIR="$(pwd)"
fi

PY="python"

# 2. Run Financial Data Collection
echo ""
echo "[Step 1/3] Collecting Financial Data (t3320)..."
# Using default tickers.txt and outdir data/financial_data
$PY modifications/append_financial_data.py \
    --tickers tickers.txt \
    --outdir data/financial_data \
    --sleep-sec 1.1
echo "[Step 1/3] Done."

# 3. Run Stock Price Collection
echo ""
echo "[Step 2/3] Collecting Stock Prices (t1305)..."
# Using default tickers.txt and outdir data/price_data
$PY modifications/append_stock_prices.py \
    --tickers tickers.txt \
    --outdir data/price_data \
    --snapshot-date "$SNAPSHOT_DATE" \
    --sleep-sec 1.0
echo "[Step 2/3] Done."

# 4. Run News Crawling
echo ""
echo "[Step 3/3] Collecting Naver News..."
# Using default KOSPI_KOSDAQ.csv for tickers
$PY modifications/news_naver.py \
    --snapshot-date "$SNAPSHOT_DATE" \
    --ticker-file "KOSPI_KOSDAQ.csv" \
    --outdir "data/news_naver/{date}" \
    --days 1 \
    --per-query 20 \
    --sleep-sec 0.2
echo "[Step 3/3] Done."

echo "============================================================"
echo "Daily Crawling Finished Successfully!"
echo "============================================================"
