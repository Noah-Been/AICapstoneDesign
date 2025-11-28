from __future__ import annotations

import argparse
import csv
import os
import sys
import time
from typing import List, Dict, Any

from loguru import logger

# Local import without package setup: use relative path
CURRENT_DIR = os.path.dirname(__file__)
if CURRENT_DIR not in sys.path:
    sys.path.append(CURRENT_DIR)

try:
    from ls_t3320 import LsOpenApiT3320, write_csv
except Exception as e:
    logger.error("Failed to import ls_t3320: {}", e)
    raise


def load_tickers_from_txt(path: str) -> List[str]:
    tickers: List[str] = []
    if not os.path.exists(path):
        logger.error("Ticker file not found: {}", path)
        return []
        
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            t = line.strip()
            if not t:
                continue
            tickers.append(t)
    return tickers


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Batch fetch financial data (t3320) for tickers.")
    p.add_argument("--tickers", default="tickers.txt", help="Path to text file with one ticker per line (default: tickers.txt)")
    p.add_argument("--outdir", default="data/financial_data", help="Base output directory")
    p.add_argument("--sleep-sec", type=float, default=1.1, help="Sleep seconds between calls (rate limit)")
    
    args = p.parse_args(argv)

    # Load tickers
    tickers = load_tickers_from_txt(args.tickers)
    if not tickers:
        logger.error("No tickers loaded. Check input file: {}", args.tickers)
        return 1

    logger.info("Loaded {} tickers from {}", len(tickers), args.tickers)

    # Initialize Client
    try:
        client = LsOpenApiT3320()
        token = client.fetch_access_token()
        logger.success("Access token acquired ({} chars)", len(token))
    except Exception as e:
        logger.critical("Failed to initialize API client: {}", e)
        return 1

    ok = 0
    fail = 0
    count=0
    for i, t in enumerate(tickers, 1):
        if count == 2:
            break
        count+=1
        try:
            # Fetch Data
            out = client.fetch_t3320(gicode=t)
            
            block_basic = out.get("t3320OutBlock", {}) or {}
            block_financial = out.get("t3320OutBlock1", {}) or {}
            
            merged_row = {**block_basic, **block_financial}
            
            if not merged_row:
                logger.warning("[{:04d}/{}] No data for {}", i, len(tickers), t)
                fail += 1
                time.sleep(max(0.0, args.sleep_sec))
                continue

            # Extract gsyyyy and gsmm for filename
            # Default to 'unknown_date' if missing
            gsyyyy = str(merged_row.get("gsyyyy", "")).strip()
            gsmm = str(merged_row.get("gsmm", "")).strip()
            
            if not gsyyyy or not gsmm:
                # Fallback: try to use today's date or just handle gracefully
                # But requirement says: "get the gsyyyy and gsmm value from the output"
                logger.warning("[{:04d}/{}] Missing date info (gsyyyy/gsmm) for {}, using 'nodate'", i, len(tickers), t)
                date_str = "nodate"
            else:
                date_str = f"{gsyyyy}_{gsmm}"

            # Construct path: data/financial_data/{ticker}/{gsyyyy}_{gsmm}.csv
            # Note: args.outdir is data/financial_data
            ticker_dir = os.path.join(args.outdir, t)
            os.makedirs(ticker_dir, exist_ok=True)
            
            out_csv = os.path.join(ticker_dir, f"{date_str}.csv")
            
            # Save
            write_csv([merged_row], out_csv)
            logger.info("[{:04d}/{}] Saved {} -> {}", i, len(tickers), t, out_csv)
            ok += 1

        except Exception as e:
            logger.error("[{:04d}/{}] FAIL {}: {}", i, len(tickers), t, e)
            fail += 1
        
        time.sleep(max(0.0, args.sleep_sec))

    logger.success("Done. success={}, fail={}", ok, fail)
    return 0 if fail == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
