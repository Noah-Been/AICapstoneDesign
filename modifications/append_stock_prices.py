from __future__ import annotations

import argparse
import csv
import os
import sys
import time
from typing import Iterable, List, Dict, Any

from loguru import logger

# Local import without package setup: use relative path
CURRENT_DIR = os.path.dirname(__file__)
if CURRENT_DIR not in sys.path:
    sys.path.append(CURRENT_DIR)

try:
    from ls_t1305 import LsOpenApiT1305, write_csv
except Exception as e:
    logger.error("Failed to import ls_t1305: {}", e)
    raise


def load_tickers_from_txt(path: str) -> List[str]:
    tickers: List[str] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            t = line.strip()
            if not t:
                continue
            tickers.append(t)
    return tickers


def load_tickers_from_csv(path: str) -> List[str]:
    tickers: List[str] = []
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        # Expect a 'ticker' column
        if 'ticker' not in reader.fieldnames if reader.fieldnames else True:
            raise RuntimeError("CSV must have a 'ticker' header column")
        for row in reader:
            t = (row.get('ticker') or '').strip()
            if not t:
                continue
            tickers.append(t)
    return tickers


def unique_preserve_order(items: Iterable[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for x in items:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out

def download_year_price(
    client: LsOpenApiT1305, 
    ticker: str, 
    out_path: str, 
    cnt: int, 
    dwmcode: int, 
    exchgubun: str
) -> bool:
    """
    Downloads full history (cnt rows) for a ticker and saves it to out_path.
    Returns True if successful, False otherwise.
    """
    try:
        out = client.fetch_t1305(ticker, cnt=cnt, dwmcode=dwmcode, exchgubun=exchgubun)
        rows = out.get("t1305OutBlock1", []) or []
        
        if not rows:
            logger.warning("No data returned for new ticker {}", ticker)
            return False
            
        write_csv(rows, out_path)
        logger.info("NEW: Saved {} rows -> {}", len(rows), out_path)
        return True
    except Exception as e:
        logger.error("FAIL (New Download) {}: {}", ticker, e)
        return False


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Batch fetch t1305 period prices for many tickers and save CSV snapshots.")
    src = p.add_mutually_exclusive_group(required=True)
    src.add_argument("--tickers", help="Path to text file with one ticker per line")
    src.add_argument("--instruments-csv", help="Path to CSV with header including 'ticker'")
    p.add_argument("--cnt", type=int, default=60, help="Rows per ticker (default 360)")
    p.add_argument("--dwmcode", type=int, default=1, choices=[1, 2, 3], help="1=day, 2=week, 3=month")
    p.add_argument("--exchgubun", default="K", help="Exchange code K/N/U (default K)")
    p.add_argument("--snapshot-date", default=os.environ.get("SNAPSHOT_DATE", ""), help="YYYY-MM-DD (default: env SNAPSHOT_DATE or today KST)")
    p.add_argument("--outdir", default="/Users/baechangbin/codes/pythonWorkspace/AICapstoneDesign_2025_2/AICapstoneDesign/price_data", help="Output dir pattern")
    p.add_argument("--sleep-sec", type=float, default=1.0, help="Sleep seconds between calls (rate limit)")
    p.add_argument("--skip-existing", action="store_true", help="Skip if CSV already exists")
    args = p.parse_args(argv)

    # Determine snapshot date
    snapshot_date = args.snapshot_date
    if not snapshot_date:
        try:
            from datetime import datetime
            from zoneinfo import ZoneInfo
            snapshot_date = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d")
        except Exception:
            from datetime import date
            snapshot_date = date.today().isoformat()
    outdir = args.outdir.replace("{date}", snapshot_date)
    os.makedirs(outdir, exist_ok=True)

    # Load tickers
    tickers: List[str]
    if args.tickers:
        tickers = load_tickers_from_txt(args.tickers)
    else:
        tickers = load_tickers_from_csv(args.instruments_csv)
    tickers = unique_preserve_order([t.strip() for t in tickers if t and t.strip()])

    if not tickers:
        logger.error("No tickers loaded. Check input file.")
        return 2

    client = LsOpenApiT1305()
    token = client.fetch_access_token()
    logger.success("Access token acquired ({} chars)", len(token))

    ok = 0
    fail = 0
    count = 0
    for i, t in enumerate(tickers, 1):
        if count == 3:
            break
        count+=1
        out_csv = os.path.join(outdir, f"{t}.csv")

        # ---------------------------------------------------------
        # UPDATE EXISTING FILE
        # ---------------------------------------------------------
        if os.path.exists(out_csv):
            if args.skip_existing:
                logger.info("[{:04d}/{}] Skip existing (flag set) {}", i, len(tickers), out_csv)
                ok += 1
                continue
            
            try:
                # 1. Read existing file to find the last date
                existing_rows = []
                last_date = None
                with open(out_csv, "r", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    existing_rows = list(reader)
                
                if existing_rows:
                    # Assumes file is sorted Descending (newest first)
                    last_date = existing_rows[0].get("date")
                
                if not last_date:
                    # File exists but is empty or invalid -> Treat as fresh download
                    logger.warning("[{:04d}/{}] Existing file empty/invalid, re-downloading...", i, len(tickers))
                    if download_year_price(client, t, out_csv, args.cnt, args.dwmcode, args.exchgubun):
                        ok += 1
                    else:
                        fail += 1
                    time.sleep(max(0.0, args.sleep_sec))
                    continue

                # 2. Fetch small buffer (e.g., 10 days) to catch missed updates (weekends/holidays)
                # This is more robust than fetching just cnt=1
                update_buffer_cnt = 10
                out = client.fetch_t1305(t, cnt=update_buffer_cnt, dwmcode=args.dwmcode, exchgubun=args.exchgubun)
                new_rows_buffer = out.get("t1305OutBlock1", []) or []

                # 3. Filter for strictly new rows
                rows_to_add = [r for r in new_rows_buffer if r.get("date") > last_date]

                if rows_to_add:
                    # Prepend new rows to existing rows
                    updated_rows = rows_to_add + existing_rows
                    write_csv(updated_rows, out_csv)
                    logger.info("[{:04d}/{}] UPDATE: Added {} new row(s) (Latest: {}) -> {}", 
                                i, len(tickers), len(rows_to_add), rows_to_add[0]['date'], out_csv)
                    ok += 1
                else:
                    logger.info("[{:04d}/{}] SKIP: Up to date (Latest: {})", i, len(tickers), last_date)
                    ok += 1

            except Exception as e:
                logger.error("[{:04d}/{}] FAIL (Update) {}: {}", i, len(tickers), t, e)
                fail += 1

        # ---------------------------------------------------------
        # NEW FILE DOWNLOAD
        # ---------------------------------------------------------
        else:
            logger.info("[{:04d}/{}] New file, downloading full history...", i, len(tickers))
            if download_year_price(client, t, out_csv, args.cnt, args.dwmcode, args.exchgubun):
                ok += 1
            else:
                fail += 1

        time.sleep(max(0.0, args.sleep_sec))

    logger.success("Done. success={}, fail={}, outdir={}", ok, fail, outdir)
    return 0 if fail == 0 else 1
        
if __name__ == "__main__":
    raise SystemExit(main())
