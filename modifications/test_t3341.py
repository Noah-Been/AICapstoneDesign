from __future__ import annotations

import argparse
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
    from ls_t3341 import LsOpenApiT3341, write_csv
except Exception as e:
    logger.error("Failed to import ls_t3341: {}", e)
    raise


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Simple test for LS API t3341 (Financial Rankings).")
    
    # Arguments specific to t3341
    p.add_argument("--gubun", default="0", help="Market (0:All, 1:KOSPI, 2:KOSDAQ) [default: 0]")
    p.add_argument("--gubun1", default="1", help="Ranking Item Code (e.g. 1=SalesGrowth) [default: 1]")
    p.add_argument("--gubun2", default="1", help="Period/Criterion [default: 1]")
    p.add_argument("--idx", type=int, default=0, help="Start Index [default: 0]")
    
    p.add_argument("--outdir", default="./test_output", help="Output directory")
    
    args = p.parse_args(argv)

    # Ensure output directory exists
    os.makedirs(args.outdir, exist_ok=True)
    
    # Construct a filename that describes the request parameters
    filename = f"t3341_M{args.gubun}_Code{args.gubun1}_Crit{args.gubun2}_idx{args.idx}.csv"
    out_csv = os.path.join(args.outdir, filename)

    try:
        # 1. Initialize Client
        client = LsOpenApiT3341()
        
        # 2. Authenticate
        token = client.fetch_access_token()
        logger.success("Access token acquired ({} chars)", len(token))

        # 3. Fetch Data
        logger.info("Requesting t3341 (Market={}, Code={}, Crit={}, Idx={})...", 
                    args.gubun, args.gubun1, args.gubun2, args.idx)
        
        out = client.fetch_t3341(
            gubun=args.gubun, 
            gubun1=args.gubun1, 
            gubun2=args.gubun2, 
            idx=args.idx
        )

        # 4. Extract Rows
        # t3341 returns the list of companies in 't3341OutBlock1'
        rows = out.get("t3341OutBlock1", []) or []
        metadata = out.get("t3341OutBlock", {})
        
        total_count = metadata.get("cnt", "Unknown")
        
        if not rows:
            logger.warning("No data returned from API.")
            return 0

        logger.success("Fetched {} rows (Total available: {})", len(rows), total_count)

        # 5. Save to CSV
        write_csv(rows, out_csv)
        logger.info("Saved result to -> {}", out_csv)

        # 6. Preview Data
        logger.info("Preview (First 3 rows):")
        for i, row in enumerate(rows[:3]):
            logger.info("Row {}: Name={}, Code={}, Value={}", 
                        i, row.get('hname'), row.get('shcode'), row.get('salesgrowth')) # Example field

    except Exception as e:
        logger.exception("Test execution failed")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())