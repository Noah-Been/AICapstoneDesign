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
    from ls_t3320 import LsOpenApiT3320, write_csv
except Exception as e:
    logger.error("Failed to import ls_t3320: {}", e)
    raise


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Simple test for LS API t3320 (Company Financial Summary).")
    
    # Arguments specific to t3320
    p.add_argument("--gicode", required=True, help="6-digit Stock Code (e.g. 005930)")
    
    p.add_argument("--outdir", default="./test_output", help="Output directory")
    
    args = p.parse_args(argv)

    # Ensure output directory exists
    os.makedirs(args.outdir, exist_ok=True)
    
    # Construct a filename
    filename = f"t3320_{args.gicode}.csv"
    out_csv = os.path.join(args.outdir, filename)

    try:
        # 1. Initialize Client
        client = LsOpenApiT3320()
        
        # 2. Authenticate
        token = client.fetch_access_token()
        logger.success("Access token acquired ({} chars)", len(token))

        # 3. Fetch Data
        logger.info("Requesting t3320 for Company Code: {}...", args.gicode)
        
        out = client.fetch_t3320(gicode=args.gicode)

        # 4. Extract and Merge Data
        # t3320 returns two separate blocks: Basic Info (OutBlock) and Financials (OutBlock1)
        block_basic = out.get("t3320OutBlock", {}) or {}
        block_financial = out.get("t3320OutBlock1", {}) or {}
        
        # Merge them into one dictionary for a complete view
        merged_row = {**block_basic, **block_financial}
        
        if not merged_row:
            logger.warning("No data returned from API for code {}", args.gicode)
            return 0

        logger.success("Fetched data for: {} ({})", merged_row.get('company'), merged_row.get('gicode'))

        # 5. Save to CSV
        # write_csv expects a list of rows, so we wrap our single merged row in a list
        write_csv([merged_row], out_csv)
        logger.info("Saved result to -> {}", out_csv)

        # 6. Preview Data
        logger.info("Preview:")
        logger.info("  Company: {}", merged_row.get('company'))
        logger.info("  Price:   {} KRW", merged_row.get('price'))
        logger.info("  PER:     {}", merged_row.get('per'))
        logger.info("  PBR:     {}", merged_row.get('pbr'))
        logger.info("  ROE:     {}%", merged_row.get('roe'))

    except Exception as e:
        logger.exception("Test execution failed")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())