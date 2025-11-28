from __future__ import annotations

import argparse
import csv
import os
import sys
from typing import Any, Dict, List

import httpx
from loguru import logger
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential
from dotenv import load_dotenv

# Load .env from project root
# Ensure we can find the .env file regardless of where this script is run
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
dotenv_path = os.path.join(project_root, '.env')
load_dotenv(dotenv_path=dotenv_path)

def _bool_env(name: str, default: bool = True) -> bool:
    val = os.environ.get(name)
    if val is None:
        return default
    return str(val).strip().lower() not in {"0", "false", "no"}


class LsOpenApiT3320:
    def __init__(self) -> None:
        self.base_url = os.environ.get("LS_BASE_URL", "https://openapi.ls-sec.co.kr:8080").rstrip("/")
        self.tr_path = os.environ.get("LS_TR_GATEWAY_PATH", "/stock/investinfo")
        self.app_key = os.environ.get("LS_APP_KEY")
        self.app_secret = os.environ.get("LS_SECRET_KEY")
        self.verify_ssl = _bool_env("LS_VERIFY_SSL", True)
        self.mac_address = os.environ.get("LS_MAC_ADDRESS", "")
        self.mock = _bool_env("LS_MOCK", False)

        if not self.mock and (not self.app_key or not self.app_secret):
            logger.warning("LS_APP_KEY or LS_SECRET_KEY not found in environment.")

        self._access_token: str | None = None

    @retry(
        reraise=True,
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=0.5, min=0.5, max=4),
        retry=retry_if_exception_type(httpx.HTTPError),
    )
    def fetch_access_token(self) -> str:
        if self.mock:
            self._access_token = "MOCK_TOKEN"
            return self._access_token
        
        url = f"{self.base_url}/oauth2/token"
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        data = {
            "grant_type": "client_credentials",
            "appkey": self.app_key,
            "appsecretkey": self.app_secret,
            "scope": "oob",
        }
        
        with httpx.Client(verify=self.verify_ssl, timeout=10.0) as client:
            resp = client.post(url, headers=headers, data=data)
            resp.raise_for_status()
            payload = resp.json()
        
        token = payload.get("access_token")
        if not token:
            raise RuntimeError(f"No access_token in response: {payload}")
        self._access_token = token
        return token

    def _headers(self, tr_cd: str, tr_cont: str, tr_cont_key: str) -> Dict[str, str]:
        if not self._access_token:
            raise RuntimeError("Access token not fetched. Call fetch_access_token() first.")
        headers = {
            "Authorization": f"Bearer {self._access_token}",
            "Content-Type": "application/json; charset=utf-8",
            "tr_cd": tr_cd,
            "tr_cont": tr_cont,
            "tr_cont_key": tr_cont_key,
        }
        if self.mac_address:
            headers["mac_address"] = self.mac_address
        return headers

    def _tr_post(self, tr_cd: str, body: Dict[str, Any], tr_cont: str = "N", tr_cont_key: str = "") -> httpx.Response:
        url = f"{self.base_url}{self.tr_path}"
        headers = self._headers(tr_cd, tr_cont, tr_cont_key)
        with httpx.Client(verify=self.verify_ssl, timeout=30.0) as client:
            resp = client.post(url, headers=headers, json=body)
            return resp

    def fetch_t3320(self, gicode: str) -> Dict[str, Any]:
        """
        Fetch Company Summary/Financial Info using TR t3320.
        
        Args:
            gicode (str): 6-digit stock code (e.g., "005930")
            
        Returns:
            Dict containing the API response with 't3320OutBlock' (Basic Info) 
            and 't3320OutBlock1' (Financial Info).
        """
        if self.mock:
            return {
                "rsp_cd": "00000",
                "rsp_msg": "Mock Success",
                "t3320OutBlock": {
                    "company": "Mock Company",
                    "price": 50000,
                    "marketnm": "KOSPI"
                },
                "t3320OutBlock1": {
                    "gicode": gicode,
                    "per": "10.5",
                    "pbr": "1.2",
                    "eps": "5000",
                    "roe": "12.5"
                }
            }

        body = {
            "t3320InBlock": {
                "gicode": gicode
            }
        }

        # Send request (tr_cd="t3320")
        resp = self._tr_post("t3320", body)
        
        try:
            resp.raise_for_status()
        except httpx.HTTPStatusError as e:
            logger.error("HTTP {}: {}", e.response.status_code, e.response.text)
            raise

        data = resp.json()

        if data.get("rsp_cd") and data["rsp_cd"] != "00000":
            msg = data.get("rsp_msg", "unknown error")
            raise RuntimeError(f"t3320 failed: {data['rsp_cd']} {msg}")

        return data


def write_csv(rows: List[Dict[str, Any]], path: str) -> None:
    """
    Writes a list of dictionaries to a CSV file.
    Note: For t3320, 'rows' will typically be a list containing a single merged dictionary.
    """
    if not rows:
        open(path, "w").close()
        return
    
    # Preferred column order for Company Info
    preferred = [
        "company", "gicode", "price", "marketnm", "upgubunnm", # Basic Identity
        "per", "pbr", "roe", "roa", "eps", "bps", "sps", "cps", # Valuation
        "ebitda", "evebitda", "peg", "sales", "operatingincome", # Financials
        "foreignratio", "cashrate", "capital", "sigavalue", # Stats
        "gsyyyy", "gsmm", "gsym", # Fiscal Date
        "baddress", "irtel", "homeurl" # Contact
    ]
    
    # Get all keys from the first row
    first_row_keys = rows[0].keys()
    # Sort keys: preferred first, then alphabetical rest
    cols = [c for c in preferred if c in first_row_keys] + sorted([c for c in first_row_keys if c not in preferred])
    
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in cols})


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Fetch company summary and financials via t3320")
    
    # Arguments specific to t3320
    p.add_argument("--gicode", required=True, help="6-digit Stock Code (e.g. 005930)")
    p.add_argument("--csv", default="", help="Output CSV path (optional)")
    
    args = p.parse_args(argv)

    try:
        client = LsOpenApiT3320()
        token = client.fetch_access_token()
        logger.success("Access token acquired ({} chars)", len(token))

        out = client.fetch_t3320(gicode=args.gicode)
        
        # t3320 returns two separate blocks for one company.
        # We merge them into a single dictionary for easier usage/CSV saving.
        block_basic = out.get("t3320OutBlock", {}) or {}
        block_financial = out.get("t3320OutBlock1", {}) or {}
        
        # Merge dictionaries (financial overwrites basic if keys collide, though they shouldn't)
        merged_row = {**block_basic, **block_financial}
        
        if not merged_row:
            logger.warning("No data returned for code {}", args.gicode)
            return 0

        logger.success("Fetched data for: {} ({})", merged_row.get('company'), merged_row.get('gicode'))
        logger.info("Price: {}, PER: {}, PBR: {}, ROE: {}", 
                    merged_row.get('price'), merged_row.get('per'), merged_row.get('pbr'), merged_row.get('roe'))

        if args.csv:
            # write_csv expects a list of rows
            write_csv([merged_row], args.csv)
            logger.info("Saved CSV: {}", args.csv)

        return 0
    except Exception as e:
        logger.exception("Main execution failed")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())