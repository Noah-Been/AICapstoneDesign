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

# Load .env from project root (one level up from apps/)
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

class LsOpenApiT3341:
    def __init__(self) -> None:
        self.base_url = os.environ.get("LS_BASE_URL", "https://openapi.ls-sec.co.kr:8080").rstrip("/")
        self.tr_path = os.environ.get("LS_TR_GATEWAY_PATH", "/stock/investinfo")
        self.app_key = os.environ.get("LS_APP_KEY")
        self.app_secret = os.environ.get("LS_SECRET_KEY")
        self.verify_ssl = _bool_env("LS_VERIFY_SSL", True)
        self.mac_address = os.environ.get("LS_MAC_ADDRESS", "")  # Only required for corporate accounts
        self.mock = _bool_env("LS_MOCK", False)

        if not self.mock and (not self.app_key or not self.app_secret):
            raise RuntimeError("Missing LS_APP_KEY or LS_SECRET_KEY in environment.")

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

    def fetch_t3341(self, gubun: str = "0", gubun1: str = "1", gubun2: str = "1", idx: int = 0) -> Dict[str, Any]:
        """
        Fetch financial data/rankings using TR t3341.
        
        Args:
            gubun (str): Market classification (0: All, 1: KOSPI, 2: KOSDAQ) - Default "0"
            gubun1 (str): Ranking item code - Default "1" (Sales Growth?)
            gubun2 (str): Period/Criterion - Default "1"
            idx (int): Start index for pagination - Default 0
            
        Returns:
            Dict containing the API response with 't3341OutBlock' and 't3341OutBlock1'.
        """
        if self.mock:
            return {
                "t3341OutBlock": {"cnt": 2, "idx": 0},
                "t3341OutBlock1": [
                    {
                        "hname": "MockCompany A",
                        "shcode": "000001",
                        "per": "10.5",
                        "pbr": "1.0",
                        "salesgrowth": 15.5
                    },
                    {
                        "hname": "MockCompany B",
                        "shcode": "000002",
                        "per": "15.2",
                        "pbr": "2.1",
                        "salesgrowth": 20.1
                    }
                ],
                "rsp_cd": "00000",
                "rsp_msg": "Mock success"
            }

        body = {
            "t3341InBlock": {
                "gubun": gubun,
                "gubun1": gubun1,
                "gubun2": gubun2,
                "idx": idx
            }
        }

        # Send request (tr_cd="t3341")
        resp = self._tr_post("t3341", body)
        
        try:
            resp.raise_for_status()
        except httpx.HTTPStatusError as e:
            logger.error("HTTP {}: {}", e.response.status_code, e.response.text)
            raise

        data = resp.json()

        if data.get("rsp_cd") and data["rsp_cd"] != "00000":
            msg = data.get("rsp_msg", "unknown error")
            raise RuntimeError(f"t3341 failed: {data['rsp_cd']} {msg}")

        return data

def write_csv(rows: List[Dict[str, Any]], path: str) -> None:
    if not rows:
        open(path, "w").close()
        return
    
    # Preferred order for Financial/Ranking Data (t3341)
    preferred = [
        "rank",
        "hname",
        "shcode",
        "per",
        "pbr",
        "roe",
        "eps",
        "bps",
        "salesgrowth",
        "operatingincomegrowt",
        "ordinaryincomegrowth",
        "liabilitytoequity",
        "enterpriseratio",
        "peg"
    ]
    
    # Get all keys from the first row, organize them: preferred first, then the rest
    first_row_keys = rows[0].keys()
    cols = [c for c in preferred if c in first_row_keys] + [c for c in first_row_keys if c not in preferred]
    
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in cols})


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Fetch financial rankings via t3341 and optionally save CSV")
    
    # Arguments specific to t3341
    p.add_argument("--gubun", default="0", help="Market (0:All, 1:KOSPI, 2:KOSDAQ) [default: 0]")
    p.add_argument("--gubun1", default="1", help="Ranking Item Code (e.g. 1=SalesGrowth) [default: 1]")
    p.add_argument("--gubun2", default="1", help="Period/Criterion [default: 1]")
    p.add_argument("--idx", type=int, default=0, help="Start Index [default: 0]")
    
    p.add_argument("--csv", default="", help="Output CSV path (optional)")
    args = p.parse_args(argv)

    try:
        client = LsOpenApiT3341()
        token = client.fetch_access_token()
        logger.success("Access token acquired ({} chars)", len(token))

        out = client.fetch_t3341(gubun=args.gubun, gubun1=args.gubun1, gubun2=args.gubun2, idx=args.idx)
        
        # t3341 returns list in 't3341OutBlock1'
        rows = out.get("t3341OutBlock1", []) or []
        metadata = out.get("t3341OutBlock", {})
        
        logger.success("Fetched {} rows (Total count in metadata: {})", len(rows), metadata.get("cnt", "?"))

        if args.csv:
            write_csv(rows, args.csv)
            logger.info("Saved CSV: {}", args.csv)
        else:
            # Print top 5 for verification if no CSV requested
            for i, r in enumerate(rows[:5]):
                logger.info("row[{}]: {} ({}) - PER: {}, PBR: {}", 
                            i, r.get('hname'), r.get('shcode'), r.get('per'), r.get('pbr'))

        return 0
    except Exception as e:
        logger.exception("Main execution failed")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())