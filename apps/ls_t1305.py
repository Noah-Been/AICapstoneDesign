from __future__ import annotations

import argparse
import csv
import os
from typing import Any, Dict, List

import httpx
from loguru import logger
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential
from dotenv import load_dotenv

def _bool_env(name: str, default: bool = True) -> bool:
    val = os.environ.get(name)
    if val is None:
        return default
    return str(val).strip().lower() not in {"0", "false", "no"}


class LsOpenApiT1305:
    def __init__(self) -> None:
        self.base_url = os.environ.get("LS_BASE_URL", "https://openapi.ls-sec.co.kr:8080").rstrip("/")
        self.tr_path = os.environ.get("LS_TR_GATEWAY_PATH", "/stock/market-data")
        self.app_key = os.environ.get("LS_APP_KEY")
        self.app_secret = os.environ.get("LS_SECRET_KEY")
        self.verify_ssl = _bool_env("LS_VERIFY_SSL", True)
        self.mac_address = os.environ.get("LS_MAC_ADDRESS", "")  # 법인 계정일 때만 필요
        self.mock = _bool_env("LS_MOCK", False)

        if not self.mock and (not self.app_key or not self.app_secret):
            raise RuntimeError("Missing LS_APP_KEY or LS_SECRET_KEY in environment. Please set them in your .env file.")

        self._access_token: str | None = None

    @retry(
        reraise=True,
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=0.5, min=0.5, max=4),
        retry=retry_if_exception_type(httpx.HTTPError),
    )
    def fetch_access_token(self) -> str:
        if self.mock:
            logger.info("[MOCK] Skipping token fetch; returning dummy token")
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
        logger.info("Requesting LS access token @ {}", url)
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

    def fetch_t1305(self, shcode: str, cnt: int = 120, dwmcode: int = 1, exchgubun: str = "K") -> Dict[str, Any]:
        if self.mock:
            from datetime import datetime, timedelta
            today = datetime(2025, 10, 15)
            rows = []
            price = 90000
            for i in range(cnt):
                d = today - timedelta(days=i)
                while d.weekday() >= 5:
                    d -= timedelta(days=1)
                open_p = price
                high_p = open_p + 3000
                low_p = open_p - 2000
                close_p = open_p + (500 if i % 3 == 0 else -300)
                volume = 10_000_00 + i * 1234
                rows.append({
                    "date": d.strftime("%Y%m%d"),
                    "open": open_p,
                    "high": high_p,
                    "low": low_p,
                    "close": close_p,
                    "sign": "2" if close_p >= open_p else "5",
                    "change": close_p - open_p,
                    "diff": f"{(close_p-open_p)/open_p*100:.2f}",
                    "volume": volume,
                    "shcode": shcode,
                    "marketcap": 500_000_000,
                })
                price = close_p
            return {"t1305OutBlock": {"date": rows[-1]["date"], "cnt": cnt, "idx": 0}, "t1305OutBlock1": rows}
        total_rows: List[Dict[str, Any]] = []
        next_date = ""
        tr_cont = "N"
        tr_cont_key = ""

        while True:
            body = {
                "t1305InBlock": {
                    "shcode": shcode,
                    "dwmcode": dwmcode,
                    "date": next_date,
                    "idx": 0,
                    "cnt": max(1, min(cnt - len(total_rows), cnt)),
                    "exchgubun": exchgubun,
                }
            }
            resp = self._tr_post("t1305", body, tr_cont=tr_cont, tr_cont_key=tr_cont_key)
            try:
                resp.raise_for_status()
            except httpx.HTTPStatusError as e:
                logger.error("HTTP {}: {}", e.response.status_code, e.response.text)
                raise

            data = resp.json()

            h_tr_cont = resp.headers.get("tr_cont") or resp.headers.get("Tr-Cont") or resp.headers.get("TR_CONT")
            h_tr_cont_key = resp.headers.get("tr_cont_key") or resp.headers.get("Tr-Cont-Key") or resp.headers.get("TR_CONT_KEY") or ""

            if data.get("rsp_cd") and data["rsp_cd"] != "00000":
                msg = data.get("rsp_msg", "unknown error")
                raise RuntimeError(f"t1305 failed: {data['rsp_cd']} {msg}")

            out = data.get("t1305OutBlock", {}) or {}
            rows = data.get("t1305OutBlock1", []) or []

            if isinstance(rows, list):
                total_rows.extend(rows)

            logger.info("Fetched batch: {} rows (accum={}/{})", len(rows), len(total_rows), cnt)

            if len(total_rows) >= cnt:
                tr_cont = "N"
                break

            if (h_tr_cont or "N").upper().startswith("Y"):
                tr_cont = "Y"
                tr_cont_key = h_tr_cont_key or ""
                next_date = out.get("date", next_date)
                if not next_date:
                    logger.warning("Continuation indicated but no next date found; stopping.")
                    break
            else:
                tr_cont = "N"
                break

        return {
            "t1305OutBlock": data.get("t1305OutBlock", {}),
            "t1305OutBlock1": total_rows[:cnt],
        }


def write_csv(rows: List[Dict[str, Any]], path: str) -> None:
    if not rows:
        open(path, "w").close()
        return
    preferred = [
        "date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "value",
        "diff",
        "change",
        "sign",
        "shcode",
        "marketcap",
    ]
    cols = [c for c in preferred if c in rows[0]] + [c for c in rows[0].keys() if c not in preferred]
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in cols})


def main(argv: list[str] | None = None) -> int:
    dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
    load_dotenv(dotenv_path=dotenv_path)

    p = argparse.ArgumentParser(description="Fetch period prices via t1305 and optionally save CSV")
    p.add_argument("--shcode", required=True, help="6-digit stock code (e.g., 005930)")
    p.add_argument("--cnt", type=int, default=120, help="Number of rows to fetch (default: 120)")
    p.add_argument("--dwmcode", type=int, default=1, choices=[1, 2, 3], help="1=day, 2=week, 3=month")
    p.add_argument("--exchgubun", default="K", help="Exchange code: K/N/U (default K)")
    p.add_argument("--csv", default="", help="Output CSV path (optional)")
    args = p.parse_args(argv)

    client = LsOpenApiT1305()
    token = client.fetch_access_token()
    logger.success("Access token acquired ({} chars)", len(token))

    out = client.fetch_t1305(args.shcode, cnt=args.cnt, dwmcode=args.dwmcode, exchgubun=args.exchgubun)
    rows = out.get("t1305OutBlock1", [])
    logger.success("Fetched {} rows", len(rows))

    if args.csv:
        write_csv(rows, args.csv)
        logger.info("Saved CSV: {}", args.csv)
    else:
        for i, r in enumerate(rows[:3]):
            logger.info("row[{}]: {}", i, r)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())