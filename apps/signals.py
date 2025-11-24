from __future__ import annotations

import argparse
import csv
import json
import os
from dataclasses import dataclass
from statistics import mean
from typing import Dict, List, Tuple, Any


def safe_float(x: Any, default: float | None = None) -> float | None:
    try:
        if x is None:
            return default
        s = str(x).strip()
        if s == "" or s.lower() == "nan":
            return default
        return float(s)
    except Exception:
        return default


def read_prices_csv(path: str) -> Tuple[List[str], List[float], List[float], List[float], List[float], List[int]]:
    dates: List[str] = []
    opens: List[float] = []
    highs: List[float] = []
    lows: List[float] = []
    closes: List[float] = []
    vols: List[int] = []
    with open(path, "r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            d = row.get("date") or row.get("Date")
            o = safe_float(row.get("open"))
            h = safe_float(row.get("high"))
            l = safe_float(row.get("low"))
            c = safe_float(row.get("close"))
            v = row.get("volume")
            v_int = None
            try:
                v_int = int(float(v)) if v is not None and str(v).strip() != "" else None
            except Exception:
                v_int = None
            if d and o is not None and h is not None and l is not None and c is not None and v_int is not None:
                dates.append(d)
                opens.append(o)
                highs.append(h)
                lows.append(l)
                closes.append(c)
                vols.append(v_int)

    # Ensure newest→oldest ordering by sorting dates descending if needed
    # Assume date in YYYYMMDD. If not strictly sorted, sort now.
    try:
        pairs = list(zip(dates, opens, highs, lows, closes, vols))
        pairs.sort(key=lambda x: x[0], reverse=True)
        dates, opens, highs, lows, closes, vols = map(list, zip(*pairs))
    except Exception:
        pass
    return dates, opens, highs, lows, closes, vols


def pct_rank(value: float, arr: List[float]) -> float:
    if not arr:
        return 0.0
    cnt = sum(1 for x in arr if x <= value)
    return cnt / len(arr) * 100.0


@dataclass
class SignalResult:
    ticker: str
    date: str
    score: float
    metrics: Dict[str, Any]


def compute_signals_for_ticker(ticker: str, csv_path: str, lookback: int = 60) -> SignalResult | None:
    dates, _opens, highs, _lows, closes, vols = read_prices_csv(csv_path)
    if len(dates) < lookback or len(closes) < lookback:
        return None
    # Latest snapshot metrics use index 0 (newest)
    close0 = closes[0]
    volume0 = vols[0]
    window_closes = closes[:lookback]
    window_highs = highs[:lookback]
    window_vols = vols[:lookback]

    high60 = max(window_highs)
    proximity_pct = 0.0 if high60 <= 0 else (high60 - close0) / high60 * 100.0  # 0=at high, larger=far
    proximity_score = max(0.0, 100.0 - proximity_pct)  # higher is better, cap at 100

    if len(closes) > 20:
        ret20 = (close0 / closes[20] - 1.0) * 100.0
    else:
        ret20 = None

    vol_pct = pct_rank(volume0, window_vols)

    ma20 = mean(window_closes[:20]) if len(window_closes) >= 20 else None
    ma60 = mean(window_closes[:60]) if len(window_closes) >= 60 else None
    above_ma20 = (ma20 is not None and close0 > ma20)
    trend_ma = (ma20 is not None and ma60 is not None and ma20 > ma60)
    ma_points = (50.0 if above_ma20 else 0.0) + (50.0 if trend_ma else 0.0)

    # Build score with simple weights
    # proximity_score in [0,100]
    # ret20 normalized: map -20..+20 to 0..100
    def normalize_ret20(x: float | None) -> float:
        if x is None:
            return 50.0
        lo, hi = -20.0, 20.0
        if x <= lo:
            return 0.0
        if x >= hi:
            return 100.0
        return (x - lo) / (hi - lo) * 100.0

    w_prox, w_ret, w_vol, w_ma = 0.5, 0.3, 0.15, 0.05
    score = (
        w_prox * proximity_score
        + w_ret * normalize_ret20(ret20)
        + w_vol * vol_pct
        + w_ma * ma_points
    )

    metrics = {
        "close": close0,
        "high60": high60,
        "proximity_pct": round(proximity_pct, 2),
        "proximity_score": round(proximity_score, 2),
        "ret20_pct": None if ret20 is None else round(ret20, 2),
        "vol_percentile": round(vol_pct, 1),
        "ma20": None if ma20 is None else round(ma20, 2),
        "ma60": None if ma60 is None else round(ma60, 2),
        "above_ma20": above_ma20,
        "ma20_gt_ma60": trend_ma,
    }
    return SignalResult(ticker=ticker, date=dates[0], score=round(score, 3), metrics=metrics)


def write_jsonl(path: str, items: List[SignalResult]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for it in items:
            f.write(json.dumps({
                "ticker": it.ticker,
                "date": it.date,
                "score": it.score,
                "metrics": it.metrics,
            }, ensure_ascii=False) + "\n")


def write_topn_json(path: str, items: List[SignalResult], top_n: int) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    payload = [
        {
            "rank": i + 1,
            "ticker": it.ticker,
            "score": it.score,
            "date": it.date,
            "metrics": it.metrics,
        }
        for i, it in enumerate(items[:top_n])
    ]
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def main(argv: List[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Compute ranking signals from CSV prices and output JSONL + Top-N JSON")
    p.add_argument("--snapshot-date", default=os.environ.get("SNAPSHOT_DATE", ""), help="YYYY-MM-DD; default env SNAPSHOT_DATE")
    p.add_argument("--prices-dir", default="/home/jinwoo0111/빅데이터캡디/mvp/data/snapshots/{date}/prices", help="Dir with {ticker}.csv files")
    p.add_argument("--out-jsonl", default="/home/jinwoo0111/빅데이터캡디/mvp/data/snapshots/{date}/rank_signals.jsonl", help="Output JSONL path")
    p.add_argument("--top-n", type=int, default=20, help="Top N count to export")
    p.add_argument("--out-top", default="/home/jinwoo0111/빅데이터캡디/mvp/data/snapshots/{date}/topN.json", help="Top-N JSON output path")
    p.add_argument("--lookback", type=int, default=60, help="Lookback bars for signals (default 60)")
    args = p.parse_args(argv)

    date = args.snapshot_date or __import__("datetime").date.today().isoformat()
    prices_dir = args.prices_dir.replace("{date}", date)
    out_jsonl = args.out_jsonl.replace("{date}", date)
    out_top = args.out_top.replace("{date}", date)

    if not os.path.isdir(prices_dir):
        print(f"Prices dir not found: {prices_dir}")
        return 2

    results: List[SignalResult] = []
    files = [f for f in os.listdir(prices_dir) if f.endswith('.csv')]
    files.sort()
    for i, fname in enumerate(files, 1):
        ticker = fname[:-4]
        csv_path = os.path.join(prices_dir, fname)
        res = compute_signals_for_ticker(ticker, csv_path, lookback=args.lookback)
        if res is None:
            continue
        results.append(res)

    # Sort by score desc
    results.sort(key=lambda x: x.score, reverse=True)
    write_jsonl(out_jsonl, results)
    write_topn_json(out_top, results, args.top_n)

    # Print a small preview
    print(f"Computed signals for {len(results)} tickers. Top {args.top_n}:")
    for i, it in enumerate(results[:args.top_n], 1):
        print(f"{i:02d}. {it.ticker}  score={it.score:.1f}  prox={it.metrics['proximity_pct']}%  ret20={it.metrics['ret20_pct']}")

    print(f"Saved: {out_jsonl}")
    print(f"Saved: {out_top}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

