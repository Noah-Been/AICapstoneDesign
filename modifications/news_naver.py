from __future__ import annotations

import argparse
import json
import os
import re
import time
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from html import unescape
from typing import Dict, List, Tuple, Any
import csv

import httpx
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from dotenv import load_dotenv

KST = timezone(timedelta(hours=9))


def load_top_tickers(top_file: str) -> List[str]:
    
    with open(top_file, "r", encoding="utf-8") as f:
        arr = json.load(f)
    tickers = [str(x.get("ticker")) for x in arr if x.get("ticker")]
    return [t.zfill(6) for t in tickers]


def load_name_map(file_path: str) -> Dict[str, str]:
    import csv
    mapping: Dict[str, str] = {}
    if not os.path.isfile(file_path):
        return mapping
    
    for enc in ("cp949", "euc-kr", "utf-8", "latin1"):
        try:
            with open(file_path, "r", encoding=enc, newline="") as f:
                reader = csv.reader(f)
                next(reader, None)  # Skip header
                for row in reader:
                    if not row or len(row) < 2:
                        continue
                    code = row[0].strip().zfill(6)
                    name = row[1].strip()
                    if len(code) == 6 and code.isdigit() and name:
                        mapping[code] = name
            break
        except Exception:
            continue
    return mapping


def strip_tags(text: str) -> str:
    text = unescape(text or "")
    return re.sub(r"<[^>]+>", "", text)


def domain_of(url: str) -> str:
    try:
        from urllib.parse import urlparse
        netloc = urlparse(url).netloc.lower()
        if netloc.startswith("www."):
            netloc = netloc[4:]
        return netloc
    except Exception:
        return ""


WHITELIST_DOMAINS = {
    "yna.co.kr", "news1.kr", "edaily.co.kr", "mk.co.kr", "hankookilbo.com",
    "chosun.com", "joins.com", "joongang.co.kr", "hani.co.kr", "khan.co.kr",
    "biz.heraldcorp.com", "heraldcorp.com", "moneys.co.kr", "fnnews.com",
    "asiae.co.kr", "view.asiae.co.kr", "ajunews.com", "digitaltoday.co.kr",
    "edaily.co.kr", "ytn.co.kr", "mbc.co.kr", "sbs.co.kr", "kbs.co.kr",
    "news.naver.com",
}


def is_whitelisted(domain: str) -> bool:
    d = (domain or "").lower()
    return d in WHITELIST_DOMAINS or any(d.endswith("." + w) for w in WHITELIST_DOMAINS)


@retry(reraise=True,
       stop=stop_after_attempt(5),
       wait=wait_exponential(multiplier=0.5, min=0.5, max=6),
       retry=retry_if_exception_type(Exception))
def fetch_html(url: str, timeout: float = 10.0) -> str:
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118 Safari/537.36",
        "Accept-Language": "ko,en;q=0.8",
    }
    with httpx.Client(headers=headers, timeout=timeout, follow_redirects=True) as c:
        r = c.get(url)
        r.raise_for_status()
        return r.text


def extract_main_text(html: str) -> str:
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "lxml")
    for tag in soup(["script", "style", "nav", "footer", "header", "aside", "form"]):
        tag.decompose()
    candidates: List[Tuple[int, str]] = []
    def tlen(node) -> int:
        return len((node.get_text(" ", strip=True) or "").strip())
    for el in soup.find_all("article"):
        candidates.append((tlen(el), el.get_text(" ", strip=True)))
    hints = ["content", "article", "post", "entry", "view", "news", "read"]
    for h in hints:
        for el in soup.select(f"[id*='{h}'],[class*='{h}']"):
            candidates.append((tlen(el), el.get_text(" ", strip=True)))
    if not candidates:
        ps = [p.get_text(" ", strip=True) for p in soup.find_all("p")]
        joined = "\n\n".join([x for x in ps if len(x) >= 40])
        if joined:
            return joined[:15000]
    if not candidates:
        body = soup.get_text(" ", strip=True)
        return (body or "")[:15000]
    best = max(candidates, key=lambda x: x[0])[1]
    return best[:15000]


def naver_headers() -> Dict[str, str]:
    cid = os.environ.get("NAVER_CLIENT_ID")
    csec = os.environ.get("NAVER_CLIENT_SECRET")
    if not cid or not csec:
        raise RuntimeError("Missing NAVER_CLIENT_ID or NAVER_CLIENT_SECRET in env. Please set them in your .env file.")
    return {
        "X-Naver-Client-Id": cid,
        "X-Naver-Client-Secret": csec,
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    }


def fetch_news_query(client: httpx.Client, query: str, max_items: int, sort: str = "date") -> List[Dict[str, Any]]:
    base = "https://openapi.naver.com/v1/search/news.json"
    items: List[Dict[str, Any]] = []
    start = 1
    while len(items) < max_items:
        display = min(100, max_items - len(items))
        params = {"query": query, "display": display, "start": start, "sort": sort}
        for attempt in range(5):
            try:
                resp = client.get(base, params=params)
                logger.debug(f"Requesting: {resp.url}")
                if resp.status_code == 429:
                    time.sleep(0.8 * (attempt + 1))
                    continue
                resp.raise_for_status()
                data = resp.json()
                break
            except Exception:
                if attempt == 4:
                    raise
                time.sleep(0.8 * (attempt + 1))
        chunk = data.get("items", []) or []
        items.extend(chunk)
        if not chunk or len(chunk) < display:
            break
        start += display
        time.sleep(0.1)
    return items[:max_items]


def to_iso(dt: datetime) -> str:
    return dt.astimezone(KST).isoformat()


def parse_pubdate(s: str) -> datetime | None:
    try:
        dt = parsedate_to_datetime(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def dedup_key(title: str, url: str, pub: datetime | None) -> str:
    t = strip_tags(title).lower().strip()
    d = domain_of(url)
    bucket = ""
    if pub:
        ts = pub.astimezone(KST).strftime("%Y%m%d%H")
        bucket = ts
    return f"{t}|{d}|{bucket}"


def collect_for_ticker(client: httpx.Client, ticker: str, name: str | None, since_kst: datetime, per_query: int, *, require_both_in_title: bool = True, with_body: bool = False) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    queries = []
    if name:
        queries.append(name)
    queries.append(ticker)
    for q in queries:
        try:
            items = fetch_news_query(client, q, per_query, sort="date")
        except Exception as e:
            logger.error("NAVER API fail {}: {}", q, e)
            items = []
        for it in items:
            title = strip_tags(it.get("title", ""))
            desc = strip_tags(it.get("description", ""))
            link = it.get("link") or ""
            origin = it.get("originallink") or ""
            chosen = link if domain_of(link) == "news.naver.com" else (origin or link)
            origin = chosen
            dom = domain_of(origin)
            pub_raw = it.get("pubDate") or ""
            pub_dt = parse_pubdate(pub_raw)
            if pub_dt is None:
                continue
            if pub_dt.astimezone(KST) < since_kst:
                continue
            tnorm = title.replace(" ", "").lower()
            ok_name = False
            if name:
                ok_name = (name.replace(" ", "").lower() in tnorm)
            ok_ticker = (ticker in tnorm)
            if require_both_in_title:
                if not ok_name:
                    continue
            else:
                if not (ok_name or ok_ticker):
                    continue
            norm_key = re.sub(r"\s+", " ", title).strip().lower()
            dk = f"{norm_key}"
            existing_idx = next((i for i, r in enumerate(results) if r.get("_norm_key") == dk), None)
            rec = {
                "ticker": ticker,
                "query": q,
                "title": title,
                "url": origin,
                "publisher": dom,
                "published_at": to_iso(pub_dt),
                "snippet": desc,
                "source": "naver_news",
                "_norm_key": dk,
                "_wl": 1 if is_whitelisted(dom) else 0,
            }
            if existing_idx is None:
                results.append(rec)
            else:
                prev = results[existing_idx]
                if rec["_wl"] > prev["_wl"]:
                    results[existing_idx] = rec
        time.sleep(0.15)
    results.sort(key=lambda x: (x["_wl"], x["published_at"]), reverse=True)
    if with_body:
        for r in results:
            try:
                html = fetch_html(r["url"], timeout=10.0)
                body = extract_main_text(html)
                r["content"] = body
            except Exception:
                r["content"] = ""
        if require_both_in_title:
            filtered: List[Dict[str, Any]] = []
            for r in results:
                ttl = (r.get("title") or "").replace(" ", "").lower()
                has_ticker = r.get("ticker") in ttl or (r.get("content") or "").find(r.get("ticker") or "") >= 0
                if has_ticker:
                    filtered.append(r)
            results = filtered
    for r in results:
        r.pop("_norm_key", None)
        r.pop("_wl", None)
    return results


def write_csv(path: str, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    os.makedirs(os.path.dirname(path), exist_ok=True)
    # Define headers based on the keys in the first row, or a fixed set if preferred
    # The keys in collect_for_ticker are: ticker, query, title, url, publisher, published_at, snippet, source, (content)
    headers = ["ticker", "query", "title", "url", "publisher", "published_at", "snippet", "source", "content"]
    
    # Filter headers that actually exist in the data (e.g. content might be missing)
    # But for CSV consistency, it's better to have fixed headers.
    # Let's just use the keys from the first item + ensure 'content' is there if needed.
    
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_jsonl(path: str, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        try:
            if os.path.exists(path) and os.path.getsize(path) == 0:
                os.remove(path)
        except Exception:
            pass
        return
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    os.replace(tmp, path)


def main(argv: List[str] | None = None) -> int:
    dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
    load_dotenv(dotenv_path=dotenv_path)

    p = argparse.ArgumentParser(description="Collect Naver News for All tickers and save CSV per ticker")
    p.add_argument("--snapshot-date", default=os.environ.get("SNAPSHOT_DATE", ""), help="YYYY-MM-DD; default env SNAPSHOT_DATE or today")
    p.add_argument("--ticker-file", default="KOSPI_KOSDAQ.csv", help="Path to KOSPI_KOSDAQ.csv")
    p.add_argument("--outdir", default="data/news_naver/{date}", help="Output dir pattern")
    p.add_argument("--days", type=int, default=1, help="Window in days (default 1 for today)")
    p.add_argument("--per-query", type=int, default=20, help="Max items per query (name/ticker)")
    p.add_argument("--topk", type=int, default=100, help="Max items per ticker after filtering (default 100)")
    p.add_argument("--omit-snippet", action="store_true", help="Do not include API snippet text")
    p.add_argument("--require-both", action="store_true", help="Require both company name and ticker in title")
    p.add_argument("--with-body", action="store_true", help="Fetch article body and include as 'content'")
    p.add_argument("--sleep-sec", type=float, default=0.2, help="Sleep between tickers")
    args = p.parse_args(argv)

    date = args.snapshot_date or datetime.now(KST).date().isoformat()
    
    # outdir logic
    outdir = args.outdir.replace("{date}", date)
    
    # Load all tickers from specified file
    names = load_name_map(args.ticker_file)
    tickers = sorted(names.keys())
    
    headers = naver_headers()
    since_kst = datetime.fromisoformat(date).replace(tzinfo=KST) - timedelta(days=args.days - 1)

    logger.info("Collecting Naver News for {} tickers since {} (KST)", len(tickers), since_kst.date().isoformat())

    with httpx.Client(headers=headers, timeout=10.0) as client:
        count = 0
        for i, t in enumerate(tickers, 1):
            if count == 1:
                break
            count += 1
            name = names.get(t)
            try:
                rows = collect_for_ticker(
                    client, t, name, since_kst, per_query=args.per_query,
                    require_both_in_title=args.require_both,
                    with_body=args.with_body,
                )
                # No strict topk limit mentioned in new requirements, but user said "cutoff is maximum 20 news per query".
                # The collect_for_ticker uses per_query.
                # If we want to limit total results, we can use args.topk.
                # User said "If it has too many news, then the cutoff should be set. The cutoff is maximum 20 news per query."
                # This seems to refer to the API query limit.
                
                if args.omit_snippet:
                    for r in rows:
                        r.pop("snippet", None)
            except httpx.HTTPStatusError as e:
                logger.error("HTTP {} for {}: {}", e.response.status_code, t, e.response.text)
                rows = []
            except Exception as e:
                logger.error("Fail {}: {}", t, e)
                rows = []
            
            if rows:
                out_path = os.path.join(outdir, f"{t}.csv")
                write_csv(out_path, rows)
                logger.info("[{:02d}/{}] {} items -> {}", i, len(tickers), len(rows), out_path)
            else:
                logger.debug("[{:02d}/{}] No items for {}", i, len(tickers), t)
                
            time.sleep(max(0.0, args.sleep_sec))

    logger.success("Done. Output dir: {}", outdir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())