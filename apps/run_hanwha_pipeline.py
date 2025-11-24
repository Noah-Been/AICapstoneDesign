import argparse
import os
import re
import json
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Tuple

import httpx
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, urlunparse
from dotenv import load_dotenv

# --- Configuration ---
BASE = "https://m.hanwhawm.com:9090"
LIST_PATH = "/M/main/research/main/list.cmd?depth3_id=anls1"
VIEW_DEFAULT = "/M/main/research/main/view.cmd"
KST = timezone(timedelta(hours=9))

# --- Stage 1: Fetch Report List ---

def canon_url(u: str) -> str:
    try:
        p = urlparse(u)
        return urlunparse((p.scheme, p.netloc, p.path, "", p.query, ""))
    except Exception:
        return u

def parse_hanwha_daily_rows(html: str, target_date: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "lxml")
    ul = soup.find("ul", class_="bbs_researchList")
    if not ul: return []

    items = ul.find_all("li")
    out: List[Dict[str, Any]] = []
    seen: set[Tuple[str, str]] = set()

    for li in items:
        date_el = li.find("em", class_="date")
        if not date_el: continue
        
        date_txt_from_site = (date_el.get_text(strip=True) or "")
        date_txt_standard = date_txt_from_site.replace('/', '-')

        if date_txt_standard != target_date: continue

        a_tag = li.find("a")
        if not a_tag: continue

        title_el = li.find("span", class_="tit")
        if not title_el: continue
        title = title_el.get_text(strip=True)

        href = a_tag.get("href") or ""
        seq_no = None
        m = re.search(r"view\('(\d+)'", href)
        if m: seq_no = m.group(1)
        if not seq_no: continue

        detail_url = f"{BASE}{VIEW_DEFAULT}?depth3_id=anls1&seq={seq_no}"
        
        key = (title, canon_url(detail_url))
        if key in seen: continue
        seen.add(key)

        info_el = li.find(class_="info")
        author = ""
        if info_el:
            author_text = info_el.get_text(" ", strip=True)
            author = author_text.replace(date_txt_from_site, "").strip()

        out.append({
            "house": "한화투자증권",
            "house_id": "hanwha",
            "title": title,
            "url": detail_url,
            "published_at": date_txt_standard,
            "author": author,
            "source": "broker_research",
        })
    return out

# --- Stage 2: Fetch Content from Detail Page ---

def fetch_hanwha_content(session: httpx.Client, detail_url: str) -> str:
    try:
        print(f"  [Step 1] Fetching content from {detail_url}")
        html = session.get(detail_url).text
        soup = BeautifulSoup(html, "lxml")
        content_div = soup.find("div", class_="bbs_viewContainer")
        if content_div:
            # Remove the attachment table if it exists
            attach_div = content_div.find("div", class_="researchAttach")
            if attach_div: attach_div.decompose()
            
            text = content_div.get_text("\n", strip=True)
            print("    - Content fetched successfully.")
            return text
    except Exception as e:
        print(f"    - ERROR: Could not fetch content: {e}")
    return ""

# --- Stage 3: Append to JSONL ---

def append_to_jsonl(file_path: str, data: Dict[str, Any]):
    with open(file_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(data, ensure_ascii=False) + "\n")

# --- Main Pipeline ---

def main():
    parser = argparse.ArgumentParser(description="Pipeline for Hanwha reports using web page content.")
    parser.add_argument("--date", default=datetime.now(KST).strftime('%Y-%m-%d'), help="Target date in YYYY-MM-DD format.")
    parser.add_argument("--outdir", default=os.path.join(os.getcwd(), "data", "snapshots"), help="Main output directory for snapshots.")
    args = parser.parse_args()

    headers = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"}
    output_dir_dated = os.path.join(args.outdir, args.date, "research")
    os.makedirs(output_dir_dated, exist_ok=True)

    print(f"--- Starting Hanwha (Web Content) Pipeline for {args.date} ---")
    with httpx.Client(headers=headers, timeout=20.0, follow_redirects=True, verify=False) as session:
        try:
            list_html = session.get(f"{BASE}{LIST_PATH}").text
            reports_to_process = parse_hanwha_daily_rows(list_html, args.date)
            print(f"Found {len(reports_to_process)} reports to process.")
        except Exception as e:
            print(f"Failed to fetch report list: {e}")
            reports_to_process = []

        for report in reports_to_process:
            print(f"\n--- Processing Report: {report['title']} ---")
            
            content = fetch_hanwha_content(session, report['url'])
            if not content:
                print("    - WARNING: No content found, skipping.")
                continue

            final_data = report.copy()
            final_data['content'] = content

            try:
                all_jsonl_path = os.path.join(output_dir_dated, "all.jsonl")
                hanwha_jsonl_path = os.path.join(output_dir_dated, "by_house", "hanwha.jsonl")
                os.makedirs(os.path.dirname(hanwha_jsonl_path), exist_ok=True)
                
                append_to_jsonl(all_jsonl_path, final_data)
                append_to_jsonl(hanwha_jsonl_path, final_data)
                print(f"  [Step 2] Successfully processed and saved content for '{report['title']}'.")
            except Exception as e:
                print(f"    - ERROR: Failed to save JSONL: {e}")

    print("\n--- Pipeline Finished ---")

if __name__ == "__main__":
    main()