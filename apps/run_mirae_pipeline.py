import argparse
import os
import re
import json
import time
import subprocess
import tempfile
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Tuple

import httpx
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, urlunparse
from dotenv import load_dotenv
import google.generativeai as genai

# --- Configuration ---
BASE = "https://securities.miraeasset.com"
LIST_PATH = "/bbs/board/message/list.do?categoryId=1800"
KST = timezone(timedelta(hours=9))

# --- Stage 1: Fetch Report List ---

def canon_url(u: str) -> str:
    try:
        p = urlparse(u)
        return urlunparse((p.scheme, p.netloc, p.path, "", p.query, ""))
    except Exception:
        return u

def parse_daily_rows(html: str, target_date: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table", class_="bbs_linetype2")
    if not table: return []
    tbody = table.find("tbody")
    if not tbody: return []
    
    rows = tbody.find_all("tr")
    out: List[Dict[str, Any]] = []
    seen: set[Tuple[str, str]] = set()

    for r in rows:
        tds = r.find_all("td")
        if len(tds) < 4: continue

        date_txt = (tds[0].get_text(strip=True) or "")
        if date_txt != target_date: continue
        
        a_detail = tds[1].find('a')
        if not a_detail: continue
        
        title = a_detail.get_text(strip=True)
        href_detail = a_detail.get('href') or ''
        link = ''
        if href_detail.startswith('javascript:view('):
            try:
                inside = href_detail[href_detail.find('(')+1:href_detail.find(')')]
                parts = [p.strip().strip("'\"") for p in inside.split(',')]
                msg_id = parts[0]
                link = f"{BASE}/bbs/board/message/view.do?messageId={msg_id}&categoryId=1800"
            except Exception:
                link = ''
        if not link: link = urljoin(BASE, href_detail)
        
        key = (title, canon_url(link))
        if key in seen: continue
        seen.add(key)

        pdf_url = None
        a_pdf = tds[2].find('a', href=True)
        if a_pdf:
            href_pdf = a_pdf.get('href', '')
            if 'javascript:downConfirm' in href_pdf:
                match = re.search(r"downConfirm\('([^,]+)'", href_pdf)
                if match: pdf_url = match.group(1)

        if not pdf_url: continue

        author = (tds[3].get_text(strip=True) or "")
        
        out.append({
            "house": "미래에셋증권",
            "house_id": "miraeasset",
            "title": title,
            "url": link,
            "pdf_url": pdf_url,
            "published_at": datetime.strptime(date_txt, "%Y-%m-%d").replace(tzinfo=KST).date().isoformat(),
            "author": author,
            "source": "broker_research",
        })
    return out

# --- Stage 2: Download PDF ---

def download_pdf(session: httpx.Client, pdf_url: str, temp_dir: str) -> str | None:
    try:
        print(f"  [Step 1] Downloading PDF from {pdf_url}")
        pdf_resp = session.get(pdf_url)
        pdf_resp.raise_for_status()
        pdf_path = os.path.join(temp_dir, "report.pdf")
        with open(pdf_path, "wb") as f:
            f.write(pdf_resp.content)
        print(f"    - Saved to {pdf_path}")
        return pdf_path
    except Exception as e:
        print(f"    - ERROR: Failed to download PDF: {e}")
        return None

# --- Stage 3: OCR ---

def ocr_pdf(pdf_path: str, lang: str = 'kor') -> str | None:
    with tempfile.TemporaryDirectory() as image_temp_dir:
        print(f"  [Step 2] Performing OCR...")
        image_prefix = os.path.join(image_temp_dir, "page")
        convert_command = ["pdftoppm", "-png", pdf_path, image_prefix]
        
        try:
            subprocess.run(convert_command, check=True, capture_output=True, text=True)
        except subprocess.CalledProcessError as e:
            print(f"    - ERROR: pdftoppm failed: {e.stderr}")
            return None

        image_files = sorted([f for f in os.listdir(image_temp_dir) if f.endswith(".png")])
        print(f"    - Converted PDF to {len(image_files)} images.")

        all_text = []
        for i, image_file in enumerate(image_files, 1):
            image_path = os.path.join(image_temp_dir, image_file)
            output_prefix = os.path.join(image_temp_dir, f"ocr_output_{i}")
            ocr_command = ["tesseract", image_path, output_prefix, "-l", lang]
            
            try:
                subprocess.run(ocr_command, check=True, capture_output=True, text=True)
                with open(f"{output_prefix}.txt", "r", encoding="utf-8") as f:
                    all_text.append(f.read())
            except subprocess.CalledProcessError as e:
                print(f"      - ERROR: Tesseract failed on page {i}: {e.stderr}")
                continue
        
        if all_text:
            print("    - OCR successful.")
            return "\n\n--- Page Break ---\n\n".join(all_text)
        else:
            print("    - ERROR: OCR process did not produce any text.")
            return None

# --- Stage 4: Refine with Gemini ---

def refine_text_with_gemini(text_to_refine: str, api_key: str) -> str | None:
    print("  [Step 3] Refining text with Gemini API...")
    genai.configure(api_key=api_key)
    model = genai.GenerativeModel('gemini-2.0-flash-lite')
    
    prompt = f"""You are an expert assistant specializing in cleaning and structuring text extracted from financial research reports. The following text was extracted from a PDF report using OCR and contains numerous errors. Your task is to:
1. **Reconstruct Sentences & Paragraphs:** Fix broken sentences and merge lines into coherent paragraphs.
2. **Correct OCR Errors:** Correct obvious spelling and character recognition errors based on financial context.
3. **Format for Readability:** Structure the text with clear headings, paragraphs, and bullet points. Use markdown.
4. **Preserve Key Data:** Accurately preserve all numerical data, such as financial figures, dates, percentages, and stock prices.
5. **Handle Tables:** Format data that looks like a table as a markdown table.
6. **Language:** The output must be in Korean.

Here is the OCR text to be refined:
---
{text_to_refine}
---
"""
    
    try:
        response = model.generate_content(prompt)
        print("    - Refinement successful.")
        return response.text
    except Exception as e:
        print(f"    - ERROR: Gemini API call failed: {e}")
        return None

# --- Stage 5 & 6: Append to JSONL ---

def append_to_jsonl(file_path: str, data: Dict[str, Any]):
    with open(file_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(data, ensure_ascii=False) + "\n")

# --- Main Pipeline ---

def main():
    dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
    load_dotenv(dotenv_path=dotenv_path)

    parser = argparse.ArgumentParser(description="Full pipeline for Mirae Asset reports: Download -> OCR -> Refine -> Save")
    parser.add_argument("--date", default=datetime.now(KST).strftime('%Y-%m-%d'), help="Target date in YYYY-MM-DD format.")
    parser.add_argument("--outdir", default=os.path.join(os.getcwd(), "data", "snapshots"), help="Main output directory for snapshots.")
    args = parser.parse_args()

    gemini_api_key = os.environ.get("GEMINI_API_KEY")
    if not gemini_api_key or "YOUR_GEMINI_API_KEY" in gemini_api_key:
        print("Error: GEMINI_API_KEY not set in .env file.")
        return

    headers = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"}
    output_dir_dated = os.path.join(args.outdir, args.date, "research")
    os.makedirs(output_dir_dated, exist_ok=True)

    print(f"--- Starting Mirae Asset Pipeline for {args.date} ---")
    with httpx.Client(headers=headers, timeout=20.0, follow_redirects=True) as session:
        try:
            list_html = session.get(f"{BASE}{LIST_PATH}").text
            reports_to_process = parse_daily_rows(list_html, args.date)
            print(f"Found {len(reports_to_process)} reports to process.")
        except Exception as e:
            print(f"Failed to fetch report list: {e}")
            reports_to_process = []

        for report in reports_to_process:
            print(f"\n--- Processing Report: {report['title']} ---")
            
            with tempfile.TemporaryDirectory() as temp_dir:
                pdf_path = download_pdf(session, report["pdf_url"], temp_dir)
                if not pdf_path: continue

                ocr_text = ocr_pdf(pdf_path)
                if not ocr_text: continue

                refined_content = refine_text_with_gemini(ocr_text, gemini_api_key)
                if not refined_content: continue

                final_data = report.copy()
                final_data['content'] = refined_content
                final_data.pop('pdf_url', None)

                try:
                    all_jsonl_path = os.path.join(output_dir_dated, "all.jsonl")
                    mirae_jsonl_path = os.path.join(output_dir_dated, "by_house", "miraeasset.jsonl")
                    os.makedirs(os.path.dirname(mirae_jsonl_path), exist_ok=True)
                    
                    append_to_jsonl(all_jsonl_path, final_data)
                    append_to_jsonl(mirae_jsonl_path, final_data)
                    print(f"  [Step 4] Successfully processed and saved content for '{report['title']}'.")
                except Exception as e:
                    print(f"    - ERROR: Failed to save JSONL: {e}")

            print("\n--- Waiting for 3 seconds to avoid rate limiting ---")
            time.sleep(3)

    print("\n--- Pipeline Finished ---")

if __name__ == "__main__":
    main()