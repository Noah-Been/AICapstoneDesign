
import argparse
import os
import re
import json
import subprocess
import tempfile
import time
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Tuple

import httpx
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, urlunparse
from dotenv import load_dotenv
import google.generativeai as genai

# --- Configuration ---
BASE = "https://www.samsungpop.com"
LIST_URL = "https://www.samsungpop.com/sscommon/jsp/search/research/research_pop.jsp#bm"
KST = timezone(timedelta(hours=9))

# --- Stage 1: Fetch and Parse Report List ---

def parse_samsung_daily_rows(html: str, target_date: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "lxml")
    out: List[Dict[str, Any]] = []
    seen: set[Tuple[str, str]] = set()

    table = soup.find("table", class_="tbl-type board")
    if not table:
        return []

    for row in table.find_all("tr"):
        cols = row.find_all("td")
        if len(cols) != 3:
            continue

        date_txt = cols[2].get_text(strip=True)
        if date_txt != target_date:
            continue

        a_tag = cols[0].find("a")
        if not a_tag or not a_tag.get("href"):
            continue

        title = (a_tag.get("title") or a_tag.get_text(strip=True)).strip()
        author = cols[1].get_text(strip=True)
        pdf_url = urljoin(BASE, a_tag["href"])

        key = (title, pdf_url)
        if key in seen:
            continue
        seen.add(key)

        out.append({
            "house": "삼성증권",
            "house_id": "samsung",
            "title": title,
            "url": pdf_url, # Direct PDF link
            "pdf_url": pdf_url,
            "published_at": target_date,
            "author": author,
            "source": "broker_research",
        })
    return out

# --- Stage 2: Download, OCR, Refine (Reused Logic) ---

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
            return "\n\n---" + "-" * 5 + " Page Break " + "-" * 5 + "---\\n\n".join(all_text)
        else:
            print("    - ERROR: OCR process did not produce any text.")
            return None

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

def append_to_jsonl(file_path: str, data: Dict[str, Any]):
    with open(file_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(data, ensure_ascii=False) + "\n")

# --- Main Pipeline ---

def main():
    dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
    load_dotenv(dotenv_path=dotenv_path)

    parser = argparse.ArgumentParser(description="Full pipeline for Samsung Securities reports: Crawl -> Download -> OCR -> Refine -> Save")
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

    print(f"--- Starting Samsung Securities Pipeline for {args.date} ---")
    with httpx.Client(headers=headers, timeout=30.0, follow_redirects=True) as session:
        try:
            list_html = session.get(LIST_URL).text
            reports_to_process = parse_samsung_daily_rows(list_html, args.date)
            print(f"Found {len(reports_to_process)} reports to process.")
        except Exception as e:
            print(f"Failed to fetch report list: {e}")
            reports_to_process = []

        for report in reports_to_process:
            print(f"\n--- Processing Report: {report['title']} ---")
            
            pdf_url = report.get("pdf_url")
            if not pdf_url:
                print("    - WARNING: PDF URL not found, skipping.")
                continue

            with tempfile.TemporaryDirectory() as temp_dir:
                pdf_path = download_pdf(session, pdf_url, temp_dir)
                if not pdf_path: continue

                ocr_text = ocr_pdf(pdf_path)
                if not ocr_text: continue

                refined_content = refine_text_with_gemini(ocr_text, gemini_api_key)
                if not refined_content: continue

                final_data = report.copy()
                final_data['content'] = refined_content
                final_data.pop('pdf_url', None) # Clean up
                
                try:
                    all_jsonl_path = os.path.join(output_dir_dated, "all.jsonl")
                    samsung_jsonl_path = os.path.join(output_dir_dated, "by_house", "samsung.jsonl")
                    os.makedirs(os.path.dirname(samsung_jsonl_path), exist_ok=True)
                    
                    append_to_jsonl(all_jsonl_path, final_data)
                    append_to_jsonl(samsung_jsonl_path, final_data)
                    print(f"  [Step 4] Successfully processed and saved content for '{report['title']}'.")
                except Exception as e:
                    print(f"    - ERROR: Failed to save JSONL: {e}")
            
            print("\n--- Waiting for 3 seconds to avoid rate limiting ---")
            time.sleep(3)

    print("\n--- Pipeline Finished ---")

if __name__ == "__main__":
    main()
