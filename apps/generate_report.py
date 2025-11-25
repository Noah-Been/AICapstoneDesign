import argparse
import os
import json
import time
import csv
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any

import google.generativeai as genai
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential

KST = timezone(timedelta(hours=9))

# --- Data Loading Functions ---
def load_json_or_empty_list(path: str) -> List[Dict]:
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return []

def load_jsonl_or_empty_list(path: str) -> List[Dict]:
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            return [json.loads(line) for line in f if line.strip()]
    except (json.JSONDecodeError, IOError):
        return []

# --- LLM Generation Functions ---
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def generate_with_retry(model: genai.GenerativeModel, prompt: str) -> str:
    print(f"    - Calling Gemini API ({model.model_name})...")
    response = model.generate_content(prompt)
    return response.text

def generate_all_reports_summary(reports: List[Dict], model: genai.GenerativeModel) -> str:
    print("--- Generating Section 1: All Reports Summary ---")
    if not reports:
        return "### 1. 📝 증권사 리포트 전체 요약\n\n- 오늘 수집된 증권사 리포트가 없습니다.\n"

    report_texts = []
    for report in reports:
        report_texts.append(f"- **{report.get('house', '')} / {report.get('author', '')}**: {report.get('title', '')}\n  - Content: {report.get('content', '')[:1500]}")
    reports_str = "\n".join(report_texts)

    prompt = f"""You are an expert financial analyst. Based on the list of brokerage reports below, please write a concise summary for EACH report. Present the output as a markdown bulleted list.

**Reports Data:**
{reports_str}

**Your Output Format:**
- **[증권사명] 회사명:** [요약 내용]
- **[증권사명] 산업명:** [요약 내용]
"""
    
    try:
        summary = generate_with_retry(model, prompt)
        return f"### 1. 📝 증권사 리포트 전체 요약\n\n{summary}\n"
    except Exception as e:
        print(f"    - FAILED to generate all reports summary: {e}")
        return "### 1. 📝 증권사 리포트 전체 요약\n\n- 리포트 요약 생성에 실패했습니다.\n"

def generate_top_n_list(stocks: List[Dict]) -> str:
    print("--- Generating Section 2: Top 20 List ---")
    if not stocks:
        return "### 2. 🏆 Top 20 종목 및 선정 신호\n\n- Top 20 종목을 찾을 수 없습니다.\n"
    
    lines = []
    for i, stock in enumerate(stocks, 1):
        reason = f"prox: {stock['metrics']['proximity_pct']}% / ret20: {stock['metrics']['ret20_pct']}%"
        lines.append(f"{i}. **{stock.get('name', stock['ticker'])} ({stock['ticker']}):** {reason}")
    
    return f"### 2. 🏆 Top 20 종목 및 선정 신호\n\n" + "\n".join(lines) + "\n"

def generate_social_trends_section(stocks: List[Dict], base_path: str, model: genai.GenerativeModel) -> str:
    print("--- Generating Section 3: Key Stocks Social Trends ---")
    
    content_parts = []
    stocks_with_social_data = []

    for stock in stocks:
        ticker = stock.get("ticker")
        news_path = os.path.join(base_path, "news_strict", f"{ticker}.jsonl")
        blogs_path = os.path.join(base_path, "blogs_strict", f"{ticker}.jsonl")
        news = load_jsonl_or_empty_list(news_path)
        blogs = load_jsonl_or_empty_list(blogs_path)
        if news or blogs:
            stocks_with_social_data.append((stock, news, blogs))

    if not stocks_with_social_data:
        return "### 3. 📰 주요 종목 소셜 동향\n\n- Top 20 종목에 대한 최신 뉴스/블로그가 없습니다.\n"

    for i, (stock, news, blogs) in enumerate(stocks_with_social_data, 1):
        print(f"  - [{i}/{len(stocks_with_social_data)}] Generating social summary for {stock.get('name')}")
        news_str = "\n".join([f"- {item.get('title', '')}" for item in news])
        blogs_str = "\n".join([f"- {item.get('title', '')}" for item in blogs])

        prompt = f"""You are an expert financial analyst. Based on the news and blog titles below for the stock **{stock.get('name')} ({stock.get('ticker')})**, please perform two tasks:
1.  Write a summary of the latest trends and issues in Korean, presented as markdown bullet points.
2.  Write a single, concise sentence for '투자 참고사항' (Investment Reference Point) that highlights the key takeaway or risk.

**News Titles:**
{news_str if news else "N/A"}

**Blog Titles:**
{blogs_str if blogs else "N/A"}

**Your Output Format:**
- **최신 뉴스/블로그 요약:**
  - [Summary point 1]
  - [Summary point 2]
- **투자 참고사항:** [Single sentence takeaway]
"""
        try:
            summary = generate_with_retry(model, prompt)
            content_parts.append(f"#### **{stock.get('name')} ({stock.get('ticker')})**\n{summary}")
        except Exception as e:
            print(f"    - FAILED to generate social summary for {stock.get('ticker')}: {e}")
            content_parts.append(f"#### **{stock.get('name')} ({stock.get('ticker')})**\n- 소셜 동향 요약 생성에 실패했습니다.")
        time.sleep(3) # Rate limit for flash model

    return f"### 3. 📰 주요 종목 소셜 동향\n\n" + "\n".join(content_parts) + "\n"

def generate_final_summary(full_report: str, model: genai.GenerativeModel) -> str:
    print("--- Generating Section 4: Final Summary ---")
    prompt = f"""You are a top-tier financial analyst. Below is a full daily market report. Read it carefully and write a "핵심 요약 (3줄)" (Core Summary (3 lines)). This summary must be extremely concise and highlight only the most critical, must-know information for an investor today. Focus on the most noteworthy market events or stock movements.

**Full Report:**
{full_report[:20000]} 

**Your Output:**
[Your 3-line summary here]"""
    try:
        summary = generate_with_retry(model, prompt)
        return f"### 4. ✨ 핵심 요약 (3줄)\n\n{summary}\n"
    except Exception as e:
        print(f"    - FAILED to generate final summary: {e}")
        return "### 4. ✨ 핵심 요약 (3줄)\n\n- 최종 요약 생성에 실패했습니다.\n"

# --- Main Execution ---
def main():
    dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
    load_dotenv(dotenv_path=dotenv_path)

    parser = argparse.ArgumentParser(description="Generate a daily report markdown file from collected data.")
    parser.add_argument("--snapshot-date", default=datetime.now(KST).strftime('%Y-%m-%d'), help="Target date in YYYY-MM-DD format.")
    args = parser.parse_args()

    gemini_api_key = os.environ.get("GEMINI_API_KEY")
    if not gemini_api_key or "YOUR_GEMINI_API_KEY" in gemini_api_key:
        print("Error: GEMINI_API_KEY not set in .env file.")
        return

    genai.configure(api_key=gemini_api_key)
    pro_model = genai.GenerativeModel('gemini-2.5-pro')
    flash_model = genai.GenerativeModel('gemini-2.0-flash')

    # --- Data Loading ---
    print(f"--- Loading all data for {args.snapshot_date} ---")
    # base_path = f"/Users/baechangbin/codes/pythonWorkspace/AICapstoneDesign_2025_2/project_daily_asset_report/mvp/data/snapshots/{args.snapshot_date}"
    base_path = os.path.join(os.getcwd(), "mvp/data", "snapshots", args.snapshot_date)
    top_n_path = os.path.join(base_path, "topN.json")
    all_reports_path = os.path.join(base_path, "research", "all.jsonl")

    top_n_stocks = load_json_or_empty_list(top_n_path)
    all_reports = load_jsonl_or_empty_list(all_reports_path)
    
    name_map = {}
    # for f in ["/Users/baechangbin/codes/pythonWorkspace/AICapstoneDesign_2025_2/project_daily_asset_report/mvp/data/KOSPI200.csv", "/Users/baechangbin/codes/pythonWorkspace/AICapstoneDesign_2025_2/project_daily_asset_report/mvp/data/KOSDDAQ150.csv"]:
    for f in [os.path.join(os.getcwd(), "mvp/data", "KOSPI200.csv"), os.path.join(os.getcwd(), "mvp/data", "KOSDDAQ150.csv")]:
        try:
            with open(f, 'r', encoding='cp949') as csvfile:
                reader = csv.reader(csvfile)
                next(reader) # Skip header
                for row in reader:
                    if len(row) > 1:
                        name_map[row[0].strip()] = row[1].strip()
        except Exception as e:
            print(f"Warning: Could not read {f}: {e}")

    for stock in top_n_stocks:
        stock['name'] = name_map.get(stock['ticker'], stock['ticker'])

    if not top_n_stocks:
        print(f"Error: topN.json not found or empty at {top_n_path}")
        return

    # --- Section Generation ---
    md_part1 = generate_all_reports_summary(all_reports, pro_model)
    md_part2 = generate_top_n_list(top_n_stocks)
    md_part3 = generate_social_trends_section(top_n_stocks, base_path, flash_model)
    
    # --- Final Assembly ---
    final_report_parts = [
        f"# 📈 데일리 브리핑 ({args.snapshot_date})\n",
        md_part1,
        md_part2,
        md_part3
    ]
    
    # --- Final Summary Generation ---
    md_part4 = generate_final_summary("\n---\n".join(final_report_parts), pro_model)
    final_report_parts.append(md_part4)

    # --- File Output ---
    final_report_md = "\n---\n".join(final_report_parts)
    # output_filename = f"/Users/baechangbin/codes/pythonWorkspace/AICapstoneDesign_2025_2/project_daily_asset_report/mvp/reports/daily_report_{args.snapshot_date}.md"
    reports_dir = os.path.join(os.getcwd(), "reports")
    os.makedirs(reports_dir, exist_ok=True)
    output_filename = os.path.join(reports_dir, f"daily_report_{args.snapshot_date}.md")

    with open(output_filename, "w", encoding="utf-8") as f:
        f.write(final_report_md)

    print(f"\n--- ✅ Report generation complete! ---")
    print(f"Final report saved to: {output_filename}")

if __name__ == "__main__":
    main()