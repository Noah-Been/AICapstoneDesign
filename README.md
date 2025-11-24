# AI 기반 데일리 금융 리포트 자동 생성 프로젝트

## 1. 프로젝트 개요

본 프로젝트는 개인 투자자를 위한 지능형 금융 정보 솔루션의 MVP(Minimum Viable Product)입니다. 매일 증권사 리포트, 뉴스, 주가 데이터 등 방대한 금융 정보를 자동으로 수집 및 분석하고, AI를 활용하여 핵심 내용을 요약한 데일리 리포트를 마크다운 형식으로 생성합니다.

## 2. 주요 기능

- **데일리 데이터 자동 수집**:
  - **주가 데이터**: LS증권 API를 통해 KOSPI 200, KOSDAQ 150 종목의 일봉 데이터 수집
  - **증권사 리포트**: 주요 증권사(미래에셋, 한화, 유진, 삼성) 웹사이트에서 리포트 수집 (PDF OCR 및 웹 크롤링 활용)
  - **소셜 데이터**: 네이버 검색 API를 통해 당일의 주요 종목 관련 뉴스 및 블로그 수집

- **관심 종목 선정**:
  - 수집된 주가 데이터를 기반으로 60일 신고가 근접도, 20일 수익률 등 기술적 지표를 종합하여 매일 **Top 20 관심 종목** 선정

- **데일리 리포트 자동 생성**:
  - 수집 및 분석된 모든 정보를 종합하여, Google Gemini API(Pro/Flash)를 통해 4가지 섹션으로 구성된 데일리 리포트 자동 생성
    1. 증권사 리포트 전체 요약
    2. Top 20 종목 및 선정 신호
    3. 주요 종목 소셜 동향
    4. 시장 핵심 요약 (3줄)

- **전체 파이프라인 자동화**:
  - `run_daily_pipeline.sh` 쉘 스크립트를 통해 데이터 수집부터 리포트 생성까지의 전 과정을 단일 명령어로 실행

## 3. 기술 스택

- **언어**: Python 3
- **주요 라이브러리**: `httpx`, `beautifulsoup4`, `pdfminer.six`, `google-generativeai`, `tenacity`, `loguru`
- **외부 API**: LS증권 OpenAPI, 네이버 검색 API, Google Gemini API
- **기타 도구**: Tesseract OCR

## 4. 설치 및 설정 방법

1.  **저장소 복제**:
    ```bash
    git clone [본 저장소 URL]
    cd [프로젝트 디렉토리]
    ```

2.  **가상 환경 생성 및 활성화**:
    ```bash
    python -m venv .venv
    source .venv/bin/activate
    ```

3.  **필요 라이브러리 설치**:
    ```bash
    pip install -r requirements.txt
    ```

4.  **.env 파일 설정**:
    - `.env` 파일을 생성하고 아래와 같이 API 키를 입력합니다.
    ```
    # LS증권 API
    LS_APP_KEY="YOUR_LS_APP_KEY"
    LS_SECRET_KEY="YOUR_LS_SECRET_KEY"

    # 네이버 검색 API
    NAVER_CLIENT_ID="YOUR_NAVER_CLIENT_ID"
    NAVER_CLIENT_SECRET="YOUR_NAVER_CLIENT_SECRET"

    # Gemini API
    GEMINI_API_KEY="YOUR_GEMINI_API_KEY"
    ```

## 5. 사용 방법

- 특정 날짜의 리포트를 생성하려면 아래 명령어를 실행합니다.
  ```bash
  bash apps/run_daily_pipeline.sh YYYY-MM-DD
  ```

- 날짜를 지정하지 않으면 오늘 날짜로 리포트를 생성합니다.
  ```bash
  bash apps/run_daily_pipeline.sh
  ```

- 생성된 리포트는 `reports/` 디렉토리 안에 `daily_report_{date}.md` 파일로 저장됩니다.

## 6. 프로젝트 구조

```
.mvp/
├── apps/           # 핵심 소스 코드
├── data/           # 수집된 원본 데이터 (Git 무시)
├── reports/        # 생성된 데일리 리포트 (Git 무시)
├── .venv/          # 파이썬 가상 환경 (Git 무시)
├── .env            # API 키 등 비밀 정보 (Git 무시)
├── .gitignore      # Git 무시 파일 목록
├── requirements.txt  # 필요 라이브러리
└── README.md       # 프로젝트 설명 파일
```
