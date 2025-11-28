# 데이터 처리 파트 (AI 기반 데일리 금융 리포트 자동 생성 프로젝트)

## 1. run_daily_crawling.sh
- news_naver.py, append_stock_prices.py, append_financial_data.py를 실행합니다
- 모든 python script의 결과는 data/에 저장됩니다
- SNAPSHOT DATE는 오늘 날짜가 default로 설정됩니다

## 2. news_naver.py
- 네이버 뉴스를 수집합니다
- 수집된 뉴스는 data/news_naver/{SNAPSHOT_DATE}/{ticker}.csv에 저장됩니다
- 관련 뉴스 숫자가 많을 경우 cutoff를 적용합니다

## 3. append_stock_prices.py
- 주가 데이터를 저장합니다
- 수집된 주가 데이터는 data/price_data/{ticker}.csv에 저장됩니다
- 주가 데이터가 존재하는 경우, 최근 날짜의 데이터를 추가합니다
- 주가 데이터가 존재하지 않는 경우, 최근 60일의 데이터를 전부 다운로드합니다

## 4. append_financial_data.py
- 재무 데이터를 저장합니다
- 수집된 재무 데이터는 data/financial_data/{ticker}/{최근 재무 결산 년월}.csv에 저장됩니다

## 5. 코드 실행을 위해 필요한 파일
- tickers.txt
- KOSPI_KOSDAQ.csv

## 6. 사용 방법

- 특정 날짜의 리포트를 생성하려면 아래 명령어를 실행합니다.
  ```bash
  bash modifications/run_daily_crawling.sh YYYY-MM-DD
  ```

- 날짜를 지정하지 않으면 오늘 날짜로 리포트를 생성합니다.
  ```bash
  bash modifications/run_daily_crawling.sh
  ```