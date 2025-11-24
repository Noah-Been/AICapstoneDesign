# Apps

이 디렉토리는 데일리 리포트 생성을 위한 핵심 애플리케이션 소스 코드를 포함하고 있습니다.

각 스크립트의 역할은 다음과 같습니다:

- **데이터 수집**: `batch_prices.py`, `run_*_pipeline.py`
- **데이터 분석**: `signals.py`
- **소셜 데이터 크롤링**: `news_naver.py`, `blog_naver.py`
- **리포트 생성**: `generate_report.py`
- **전체 실행**: `run_daily_pipeline.sh`
