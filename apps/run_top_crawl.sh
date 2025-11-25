#!/usr/bin/env bash
set -euo pipefail

SNAPSHOT_DATE=${SNAPSHOT_DATE:-}
if [[ -z "$SNAPSHOT_DATE" ]]; then
  SNAPSHOT_DATE=$(TZ=Asia/Seoul date +%F)
fi

TOP="$(pwd)/mvp/data/snapshots/${SNAPSHOT_DATE}/topN.json"
DATA_DIR="$(pwd)/mvp/data"
NEWS_OUT="$(pwd)/mvp/data/snapshots/${SNAPSHOT_DATE}/news_strict"
BLOG_OUT="$(pwd)/mvp/data/snapshots/${SNAPSHOT_DATE}/blogs_strict"

PY="python"

# load secrets non-interactively
if [[ -f "$HOME/.secrets/naver_env.sh" ]]; then
  # shellcheck disable=SC1090
  source "$HOME/.secrets/naver_env.sh"
fi

attempts=${1:-12}  # ~12 attempts ~ with sleeps ~ 20-30min window
sleep_s=${2:-60}

echo "[run_top_crawl] SNAPSHOT_DATE=$SNAPSHOT_DATE attempts=$attempts sleep=$sleep_s"

# Pre-clean: remove any leftover zero-byte JSONL files for this date
date_path_init=$(echo "$NEWS_OUT" | sed "s/{date}/$SNAPSHOT_DATE/g")
date_path_b_init=$(echo "$BLOG_OUT" | sed "s/{date}/$SNAPSHOT_DATE/g")
mkdir -p "$date_path_init" "$date_path_b_init"
find "$date_path_init" -type f -name '*.jsonl' -size 0 -delete 2>/dev/null || true
find "$date_path_b_init" -type f -name '*.jsonl' -size 0 -delete 2>/dev/null || true

for round in $(seq 1 $attempts); do
  echo "[run_top_crawl] Round $round/$attempts: News with body"
  set +e
  $PY apps/news_naver.py \
    --snapshot-date "$SNAPSHOT_DATE" \
    --data-dir "$DATA_DIR" \
    --outdir "$NEWS_OUT" \
    --days 7 --per-query 80 --topk 15 --omit-snippet --with-body --sleep-sec 0.3
  rc1=$?
  echo "[run_top_crawl] News rc=$rc1"

  echo "[run_top_crawl] Round $round/$attempts: Blogs with body"
  $PY apps/blog_naver.py \
    --snapshot-date "$SNAPSHOT_DATE" \
    --data-dir "$DATA_DIR" \
    --outdir "$BLOG_OUT" \
    --days 7 --per-query 80 --topk 15 --omit-snippet --with-body --sleep-sec 0.3
  rc2=$?
  set -e

  echo "[run_top_crawl] round $round done rc1=$rc1 rc2=$rc2"

  # Check completion: require that every topN ticker has non-empty file in both dirs
  date_path=$(echo "$NEWS_OUT" | sed "s/{date}/$SNAPSHOT_DATE/g")
  date_path_b=$(echo "$BLOG_OUT" | sed "s/{date}/$SNAPSHOT_DATE/g")
  # Cleanup: remove empty jsonl files from this round
  find "$date_path" -type f -name '*.jsonl' -size 0 -delete 2>/dev/null || true
  find "$date_path_b" -type f -name '*.jsonl' -size 0 -delete 2>/dev/null || true
  ok_news=$(find "$date_path" -type f -name '*.jsonl' -size +10c 2>/dev/null | wc -l)
  ok_blog=$(find "$date_path_b" -type f -name '*.jsonl' -size +10c 2>/dev/null | wc -l)
  echo "[run_top_crawl] have news=$ok_news blog=$ok_blog (want ~10 each)"
  if [[ "$ok_news" -ge 8 && "$ok_blog" -ge 8 ]]; then
    echo "[run_top_crawl] sufficient coverage achieved; exiting"
    exit 0
  fi

done

echo "[run_top_crawl] completed attempts without full coverage; check logs and network"
exit 1
