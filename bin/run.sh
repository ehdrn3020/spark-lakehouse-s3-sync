#!/bin/bash
# spark-submit 실행 (local 모드). 모든 설정은 configs/config.yaml 에서 읽는다.
#   bin/run.sh --base-dt 2026-11-20                                  # 일배치 dry-run (기본)
#   bin/run.sh --base-dt 2026-11-20 --dry-run false                  # 일배치 실제 S3 쓰기
#   bin/run.sh --start-dt 2026-11-01 --end-dt 2026-11-20 --dry-run false   # 백필 (팩트만)
#   bin/run.sh --base-dt 2026-11-20 --tables shop.fct_order          # 일부 테이블만
#   cron: bin/run.sh --base-dt $(date -d yesterday +%F) --dry-run false
set -euo pipefail

APP_HOME="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONFIG="$APP_HOME/configs/config.yaml"
PYTHON="${PYTHON:-python3}"
SPARK_SUBMIT="${SPARK_HOME:+$SPARK_HOME/bin/}spark-submit"

export PYTHONPATH="$APP_HOME/src"
export PYSPARK_PYTHON="$PYTHON"

# 같은 배치가 겹쳐 돌지 않게 (Hadoop 카탈로그는 동시 커밋에 안전하지 않음)
exec 9>/tmp/spark-lakehouse-s3-sync.lock
flock -n 9 || { echo "이미 실행 중입니다" >&2; exit 1; }

read -r MASTER DRIVER_MEMORY PACKAGES < <("$PYTHON" - "$CONFIG" <<'PY'
import sys, yaml
spark = yaml.safe_load(open(sys.argv[1], encoding="utf-8"))["spark"]
print(spark["master"], spark["driver_memory"], ",".join(spark["packages"]))
PY
)

exec "$SPARK_SUBMIT" \
  --master "$MASTER" \
  --driver-memory "$DRIVER_MEMORY" \
  --packages "$PACKAGES" \
  "$APP_HOME/src/spark_lakehouse_s3_sync/main.py" \
  --config "$CONFIG" "$@"
