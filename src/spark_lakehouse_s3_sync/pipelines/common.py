"""iceberg_to_s3 · clickhouse_to_s3 가 같이 쓰는 대상(S3 Hadoop 카탈로그) 처리."""

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Callable, Iterable, List, Optional

from spark_lakehouse_s3_sync.settings import TableSpec

logger = logging.getLogger(__name__)

# 대상 테이블 스냅샷 보존 기간. 이보다 오래된 스냅샷과 그것만 쓰던 파일은 S3 에서 삭제된다.
SNAPSHOT_RETENTION_DAYS = 30

# 대상 테이블에 항상 적용할 속성: 커밋 때 오래된 vN.metadata.json 자동 삭제 (최근 100개 유지)
TARGET_TABLE_PROPERTIES = {
    "format-version": "2",
    "write.metadata.delete-after-commit.enabled": "true",
    "write.metadata.previous-versions-max": "100",
}


@dataclass(frozen=True)
class TableResult:
    table: str
    status: str  # copied | dry_run | failed
    message: str = ""


def run_tables(
    pipeline: str,
    tables: Iterable[TableSpec],
    copy_table: Callable[[TableSpec], TableResult],
) -> List[TableResult]:
    """테이블을 순서대로 처리. 한 테이블이 실패해도 나머지는 계속하고, 끝에 실패가 있으면 예외."""

    results = []
    for spec in tables:
        try:
            result = copy_table(spec)
        except Exception as error:  # noqa: BLE001 - 테이블 단위 격리
            logger.exception("[%s] failed", spec.name)
            result = TableResult(spec.name, "failed", str(error))
        logger.info("[%s] %s %s", result.table, result.status, result.message)
        results.append(result)

    failed = [result.table for result in results if result.status == "failed"]
    if failed:
        raise RuntimeError(f"{pipeline} failed tables: {failed}")
    return results


def table_exists(spark, table: str) -> bool:
    from pyspark.sql.utils import AnalysisException

    try:
        spark.sql(f"DESCRIBE TABLE {table}").collect()
        return True
    except AnalysisException:
        return False


def ensure_properties(spark, target: str) -> None:
    """기존 대상 테이블 속성이 TARGET_TABLE_PROPERTIES 와 다르면 맞춘다 (같으면 커밋하지 않음)."""

    current = {row["key"]: row["value"] for row in spark.sql(f"SHOW TBLPROPERTIES {target}").collect()}
    missing = {
        key: value
        for key, value in TARGET_TABLE_PROPERTIES.items()
        if key != "format-version" and current.get(key) != value
    }
    if missing:
        assignments = ", ".join(f"'{key}' = '{value}'" for key, value in missing.items())
        spark.sql(f"ALTER TABLE {target} SET TBLPROPERTIES ({assignments})")
        logger.info("set properties %s %s", target, missing)


def count_rows(spark, table: str, condition: Optional[str]) -> int:
    sql = f"SELECT COUNT(*) FROM {table}" + (f" WHERE {condition}" if condition else "")
    return spark.sql(sql).collect()[0][0]


def verify_and_cleanup(
    spark, target_catalog: str, table: str, condition: Optional[str], source_rows: int
) -> TableResult:
    """쓰기 후 공통 처리: 건수 검증 → 최신 metadata 경로 조회 → 오래된 스냅샷 정리."""

    target = f"{target_catalog}.{table}"
    scope = condition or "full"
    target_rows = count_rows(spark, target, condition)
    if target_rows != source_rows:
        raise RuntimeError(f"row count mismatch: source={source_rows} target={target_rows} ({scope})")
    metadata_file = latest_metadata_file(spark, target)
    expired = expire_snapshots(spark, target_catalog, table)
    return TableResult(
        table, "copied", f"rows={target_rows} scope={scope} metadata={metadata_file} expired={expired}"
    )


def expire_snapshots(spark, target_catalog: str, table: str) -> str:
    """SNAPSHOT_RETENTION_DAYS 보다 오래된 스냅샷 만료 + 그 스냅샷만 쓰던 파일 삭제.

    원본이 아니라 대상(S3) 테이블에만 실행한다. 실패하면 경고만 남긴다.
    """

    older_than = (datetime.now() - timedelta(days=SNAPSHOT_RETENTION_DAYS)).strftime("%Y-%m-%d %H:%M:%S")
    try:
        row = spark.sql(
            f"CALL {target_catalog}.system.expire_snapshots("
            f"table => '{table}', older_than => TIMESTAMP '{older_than}', retain_last => 1)"
        ).collect()[0]
        return (
            f"data_files={row['deleted_data_files_count']} "
            f"manifests={row['deleted_manifest_files_count']} "
            f"manifest_lists={row['deleted_manifest_lists_count']}"
        )
    except Exception as error:  # noqa: BLE001 - 정리 실패가 이관 실패가 되지 않도록
        logger.warning("[%s] expire_snapshots failed: %s", table, error)
        return "failed"


def latest_metadata_file(spark, target: str) -> str:
    """최신 metadata.json 경로 (외부 엔진에서 테이블을 등록·갱신할 때 사용)."""

    rows = spark.sql(
        f"SELECT file FROM {target}.metadata_log_entries ORDER BY timestamp DESC LIMIT 1"
    ).collect()
    return rows[0][0] if rows else ""
