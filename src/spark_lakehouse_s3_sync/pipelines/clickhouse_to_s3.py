"""ClickHouse 테이블을 JDBC 로 읽어 S3 의 Iceberg 테이블(Hadoop 카탈로그)로 쓰는 일배치 파이프라인.

원본(ClickHouse)은 SELECT 만 한다. 대상 테이블 이름은 <db>.<table> 을 그대로 쓴다 (예: lake.analytics.page_view_log).

1. 원본 읽기 → persist (ClickHouse 는 스냅샷이 없으므로 건수 확인과 쓰기가 같은 데이터를 쓰게 함)
   - date_column 있음(팩트): base_dt 하루치 + base_dt 컬럼 추가
   - date_column 없음(마스터성): 전체
   ── dry-run 이면 여기까지 ──
2. 대상 테이블이 없으면 읽은 스키마로 생성 (팩트는 base_dt 파티션)
3. 덮어쓰기: 팩트는 base_dt 하루치, 마스터성은 전체
4. 검증·정리 (common)
"""

import logging
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import List, Tuple

from spark_lakehouse_s3_sync.pipelines.common import (
    TARGET_TABLE_PROPERTIES,
    TableResult,
    ensure_properties,
    run_tables,
    table_exists,
    verify_and_cleanup,
)
from spark_lakehouse_s3_sync.settings import TableSpec

logger = logging.getLogger(__name__)

PARTITION_COLUMN = "base_dt"


@dataclass(frozen=True)
class ClickHouseToS3Options:
    jdbc_url: str
    user: str
    password: str = field(repr=False)
    target_catalog: str
    tables: Tuple[TableSpec, ...]
    base_dt: date
    dry_run: bool = True


def run(spark, options: ClickHouseToS3Options) -> List[TableResult]:
    return run_tables("clickhouse_to_s3", options.tables, lambda spec: _copy_table(spark, options, spec))


def _copy_table(spark, options: ClickHouseToS3Options, spec: TableSpec) -> TableResult:
    from pyspark import StorageLevel
    from pyspark.sql import functions as F

    namespace, _ = spec.name.split(".")
    target = f"{options.target_catalog}.{spec.name}"
    query, condition = _select_sql(spec, options.base_dt)
    scope = condition or "full"

    # 1. 원본 읽기 → persist
    source_df = _read_jdbc(spark, options, query).persist(StorageLevel.MEMORY_AND_DISK)
    try:
        source_rows = source_df.count()
        target_exists = table_exists(spark, target)
        logger.info("[%s] query=%s rows=%d target=%s exists=%s", spec.name, query, source_rows, target, target_exists)
        if options.dry_run:
            return TableResult(spec.name, "dry_run", f"rows={source_rows} scope={scope} target_exists={target_exists}")

        # 2. 대상 생성
        if not target_exists:
            spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {options.target_catalog}.{namespace}")
            writer = source_df.limit(0).writeTo(target).using("iceberg")
            if spec.date_column:
                writer = writer.partitionedBy(F.col(PARTITION_COLUMN))
            for key, value in TARGET_TABLE_PROPERTIES.items():
                writer = writer.tableProperty(key, value)
            writer.create()
            logger.info("created %s", target)
        else:
            ensure_properties(spark, target)

        # 3. 덮어쓰기
        source_df.writeTo(target).overwrite(F.expr(condition) if condition else F.lit(True))
    finally:
        source_df.unpersist()

    # 4. 검증·정리
    return verify_and_cleanup(spark, options.target_catalog, spec.name, condition, source_rows)


def _select_sql(spec: TableSpec, base_dt: date):
    """(ClickHouse 조회 SQL, 대상 덮어쓰기 조건). date_column 은 Date/DateTime 타입이어야 한다."""

    if not spec.date_column:
        return f"SELECT * FROM {spec.name}", None
    start = f"{base_dt.isoformat()} 00:00:00"
    end = f"{(base_dt + timedelta(days=1)).isoformat()} 00:00:00"
    column = spec.date_column
    query = (
        f"SELECT *, toDate({column}) AS {PARTITION_COLUMN} FROM {spec.name} "
        f"WHERE {column} >= toDateTime('{start}') AND {column} < toDateTime('{end}')"
    )
    return query, f"{PARTITION_COLUMN} = DATE '{base_dt.isoformat()}'"


def _read_jdbc(spark, options: ClickHouseToS3Options, query: str):
    return (
        spark.read.format("jdbc")
        .option("driver", "com.clickhouse.jdbc.Driver")
        .option("url", options.jdbc_url)
        .option("user", options.user)
        .option("password", options.password)
        .option("query", query)
        .load()
    )
