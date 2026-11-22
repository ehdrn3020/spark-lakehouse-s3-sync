"""원본 Iceberg 테이블을 읽어 S3 의 Iceberg 테이블(Hadoop 카탈로그)로 새로 쓰는 일배치 파이프라인.

원본(source 카탈로그: Hive metastore)은 읽기만 한다. metadata·data 파일과 metastore 에 쓰지 않는다.
대상(target 카탈로그: Hadoop 카탈로그, warehouse = s3://<bucket>/<s3_prefix>)에는
data·metadata·version-hint.text 를 새로 쓴다. metadata 안의 경로는 처음부터 S3 를 가리킨다.

테이블마다 아래 순서로 처리한다.

1. 원본 조회: 현재 스냅샷 id, 파티션 스펙 (DESCRIBE TABLE EXTENDED)
2. 원본을 현재 스냅샷으로 고정해 읽고 건수 확인
   (date_column 이 있으면 base_dt 하루치, 없으면 전체)
   ── dry-run 이면 여기까지 (S3 에는 쓰지 않음) ──
3. 대상 테이블이 없으면 원본과 같은 스키마·파티션 스펙으로 생성
4. 덮어쓰기: date_column 이 있으면 그 날짜만, 없으면 테이블 전체
5. 검증·정리: 원본 건수 == 대상 건수, 최신 metadata 경로 로그, 오래된 스냅샷 만료 (common)
"""

import logging
import re
from dataclasses import dataclass
from datetime import date
from typing import List, Optional, Tuple

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


@dataclass(frozen=True)
class IcebergToS3Options:
    source_catalog: str  # 원본 카탈로그 (읽기만)
    target_catalog: str  # 대상 S3 Hadoop 카탈로그
    tables: Tuple[TableSpec, ...]
    base_dt: date  # date_column 이 있는 테이블의 기준일
    dry_run: bool = True  # True 이면 원본 조회·건수 확인까지만


@dataclass(frozen=True)
class SourceInfo:
    snapshot_id: Optional[str]  # 원본 현재 스냅샷 (비어 있는 테이블이면 None)
    partitioning: List[str]  # 예: ["base_dt"], ["days(event_time)"]


def run(spark, options: IcebergToS3Options) -> List[TableResult]:
    return run_tables("iceberg_to_s3", options.tables, lambda spec: _copy_table(spark, options, spec))


def _copy_table(spark, options: IcebergToS3Options, spec: TableSpec) -> TableResult:
    namespace, _ = spec.name.split(".")
    source = f"{options.source_catalog}.{spec.name}"
    target = f"{options.target_catalog}.{spec.name}"

    # 1. 원본 조회 (읽기만)
    info = _describe_source(spark, source)
    if spec.date_column and spec.date_column not in info.partitioning:
        # 날짜 단위 덮어쓰기는 그 컬럼이 identity 파티션일 때만 파일 단위로 안전하게 교체된다
        raise ValueError(f"date_column '{spec.date_column}' 이 원본 파티션 스펙에 없습니다: {info.partitioning}")

    # 2. 원본 고정 읽기 (count 와 write 가 같은 스냅샷을 읽도록)
    condition = _condition(spec, options.base_dt)
    source_df = spark.sql(_select_sql(source, info.snapshot_id, condition))
    source_rows = source_df.count()
    target_exists = table_exists(spark, target)
    scope = condition or "full"
    logger.info(
        "[%s] source=%s snapshot=%s partitioning=%s scope=%s rows=%d target=%s exists=%s",
        spec.name, source, info.snapshot_id, info.partitioning, scope, source_rows, target, target_exists,
    )
    if options.dry_run:
        return TableResult(spec.name, "dry_run", f"rows={source_rows} scope={scope} target_exists={target_exists}")

    # 3. 대상 생성 (S3 에만 생김)
    if not target_exists:
        _create_target(spark, options.target_catalog, namespace, target, source, info.partitioning)
    else:
        ensure_properties(spark, target)

    # 4. 덮어쓰기
    from pyspark.sql import functions as F

    source_df.writeTo(target).overwrite(F.expr(condition) if condition else F.lit(True))

    # 5. 검증·정리
    return verify_and_cleanup(spark, options.target_catalog, spec.name, condition, source_rows)


def _describe_source(spark, source: str) -> SourceInfo:
    rows = spark.sql(f"DESCRIBE TABLE EXTENDED {source}").collect()
    return SourceInfo(snapshot_id=_parse_snapshot_id(rows), partitioning=_parse_partitioning(rows))


def _parse_partitioning(rows) -> List[str]:
    """DESCRIBE 결과의 파티션 스펙 (예: ["base_dt"], ["base_dt", "days(event_time)"]).

    Spark 는 두 형식으로 출력한다.
      - identity 파티션만 있을 때: '# Partition Information' → '# col_name' 헤더 → 컬럼 행
      - transform 이 있을 때:      '# Partitioning' → 'Part 0', 'Part 1' 행
    """

    partitioning, section = [], None
    for row in rows:
        name = (row["col_name"] or "").strip()
        if name in ("# Partition Information", "# Partitioning"):
            section = name
        elif section is None or name == "# col_name":
            continue
        elif not name or name.startswith("#"):
            break
        elif section == "# Partitioning" and name.startswith("Part "):
            partitioning.append(row["data_type"].strip())
        elif section == "# Partition Information":
            partitioning.append(name)
    return partitioning


def _parse_snapshot_id(rows) -> Optional[str]:
    """Table Properties 의 current-snapshot-id (스냅샷이 없으면 None)."""

    for row in rows:
        if (row["col_name"] or "").strip() == "Table Properties":
            match = re.search(r"current-snapshot-id=(\d+)", row["data_type"] or "")
            return match.group(1) if match else None
    return None


def _condition(spec: TableSpec, base_dt: date) -> Optional[str]:
    if not spec.date_column:
        return None
    return f"{spec.date_column} = DATE '{base_dt.isoformat()}'"


def _select_sql(source: str, snapshot_id: Optional[str], condition: Optional[str]) -> str:
    sql = f"SELECT * FROM {source}"
    if snapshot_id:
        sql += f" VERSION AS OF {snapshot_id}"
    if condition:
        sql += f" WHERE {condition}"
    return sql


def _create_target(
    spark, target_catalog: str, namespace: str, target: str, source: str, partitioning: List[str]
) -> None:
    """원본과 같은 스키마·파티션 스펙의 빈 테이블을 S3 에 생성."""

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {target_catalog}.{namespace}")
    partitioned_by = f" PARTITIONED BY ({', '.join(partitioning)})" if partitioning else ""
    properties = ", ".join(f"'{key}' = '{value}'" for key, value in TARGET_TABLE_PROPERTIES.items())
    spark.sql(
        f"CREATE TABLE {target} USING iceberg{partitioned_by} "
        f"TBLPROPERTIES ({properties}) "
        f"AS SELECT * FROM {source} WHERE 1 = 0"
    )
    logger.info("created %s%s", target, partitioned_by)
