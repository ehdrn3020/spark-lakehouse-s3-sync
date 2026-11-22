"""실행 진입점. bin/run.sh 가 spark-submit 으로 실행한다.

  --base-dt YYYY-MM-DD                    일배치: 차원 전체 + 팩트 하루치
  --start-dt / --end-dt YYYY-MM-DD        백필: 팩트(date_column 있는 테이블)만 날짜별
  --tables ns.table ...                   config 의 tables 중 일부만 (생략하면 전체)
  --dry-run true|false                    true(기본): 원본 조회·건수 확인까지만, S3 에 쓰지 않음
"""

import argparse
import logging
from datetime import date, timedelta
from pathlib import Path
from typing import List, Optional

from spark_lakehouse_s3_sync.pipelines import clickhouse_to_s3, iceberg_to_s3
from spark_lakehouse_s3_sync.settings import AppConfig, load_config

DEFAULT_CONFIG = Path(__file__).resolve().parents[2] / "configs" / "config.yaml"
logger = logging.getLogger(__name__)


def main(argv: Optional[List[str]] = None) -> None:
    args = _parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    config = load_config(args.config)

    backfill = args.base_dt is None
    base_dates = _date_range(args.start_dt, args.end_dt) if backfill else [args.base_dt]
    iceberg_tables = _select(config.iceberg.tables, args.tables, backfill)
    clickhouse_tables = _select(config.clickhouse.tables if config.clickhouse else [], args.tables, backfill)
    if not iceberg_tables and not clickhouse_tables:
        raise ValueError("대상 테이블이 없습니다. config 의 tables 또는 --tables 를 확인하세요.")

    spark = _build_spark(config)
    try:
        failed_dates = []
        for base_dt in base_dates:
            logger.info("start base_dt=%s dry_run=%s", base_dt, args.dry_run)
            jobs = []
            if iceberg_tables:
                jobs.append(lambda: iceberg_to_s3.run(spark, iceberg_to_s3.IcebergToS3Options(
                    source_catalog=config.iceberg.catalog,
                    target_catalog=config.iceberg.target_catalog,
                    tables=iceberg_tables, base_dt=base_dt, dry_run=args.dry_run,
                )))
            if clickhouse_tables:
                jobs.append(lambda: clickhouse_to_s3.run(spark, clickhouse_to_s3.ClickHouseToS3Options(
                    jdbc_url=config.clickhouse.url, user=config.clickhouse.user, password=config.clickhouse.password,
                    target_catalog=config.iceberg.target_catalog,
                    tables=clickhouse_tables, base_dt=base_dt, dry_run=args.dry_run,
                )))
            for job in jobs:  # 한 원천이 실패해도 다른 원천·다음 날짜는 계속
                try:
                    job()
                except RuntimeError as error:
                    logger.error("base_dt=%s %s", base_dt, error)
                    failed_dates.append(base_dt.isoformat())
        if failed_dates:
            raise RuntimeError(f"failed base_dt: {sorted(set(failed_dates))}")
    finally:
        spark.stop()


def _parse_args(argv: Optional[List[str]]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Iceberg / ClickHouse → S3 Iceberg 일배치")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG))
    parser.add_argument("--tables", nargs="+")
    parser.add_argument("--base-dt", type=date.fromisoformat)
    parser.add_argument("--start-dt", type=date.fromisoformat)
    parser.add_argument("--end-dt", type=date.fromisoformat)
    parser.add_argument("--dry-run", choices=["true", "false"], default="true")
    args = parser.parse_args(argv)
    args.dry_run = args.dry_run == "true"

    has_range = args.start_dt is not None or args.end_dt is not None
    if (args.base_dt is None) == (not has_range):
        parser.error("--base-dt 또는 --start-dt/--end-dt 중 하나만 지정하세요.")
    if has_range and (args.start_dt is None or args.end_dt is None or args.start_dt > args.end_dt):
        parser.error("--start-dt 와 --end-dt 를 함께, start <= end 로 지정하세요.")
    return args


def _date_range(start: date, end: date) -> List[date]:
    return [start + timedelta(days=offset) for offset in range((end - start).days + 1)]


def _select(specs, names: Optional[List[str]], backfill: bool) -> tuple:
    """--tables 로 고르고, 백필이면 date_column 없는(전체 덮어쓰기) 테이블은 뺀다."""

    selected = [spec for spec in specs if not names or spec.name in names]
    if backfill:
        selected = [spec for spec in selected if spec.date_column]
    return tuple(selected)


def _build_spark(config: AppConfig):
    """원본(Hive) · 대상(S3 Hadoop) 카탈로그와 S3A 접속을 설정한 SparkSession."""

    from pyspark.sql import SparkSession

    source, target = config.iceberg.catalog, config.iceberg.target_catalog
    if source == target:
        raise ValueError(f"target_catalog({target}) 가 원본 catalog 와 같습니다. 원본 보호를 위해 중단합니다.")
    warehouse = f"s3://{config.aws.s3_bucket}/{config.iceberg.s3_prefix}"

    spark = (
        SparkSession.builder.appName("spark-lakehouse-s3-sync")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        # 원본: Hive metastore Iceberg 카탈로그 (읽기만)
        .config(f"spark.sql.catalog.{source}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{source}.type", "hive")
        .config(f"spark.sql.catalog.{source}.uri", config.iceberg.metastore_uri)
        # 대상: S3 Hadoop 카탈로그
        .config(f"spark.sql.catalog.{target}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{target}.type", "hadoop")
        .config(f"spark.sql.catalog.{target}.warehouse", warehouse)
        # s3:// 도 S3A 로 처리 → metadata 에는 s3:// 경로로 기록
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", config.iceberg.s3_endpoint)
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.access.key", config.aws.access_key_id)
        .config("spark.hadoop.fs.s3a.secret.key", config.aws.secret_access_key)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info("source=%s(hive) target=%s(hadoop) warehouse=%s", source, target, warehouse)
    return spark


if __name__ == "__main__":
    main()
