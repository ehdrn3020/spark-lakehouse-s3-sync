"""configs/config.yaml 을 읽어 설정 객체로 만든다. 모든 설정은 이 파일 한 곳에서 관리한다."""

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import yaml

_NAME = re.compile(r"^[A-Za-z0-9_]+$")


@dataclass(frozen=True)
class AwsConfig:
    """AWS 접속과 대상 S3 버킷."""

    access_key_id: str = field(repr=False)
    secret_access_key: str = field(repr=False)
    region_name: str
    s3_bucket: str


@dataclass(frozen=True)
class SparkConfig:
    """spark-submit 실행 옵션 (bin/run.sh 가 읽어서 사용)."""

    master: str
    driver_memory: str
    packages: List[str]


@dataclass(frozen=True)
class TableSpec:
    """이관 대상 테이블 1개."""

    name: str  # "<namespace>.<table>" (예: shop.fct_order)
    date_column: Optional[str] = None  # 있으면 base_dt 하루치만 덮어쓰기, 없으면 전체 덮어쓰기


@dataclass(frozen=True)
class IcebergConfig:
    """Iceberg 원본(Hive metastore) → S3 Iceberg(Hadoop 카탈로그) 설정."""

    catalog: str  # 원본 카탈로그 이름 (읽기만)
    metastore_uri: str  # 원본 Hive metastore thrift 주소
    target_catalog: str  # 대상 S3 Hadoop 카탈로그 이름
    s3_prefix: str  # warehouse = s3://<bucket>/<s3_prefix>
    s3_endpoint: str  # fs.s3a.endpoint
    tables: List[TableSpec]


@dataclass(frozen=True)
class ClickHouseConfig:
    """ClickHouse 원본(JDBC) 설정."""

    url: str  # jdbc:clickhouse://<host>:8123/default
    user: str
    password: str = field(repr=False)
    tables: List[TableSpec]  # date_column 있으면 하루치, 없으면 전체 덮어쓰기


@dataclass(frozen=True)
class AppConfig:
    aws: AwsConfig
    spark: SparkConfig
    iceberg: IcebergConfig
    clickhouse: Optional[ClickHouseConfig] = None  # 섹션이 없으면 ClickHouse 이관 안 함


def load_config(config_path: Union[str, Path]) -> AppConfig:
    path = Path(config_path)
    with path.open("r", encoding="utf-8") as config_file:
        raw = yaml.safe_load(config_file)
    if not isinstance(raw, dict):
        raise ValueError(f"Invalid config file: {path}")

    return AppConfig(
        aws=_load_aws(raw),
        spark=_load_spark(raw),
        iceberg=_load_iceberg(raw),
        clickhouse=_load_clickhouse(raw),
    )


def _load_aws(raw: Dict[str, Any]) -> AwsConfig:
    aws = _require_keys(raw, "aws", ["access_key_id", "secret_access_key", "region_name", "s3_bucket"])
    return AwsConfig(
        access_key_id=str(aws["access_key_id"]),
        secret_access_key=str(aws["secret_access_key"]),
        region_name=str(aws["region_name"]),
        s3_bucket=str(aws["s3_bucket"]),
    )


def _load_spark(raw: Dict[str, Any]) -> SparkConfig:
    spark = _require_keys(raw, "spark", ["master", "driver_memory", "packages"])
    return SparkConfig(
        master=str(spark["master"]),
        driver_memory=str(spark["driver_memory"]),
        packages=[str(package) for package in spark["packages"]],
    )


def _load_iceberg(raw: Dict[str, Any]) -> IcebergConfig:
    # tables 는 키만 있으면 됨 (항목을 모두 주석 처리해 비어 있는 상태 허용)
    iceberg = _require_keys(
        raw,
        "iceberg",
        ["catalog", "metastore_uri", "target_catalog", "s3_prefix", "s3_endpoint", "tables"],
        nullable=["tables"],
    )
    return IcebergConfig(
        catalog=str(iceberg["catalog"]),
        metastore_uri=str(iceberg["metastore_uri"]),
        target_catalog=str(iceberg["target_catalog"]),
        s3_prefix=str(iceberg["s3_prefix"]).strip("/"),
        s3_endpoint=str(iceberg["s3_endpoint"]),
        tables=[parse_table_spec(entry) for entry in iceberg["tables"] or []],
    )


def _load_clickhouse(raw: Dict[str, Any]) -> Optional[ClickHouseConfig]:
    if "clickhouse" not in raw:
        return None
    clickhouse = _require_keys(raw, "clickhouse", ["url", "user", "password", "tables"], nullable=["tables"])
    return ClickHouseConfig(
        url=str(clickhouse["url"]),
        user=str(clickhouse["user"]),
        password=str(clickhouse["password"]),
        tables=[parse_table_spec(entry) for entry in clickhouse["tables"] or []],
    )


def parse_table_spec(entry: Any) -> TableSpec:
    """tables 항목: "ns.table" 문자열 또는 {name: ns.table, date_column: base_dt}."""

    if isinstance(entry, dict):
        name, date_column = entry.get("name"), entry.get("date_column")
    else:
        name, date_column = entry, None

    parts = str(name or "").strip().split(".")
    if len(parts) != 2 or not all(_NAME.match(part) for part in parts):
        raise ValueError(f"tables 항목은 '<namespace>.<table>' 이어야 합니다: {entry!r}")
    if date_column is not None and not _NAME.match(str(date_column)):
        raise ValueError(f"date_column 이 올바르지 않습니다: {entry!r}")
    return TableSpec(".".join(parts), str(date_column) if date_column is not None else None)


def _require_keys(
    raw: Dict[str, Any],
    section: str,
    keys: List[str],
    nullable: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """section 아래 keys 가 모두 있는지 확인. nullable 키는 값이 비어 있어도 키만 있으면 통과."""

    values = raw.get(section) or {}
    nullable = nullable or []
    missing = [key for key in keys if key not in values or (values[key] is None and key not in nullable)]
    if missing:
        raise ValueError(f"config.yaml 의 {section} 섹션에 필수 항목이 없습니다: {missing}")
    return values
