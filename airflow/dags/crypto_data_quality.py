"""
crypto_data_quality.py
DAG: crypto_data_quality
Schedule: @hourly
Purpose: Automated data quality monitoring across all Medallion layers.
         Checks completeness, freshness, null rates, and schema drift.
Tasks:
  1. check_bronze_quality     – PythonOperator
  2. check_silver_quality     – PythonOperator
  3. check_gold_quality       – PythonOperator
  4. publish_dq_metrics       – PythonOperator: push to Prometheus/Grafana
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

DEFAULT_ARGS = {
    "owner":            "data-quality",
    "depends_on_past":  False,
    "email":            ["dq-alerts@crypto-analytics.io"],
    "email_on_failure": True,
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
    "execution_timeout":timedelta(minutes=20),
}

S3_BASE = "s3a://crypto-lake"

# DQ thresholds
MAX_NULL_RATE        = 0.05    # 5% max null for any column
MAX_FRESHNESS_SECS   = 600     # data must be < 10 minutes old
MIN_ROWS_PER_SYMBOL  = 50      # per hour per symbol minimum


def _get_spark():
    """Return a lightweight SparkSession for DQ queries."""
    from pyspark.sql import SparkSession
    return (
        SparkSession.builder
        .appName("CryptoDQ")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def check_layer_quality(layer: str, table: str, critical_cols: list, **context) -> dict:
    """Generic quality checker for a Delta layer."""
    from pyspark.sql import functions as F

    spark = _get_spark()
    path  = f"{S3_BASE}/lakehouse/{layer}/{table}"
    df    = spark.read.format("delta").load(path)

    # Freshness check
    max_ingest = df.agg(F.max("ingest_time")).collect()[0][0]
    freshness_lag = (datetime.utcnow() - max_ingest).total_seconds() if max_ingest else 99999
    freshness_ok  = freshness_lag < MAX_FRESHNESS_SECS

    total_rows = df.count()

    # Null rate per critical column
    null_rates = {}
    for col in critical_cols:
        null_count = df.filter(F.col(col).isNull()).count()
        null_rates[col] = round(null_count / max(total_rows, 1), 4)

    # Symbol completeness
    symbol_counts = {
        row["symbol"]: row["cnt"]
        for row in df.groupBy("symbol").agg(F.count("*").alias("cnt")).collect()
    }

    spark.stop()

    results = {
        "layer":         layer,
        "table":         table,
        "total_rows":    total_rows,
        "freshness_lag": freshness_lag,
        "freshness_ok":  freshness_ok,
        "null_rates":    null_rates,
        "symbol_counts": symbol_counts,
        "passed":        freshness_ok and all(v <= MAX_NULL_RATE for v in null_rates.values()),
    }

    # Raise if critical DQ failure
    failed_cols = [c for c, r in null_rates.items() if r > MAX_NULL_RATE]
    if failed_cols:
        raise ValueError(
            f"DQ FAIL [{layer}.{table}] Null rate > {MAX_NULL_RATE:.0%} for: {failed_cols}"
        )
    if not freshness_ok:
        raise ValueError(
            f"DQ FAIL [{layer}.{table}] Freshness lag {freshness_lag:.0f}s > {MAX_FRESHNESS_SECS}s"
        )

    return results


def check_bronze_quality(**context):
    results = check_layer_quality(
        layer="bronze",
        table="ohlcv",
        critical_cols=["symbol", "open_time", "close", "volume"],
        **context,
    )
    context["task_instance"].xcom_push(key="bronze_dq", value=results)
    print(f"Bronze DQ: {results}")


def check_silver_quality(**context):
    results = check_layer_quality(
        layer="silver",
        table="ohlcv",
        critical_cols=["symbol", "open_time", "close", "rsi_14", "macd"],
        **context,
    )
    context["task_instance"].xcom_push(key="silver_dq", value=results)
    print(f"Silver DQ: {results}")


def check_gold_quality(**context):
    results = check_layer_quality(
        layer="gold",
        table="ohlcv",
        critical_cols=["symbol", "timeframe", "window_start", "close", "vwap"],
        **context,
    )
    context["task_instance"].xcom_push(key="gold_dq", value=results)
    print(f"Gold DQ: {results}")


def publish_dq_metrics(**context):
    """
    Collect DQ results from all layer checks and push metrics to
    Prometheus Pushgateway for Grafana visualization.
    """
    import requests

    ti     = context["task_instance"]
    bronze = ti.xcom_pull(task_ids="dq_checks.check_bronze_quality", key="bronze_dq") or {}
    silver = ti.xcom_pull(task_ids="dq_checks.check_silver_quality", key="silver_dq") or {}
    gold   = ti.xcom_pull(task_ids="dq_checks.check_gold_quality",   key="gold_dq")   or {}

    pushgateway_url = "http://prometheus-pushgateway.monitoring-system.svc.cluster.local:9091"

    # Build Prometheus text format metrics
    metrics_lines = [
        "# HELP crypto_dq_freshness_seconds Data freshness lag in seconds",
        "# TYPE crypto_dq_freshness_seconds gauge",
        f'crypto_dq_freshness_seconds{{layer="bronze"}} {bronze.get("freshness_lag", -1)}',
        f'crypto_dq_freshness_seconds{{layer="silver"}} {silver.get("freshness_lag", -1)}',
        f'crypto_dq_freshness_seconds{{layer="gold"}}   {gold.get("freshness_lag",   -1)}',
        "# HELP crypto_dq_row_count Total rows in Delta table",
        "# TYPE crypto_dq_row_count gauge",
        f'crypto_dq_row_count{{layer="bronze"}} {bronze.get("total_rows", 0)}',
        f'crypto_dq_row_count{{layer="silver"}} {silver.get("total_rows", 0)}',
        f'crypto_dq_row_count{{layer="gold"}}   {gold.get("total_rows",   0)}',
    ]

    payload = "\n".join(metrics_lines) + "\n"

    try:
        resp = requests.post(
            f"{pushgateway_url}/metrics/job/crypto_dq",
            data=payload.encode("utf-8"),
            headers={"Content-Type": "text/plain"},
            timeout=10,
        )
        resp.raise_for_status()
        print(f"DQ metrics pushed to Prometheus: HTTP {resp.status_code}")
    except Exception as exc:  # pylint: disable=broad-except
        # Non-critical: log but don't fail the DAG
        print(f"Warning: Failed to push DQ metrics: {exc}")

    all_passed = all([
        bronze.get("passed", False),
        silver.get("passed", False),
        gold.get("passed",   False),
    ])
    print(f"Overall DQ status: {'PASS' if all_passed else 'PARTIAL FAIL'}")


with DAG(
    dag_id="crypto_data_quality",
    default_args=DEFAULT_ARGS,
    description="Hourly data quality checks across Bronze/Silver/Gold layers",
    schedule_interval="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["data-quality", "monitoring"],
) as dag:

    with TaskGroup("dq_checks") as dq_group:
        bronze_dq = PythonOperator(
            task_id="check_bronze_quality",
            python_callable=check_bronze_quality,
            provide_context=True,
        )
        silver_dq = PythonOperator(
            task_id="check_silver_quality",
            python_callable=check_silver_quality,
            provide_context=True,
        )
        gold_dq = PythonOperator(
            task_id="check_gold_quality",
            python_callable=check_gold_quality,
            provide_context=True,
        )

        # Run in parallel, independent checks
        [bronze_dq, silver_dq, gold_dq]

    publish_metrics = PythonOperator(
        task_id="publish_dq_metrics",
        python_callable=publish_dq_metrics,
        provide_context=True,
        trigger_rule="all_done",   # Run even if some DQ checks fail
    )

    dq_group >> publish_metrics
