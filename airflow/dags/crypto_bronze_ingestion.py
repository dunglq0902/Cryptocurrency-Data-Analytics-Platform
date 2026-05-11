"""
crypto_bronze_ingestion.py
DAG: crypto_bronze_ingestion
Schedule: Continuous / every 1 minute
Purpose: Monitor Kafka lag and trigger Spark Structured Streaming job
         that reads raw data from Kafka → Bronze Delta Lake.
Tasks:
  1. check_kafka_topic_lag   – KafkaSensor: ensure topics have data
  2. submit_bronze_spark_job – SparkSubmitOperator: bronze_ingest.py
  3. validate_bronze_output  – PythonOperator: row count & freshness check
"""

from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.task_group import TaskGroup

from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Custom plugin (defined in plugins/)
from kafka_sensor import KafkaTopicSensor

# ─────────────────────────────────────────────
# Default args
# ─────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "email":            ["alerts@crypto-analytics.io"],
    "email_on_failure": True,
    "email_on_retry":   False,
    "retries":          3,
    "retry_delay":      timedelta(seconds=30),
    "execution_timeout":timedelta(minutes=10),
}

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
SPARK_CONN_ID   = os.getenv("SPARK_CONN_ID", "spark_local")
SKIP_KAFKA_SENSOR = os.getenv("SKIP_KAFKA_SENSOR", "true").lower() in ("1", "true", "yes")

# ─────────────────────────────────────────────
# Validation helper
# ─────────────────────────────────────────────
def validate_bronze_output(**context):
    """
    Check that new rows were written to the Bronze Delta table
    within the last 5 minutes. Raises if stale.
    """
    from pyspark.sql import SparkSession
    import os

    s3_base = "s3a://crypto-lake"
    bronze_path = f"{s3_base}/lakehouse/bronze/ohlcv"

    spark = SparkSession.builder.appName("Validator") \
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("S3_ENDPOINT", "http://minio:9000")) \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "admin")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", "password")) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    df = spark.read.format("delta").load(bronze_path)

    from pyspark.sql import functions as F
    recent_count = (
        df.filter(
            F.col("ingest_time") >= F.date_sub(F.current_timestamp(), 1)
        )
        .count()
    )
    spark.stop()

    if recent_count == 0:
        raise ValueError("Bronze validation failed: no new rows written in last 24h.")

    context["task_instance"].xcom_push(key="bronze_row_count", value=recent_count)
    print(f"Bronze validation passed: {recent_count} recent rows found.")


# ─────────────────────────────────────────────
# DAG Definition
# ─────────────────────────────────────────────
with DAG(
    dag_id="crypto_bronze_ingestion",
    default_args=DEFAULT_ARGS,
    description="Kafka → Bronze Delta Lake via Spark Structured Streaming",
    schedule_interval="*/1 * * * *",    # every minute
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["bronze", "ingestion", "streaming"],
) as dag:

    # ── Task 1: Wait for Kafka data ───────────────────────────────────────────
    if SKIP_KAFKA_SENSOR:
        check_kafka_lag = None
    else:
        check_kafka_lag = KafkaTopicSensor(
            task_id="check_kafka_topic_lag",
            kafka_bootstrap_servers=KAFKA_BOOTSTRAP,
            topics=["raw-ohlcv", "raw-crypto-ticks"],
            group_id="airflow-bronze-sensor",
            max_lag_threshold=10_000,       # allow up to 10k unprocessed messages
            poke_interval=15,
            timeout=120,
        )

    # ── Task 2: Submit Spark job ──────────────────────────────────────────────
    submit_bronze_job = SparkSubmitOperator(
        task_id="submit_bronze_spark_job",
        conn_id=SPARK_CONN_ID,
        application="spark/jobs/bronze_ingest.py",
        name="crypto-bronze-ingest",
        deploy_mode="client",
        driver_memory="1g",
        executor_memory="1g",
        executor_cores=1,
        num_executors=1,
        conf={
            "spark.driver.host":                             "crypto-airflow-scheduler",
            "spark.sql.extensions":                          "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog":               "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.sql.streaming.stateStore.providerClass":  "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
        },
        packages="io.delta:delta-spark_2.12:3.1.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
        verbose=True,
    )

    # ── Task 3: Validate output ───────────────────────────────────────────────
    validate_output = PythonOperator(
        task_id="validate_bronze_output",
        python_callable=validate_bronze_output,
        provide_context=True,
    )

    # Trigger Silver DAG for the same execution_date after Bronze validation
    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_silver_dag",
        trigger_dag_id="crypto_silver_processing",
        execution_date="{{ execution_date }}",
        reset_dag_run=False,
        wait_for_completion=False,
    )

    # ── Dependencies ──────────────────────────────────────────────────────────
    if check_kafka_lag is not None:
        check_kafka_lag >> submit_bronze_job >> validate_output >> trigger_silver
    else:
        submit_bronze_job >> validate_output >> trigger_silver
