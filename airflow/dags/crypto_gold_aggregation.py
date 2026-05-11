"""
crypto_gold_aggregation.py
DAG: crypto_gold_aggregation
Schedule: every 15 minutes
Purpose: Silver → Gold: multi-timeframe OHLCV aggregation,
         window functions, joins, pivot/unpivot, Z-ordering.
Tasks:
  1. wait_for_silver           – ExternalTaskSensor
  2. check_silver_s3_file    – FileSensor: checkpoint on MinIO/S3
  3. submit_gold_spark_job     – SparkSubmitOperator: gold_aggregate.py
  4. optimize_gold_table       – SparkSubmitOperator: OPTIMIZE + VACUUM
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup

DEFAULT_ARGS = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "email":            ["alerts@crypto-analytics.io"],
    "email_on_failure": True,
    "retries":          2,
    "retry_delay":      timedelta(minutes=2),
    "execution_timeout":timedelta(minutes=25),
}

SPARK_CONN_ID = "spark_local"


def notify_gold_ready(**context):
    """Push Gold completion status to XCom and optionally trigger alert evaluation."""
    ti = context["task_instance"]
    process_date = context["execution_date"].strftime("%Y-%m-%d")
    metadata = {
        "layer":        "gold",
        "process_date": process_date,
        "completed_at": datetime.utcnow().isoformat(),
        "status":       "SUCCESS",
        "timeframes":   ["5m", "15m", "1h", "4h", "1d"],
    }
    ti.xcom_push(key="gold_metadata", value=metadata)
    print(f"Gold layer ready: {metadata}")


with DAG(
    dag_id="crypto_gold_aggregation",
    default_args=DEFAULT_ARGS,
    description="Silver → Gold: OHLCV aggregation + window functions",
    schedule_interval="*/15 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["gold", "aggregation", "window-functions"],
) as dag:

    # ── Task 1: Wait for Silver ────────────────────────────────────────────────
    wait_for_silver = ExternalTaskSensor(
        task_id="wait_for_silver_dag",
        external_dag_id="crypto_silver_processing",
        external_task_id="silver_processing_group.update_silver_metadata",
        allowed_states=["success"],
        timeout=180,
        poke_interval=30,
        mode="reschedule",
    )

    # ── Task 2: MinIO/S3 checkpoint file sensor ────────────────────────────────────
    check_silver_checkpoint = FileSensor(
        task_id="check_silver_s3_checkpoint",
        filepath="/checkpoints/silver_clean/_metadata",
        fs_conn_id="s3_default",
        poke_interval=20,
        timeout=120,
        mode="reschedule",
    )

    # ── Task Group: Gold Processing ────────────────────────────────────────────
    with TaskGroup("gold_processing_group") as gold_group:

        submit_gold_job = SparkSubmitOperator(
            task_id="submit_gold_spark_job",
            conn_id=SPARK_CONN_ID,
            application="spark/jobs/gold_aggregate.py",
            application_args=[
                "--mode", "batch",
                "--date", "{{ ds }}",
            ],
            name="crypto-gold-aggregate",
            deploy_mode="client",
            driver_memory="1g",
            executor_memory="1g",
            executor_cores=1,
            num_executors=1,
            conf={
                "spark.driver.host":                   "crypto-airflow-scheduler",
                "spark.sql.shuffle.partitions":        "200",
                "spark.sql.adaptive.enabled":          "true",
                "spark.sql.adaptive.skewJoin.enabled": "true",
                "spark.sql.autoBroadcastJoinThreshold":"10485760",
            },
            packages="io.delta:delta-spark_2.12:3.1.0,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
        )

        # Run Delta OPTIMIZE + VACUUM to keep Gold table performant
        optimize_gold = SparkSubmitOperator(
            task_id="optimize_gold_table",
            conn_id=SPARK_CONN_ID,
            application_args=[],
            application="",          # inline SQL via --sql flag
            conf={
            },
            name="crypto-gold-optimize",
            # Use spark-sql for Delta maintenance commands
            jars="",
            driver_memory="1g",
            executor_memory="1g",
            num_executors=1,
        )

        notify_ready = PythonOperator(
            task_id="notify_gold_ready",
            python_callable=notify_gold_ready,
            provide_context=True,
        )

        submit_gold_job >> optimize_gold >> notify_ready

    # ── Dependencies ──────────────────────────────────────────────────────────
    wait_for_silver >> check_silver_checkpoint >> gold_group
