"""
crypto_silver_processing.py
DAG: crypto_silver_processing
Schedule: every 5 minutes
Purpose: Process Bronze → Silver with cleaning, deduplication,
         and technical indicator enrichment (RSI, MACD, Bollinger Bands).
Tasks:
  1. wait_for_bronze_dag       – ExternalTaskSensor
  2. check_bronze_freshness    – PythonOperator: ensure Bronze has recent data
  3. submit_silver_spark_job   – SparkSubmitOperator: silver_clean.py
  4. run_data_quality_checks   – PythonOperator: null rate, row count
  5. update_silver_metadata    – PythonOperator: push stats via XCom
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

DEFAULT_ARGS = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "email":            ["alerts@crypto-analytics.io"],
    "email_on_failure": True,
    "retries":          2,
    "retry_delay":      timedelta(minutes=1),
    "execution_timeout":timedelta(minutes=15),
}

SPARK_CONN_ID = "spark_local"
S3_BASE     = "s3a://crypto-lake"


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────
def check_bronze_freshness(**context):
    """Verify Bronze table has rows for the last 10 minutes."""
    execution_date = context["execution_date"]
    print(f"Checking Bronze freshness for execution_date={execution_date}")
    # In production: query Delta table metadata or run a Spark count
    # Here we simulate the check
    return "submit_silver_spark_job"


def run_data_quality_checks(**context):
    """
    Post-processing data quality checks on the Silver table:
    - null_rate_per_column must be < 5%
    - row count must be > 0
    - data must be fresher than 15 minutes
    """
    ti = context["task_instance"]
    execution_date = context["execution_date"]
    process_date   = execution_date.strftime("%Y-%m-%d")

    print(f"Running Silver DQ checks for date={process_date}")

    # Simulated checks (replace with actual Spark queries in production)
    dq_results = {
        "process_date":    process_date,
        "row_count":       150_000,
        "null_rsi_rate":   0.02,
        "null_macd_rate":  0.02,
        "null_close_rate": 0.00,
        "passed":          True,
    }

    if dq_results["null_close_rate"] > 0.05:
        raise ValueError(f"DQ FAIL: close null rate {dq_results['null_close_rate']:.2%} > 5%")
    if dq_results["row_count"] == 0:
        raise ValueError("DQ FAIL: Silver table has 0 rows after processing.")

    ti.xcom_push(key="silver_dq_results", value=dq_results)
    print(f"Silver DQ passed: {dq_results}")


def update_silver_metadata(**context):
    """Push Silver processing stats to XCom for downstream DAGs."""
    ti = context["task_instance"]
    dq_results = ti.xcom_pull(task_ids="run_data_quality_checks", key="silver_dq_results")
    process_date = context["execution_date"].strftime("%Y-%m-%d")

    metadata = {
        "layer":         "silver",
        "process_date":  process_date,
        "row_count":     dq_results.get("row_count", 0) if dq_results else 0,
        "completed_at":  datetime.utcnow().isoformat(),
        "status":        "SUCCESS",
    }
    ti.xcom_push(key="silver_metadata", value=metadata)
    print(f"Silver metadata updated: {metadata}")


# ─────────────────────────────────────────────
# DAG
# ─────────────────────────────────────────────
with DAG(
    dag_id="crypto_silver_processing",
    default_args=DEFAULT_ARGS,
    description="Bronze → Silver: cleaning + technical indicators",
    schedule_interval="*/5 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=2,
    tags=["silver", "processing", "indicators"],
) as dag:

    # ── Task 1: Wait for Bronze DAG ────────────────────────────────────────────
    wait_for_bronze = ExternalTaskSensor(
        task_id="wait_for_bronze_dag",
        external_dag_id="crypto_bronze_ingestion",
        external_task_id="validate_bronze_output",
        allowed_states=["success"],
        timeout=120,
        poke_interval=20,
        mode="reschedule",
    )

    # ── Task 2: Check Bronze freshness ─────────────────────────────────────────
    check_freshness = BranchPythonOperator(
        task_id="check_bronze_freshness",
        python_callable=check_bronze_freshness,
        provide_context=True,
    )

    skip_processing = EmptyOperator(task_id="skip_processing")

    # ── Task Group: Silver Processing ──────────────────────────────────────────
    with TaskGroup("silver_processing_group") as silver_group:

        # Task 3: Submit Spark job
        submit_silver_job = SparkSubmitOperator(
            task_id="submit_silver_spark_job",
            conn_id=SPARK_CONN_ID,
            application="spark/jobs/silver_clean.py",
            application_args=[
                "--mode", "batch",
                "--date", "{{ ds }}",
            ],
            name="crypto-silver-clean",
            deploy_mode="client",
            driver_memory="1g",
            executor_memory="1g",
            executor_cores=1,
            num_executors=1,
            conf={
                "spark.driver.host":              "crypto-airflow-scheduler",
                "spark.sql.shuffle.partitions":   "100",
                "spark.sql.adaptive.enabled":     "true",
            },
            packages="io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
        )

        # Task 4: DQ checks
        dq_checks = PythonOperator(
            task_id="run_data_quality_checks",
            python_callable=run_data_quality_checks,
            provide_context=True,
        )

        # Task 5: Update metadata
        update_metadata = PythonOperator(
            task_id="update_silver_metadata",
            python_callable=update_silver_metadata,
            provide_context=True,
        )

        submit_silver_job >> dq_checks >> update_metadata

    # ── Dependencies ──────────────────────────────────────────────────────────
    wait_for_bronze >> check_freshness >> [silver_group, skip_processing]

    # Trigger Gold DAG when Silver group completes successfully
    trigger_gold = TriggerDagRunOperator(
        task_id="trigger_gold_dag",
        trigger_dag_id="crypto_gold_aggregation",
        execution_date="{{ execution_date }}",
        reset_dag_run=False,
        wait_for_completion=False,
    )

    silver_group >> trigger_gold
