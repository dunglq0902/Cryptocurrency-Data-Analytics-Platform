"""
crypto_alert_evaluation.py
DAG: crypto_alert_evaluation
Schedule: every 1 minute
Purpose: Evaluate user alert rules against Gold layer data and dispatch
         buy/sell notifications via Telegram, Email, or Webhook.
Tasks:
  1. wait_for_gold_data       – ExternalTaskSensor
  2. submit_alert_spark_job   – SparkSubmitOperator: alert_evaluator.py
  3. dispatch_notifications   – PythonOperator: consume Kafka → notify
"""

from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.external_task import ExternalTaskSensor

DEFAULT_ARGS = {
    "owner":            "alert-engine",
    "depends_on_past":  False,
    "email":            ["alerts@crypto-analytics.io"],
    "email_on_failure": True,
    "retries":          1,
    "retry_delay":      timedelta(seconds=20),
    "execution_timeout":timedelta(minutes=5),
}

SPARK_CONN_ID    = "spark_local"
KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
ALERT_TOPIC      = "alert-events"
NOTIFICATION_API = "http://alert-api:8000"


def dispatch_notifications(**context):
    """
    Consume alert events from Kafka topic and forward to the
    Notification Service (FastAPI) which handles Telegram/Email/Webhook.
    """
    import json
    import requests
    from confluent_kafka import Consumer, KafkaError

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id":          "airflow-alert-dispatcher",
        "auto.offset.reset": "latest",
        "enable.auto.commit":False,
    })
    consumer.subscribe([ALERT_TOPIC])

    dispatched = 0
    errors     = 0
    max_msgs   = 500    # Safety cap per DAG run

    try:
        for _ in range(max_msgs):
            msg = consumer.poll(timeout=2.0)
            if msg is None:
                break
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    break
                errors += 1
                continue

            try:
                alert_event = json.loads(msg.value().decode("utf-8"))
                resp = requests.post(
                    f"{NOTIFICATION_API}/api/v1/notifications/dispatch",
                    json=alert_event,
                    timeout=5,
                )
                if resp.status_code == 200:
                    dispatched += 1
                    consumer.commit(msg)
                else:
                    errors += 1
                    print(f"Dispatch failed: {resp.status_code} {resp.text}")
            except Exception as exc:  # pylint: disable=broad-except
                errors += 1
                print(f"Error processing alert event: {exc}")
    finally:
        consumer.close()

    summary = {"dispatched": dispatched, "errors": errors}
    context["task_instance"].xcom_push(key="dispatch_summary", value=summary)
    print(f"Notification dispatch complete: {summary}")

    if errors > dispatched * 0.5 and dispatched > 0:
        raise ValueError(f"Too many dispatch errors: {errors}/{dispatched + errors}")


with DAG(
    dag_id="crypto_alert_evaluation",
    default_args=DEFAULT_ARGS,
    description="Evaluate alert rules and dispatch buy/sell notifications",
    schedule_interval="*/1 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["alert", "notification", "streaming"],
) as dag:

    wait_for_gold = ExternalTaskSensor(
        task_id="wait_for_gold_data",
        external_dag_id="crypto_gold_aggregation",
        external_task_id="gold_processing_group.notify_gold_ready",
        allowed_states=["success"],
        timeout=90,
        poke_interval=15,
        mode="reschedule",
    )

    submit_alert_job = SparkSubmitOperator(
        task_id="submit_alert_spark_job",
        conn_id=SPARK_CONN_ID,
        application="spark/jobs/alert_evaluator.py",
        name="crypto-alert-evaluator",
        deploy_mode="client",
        driver_memory="1g",
        executor_memory="1g",
        executor_cores=1,
        num_executors=1,
        conf={
            "spark.driver.host":              "crypto-airflow-scheduler",
            "spark.sql.adaptive.enabled":     "true",
        },
        packages=(
            "io.delta:delta-spark_2.12:3.1.0,"
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262"
        ),
    )

    dispatch = PythonOperator(
        task_id="dispatch_notifications",
        python_callable=dispatch_notifications,
        provide_context=True,
    )

    wait_for_gold >> submit_alert_job >> dispatch
