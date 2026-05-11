"""
crypto_model_training.py  +  crypto_cleanup.py
Two DAGs in one file for brevity:
  - crypto_model_training  (@weekly)  : Spark MLlib RandomForest + K-Means
  - crypto_cleanup         (@daily)   : Delta VACUUM + OPTIMIZE
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.task_group import TaskGroup

SPARK_CONN_ID = "spark_local"
S3_BASE     = "s3a://crypto-lake"

# ══════════════════════════════════════════════
# DAG 1 — ML Model Training (@weekly)
# ══════════════════════════════════════════════

ML_DEFAULT_ARGS = {
    "owner":            "ml-team",
    "depends_on_past":  False,
    "email":            ["ml@crypto-analytics.io"],
    "email_on_failure": True,
    "retries":          1,
    "retry_delay":      timedelta(minutes=10),
    "execution_timeout":timedelta(hours=3),
}

ML_TRAINING_SCRIPT = """
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

spark = SparkSession.builder.appName("Validator") \
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("S3_ENDPOINT", "http://minio:9000")) \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "admin")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", "password")) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

# ── Load Gold data (last 90 days) ─────────────────────────────────────────────
gold_df = (
    spark.read
    .format("delta")
    .load(f"{sys.argv[1]}/lakehouse/gold/ohlcv")
    .filter(F.col("timeframe") == "1h")
    .filter(F.col("window_start") >= F.date_sub(F.current_timestamp(), 90))
    .na.drop(subset=["rsi_14", "macd", "volume_ratio", "close"])
)

# ── Feature engineering ───────────────────────────────────────────────────────
FEATURES = ["rsi_14", "macd", "volume_ratio", "ma7", "ma25", "atr_14"]

# Label: 1 = price went up next candle, 0 = down
from pyspark.sql import Window
win = Window.partitionBy("symbol").orderBy("window_start")
labeled_df = (
    gold_df
    .withColumn("next_close", F.lead("close", 1).over(win))
    .withColumn("label", F.when(F.col("next_close") > F.col("close"), 1.0).otherwise(0.0))
    .na.drop(subset=["next_close", "label"] + FEATURES)
)

# ── RandomForest Pipeline ─────────────────────────────────────────────────────
assembler = VectorAssembler(inputCols=FEATURES, outputCol="raw_features")
scaler    = StandardScaler(inputCol="raw_features", outputCol="features")
rf        = RandomForestClassifier(
    labelCol="label", featuresCol="features",
    numTrees=100, maxDepth=5, seed=42,
)
pipeline  = Pipeline(stages=[assembler, scaler, rf])

train_df, test_df = labeled_df.randomSplit([0.8, 0.2], seed=42)
model = pipeline.fit(train_df)

evaluator  = MulticlassClassificationEvaluator(labelCol="label", metricName="accuracy")
accuracy   = evaluator.evaluate(model.transform(test_df))
print(f"RandomForest accuracy: {accuracy:.4f}")

model_path = f"{sys.argv[1]}/models/rf_price_trend"
model.write().overwrite().save(model_path)
print(f"Model saved to: {model_path}")

# ── K-Means Clustering ────────────────────────────────────────────────────────
from pyspark.ml.clustering import KMeans
kmeans_features = ["rsi_14", "volume_ratio", "macd"]
km_assembler = VectorAssembler(inputCols=kmeans_features, outputCol="km_features")
kmeans = KMeans(featuresCol="km_features", k=5, seed=42)
km_pipeline = Pipeline(stages=[km_assembler, kmeans])
km_model = km_pipeline.fit(labeled_df)
km_model.write().overwrite().save(f"{sys.argv[1]}/models/kmeans_symbol_cluster")
print("K-Means model saved.")

spark.stop()
"""


def save_ml_script(**context):
    """Write the inline ML script to a temp file on the driver."""
    import tempfile, os
    script_path = "/tmp/ml_training.py"
    with open(script_path, "w") as f:
        f.write(ML_TRAINING_SCRIPT)
    context["task_instance"].xcom_push(key="ml_script_path", value=script_path)


def register_ml_model(**context):
    """Log model metadata after successful training."""
    run_date = context["execution_date"].strftime("%Y-%m-%d")
    print(f"ML models registered for week ending {run_date}")


with DAG(
    dag_id="crypto_model_training",
    default_args=ML_DEFAULT_ARGS,
    description="Weekly Spark MLlib training: RandomForest + K-Means",
    schedule_interval="@weekly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ml", "training", "mllib"],
) as ml_dag:

    prepare_script = PythonOperator(
        task_id="prepare_ml_script",
        python_callable=save_ml_script,
        provide_context=True,
    )

    train_models = SparkSubmitOperator(
        task_id="train_ml_models",
        conn_id=SPARK_CONN_ID,
        application="{{ task_instance.xcom_pull(task_ids='prepare_ml_script', key='ml_script_path') }}",
        application_args=[S3_BASE],
        name="crypto-ml-training",
        deploy_mode="client",
        driver_memory="2g",
        executor_memory="2g",
        executor_cores=2,
        num_executors=2,
        conf={
            "spark.ml.randomForest.maxMemoryInMB": "512",
        },
        packages="io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
    )

    register_model = PythonOperator(
        task_id="register_ml_model",
        python_callable=register_ml_model,
        provide_context=True,
    )

    prepare_script >> train_models >> register_model


# ══════════════════════════════════════════════
# DAG 2 — Delta Cleanup (@daily)
# ══════════════════════════════════════════════

CLEANUP_DEFAULT_ARGS = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "email_on_failure": True,
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
    "execution_timeout":timedelta(hours=1),
}


def run_delta_maintenance(layer: str, table: str, retain_hours: int = 168, **context):
    """
    Run Delta Lake VACUUM (remove old files) and OPTIMIZE (compact small files)
    for a given table. retain_hours=168 keeps 7 days of history.
    """
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName(f"DeltaCleanup-{layer}-{table}")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

    path = f"{S3_BASE}/lakehouse/{layer}/{table}"
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

    spark.sql(f"VACUUM delta.`{path}` RETAIN {retain_hours} HOURS")
    spark.sql(f"OPTIMIZE delta.`{path}`")

    spark.stop()
    print(f"Cleanup complete for {layer}/{table}")


with DAG(
    dag_id="crypto_cleanup",
    default_args=CLEANUP_DEFAULT_ARGS,
    description="Daily Delta Lake VACUUM + OPTIMIZE for all Medallion tables",
    schedule_interval="0 0 * * *",    # midnight UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["maintenance", "delta", "cleanup"],
) as cleanup_dag:

    with TaskGroup("delta_maintenance") as maintenance_group:

        vacuum_bronze = PythonOperator(
            task_id="vacuum_optimize_bronze",
            python_callable=run_delta_maintenance,
            op_kwargs={"layer": "bronze", "table": "ohlcv", "retain_hours": 168},
            provide_context=True,
        )
        vacuum_silver = PythonOperator(
            task_id="vacuum_optimize_silver",
            python_callable=run_delta_maintenance,
            op_kwargs={"layer": "silver", "table": "ohlcv", "retain_hours": 336},  # 14 days
            provide_context=True,
        )
        vacuum_gold = PythonOperator(
            task_id="vacuum_optimize_gold",
            python_callable=run_delta_maintenance,
            op_kwargs={"layer": "gold", "table": "ohlcv", "retain_hours": 720},    # 30 days
            provide_context=True,
        )

        # Run in parallel
        [vacuum_bronze, vacuum_silver, vacuum_gold]
