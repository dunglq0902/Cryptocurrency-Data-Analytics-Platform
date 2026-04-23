"""
jobs/bronze_ingest.py
Job 1 – Kafka → Bronze Delta Lake (Structured Streaming, append mode)

Chạy:
    spark-submit \
        --master k8s://https://<k8s-api> \
        spark/jobs/bronze_ingest.py
"""

import logging
import os
import sys

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StringType

# Add project root to path so relative imports work when spark-submitted
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from schemas.bronze_schema import BRONZE_OHLCV_SCHEMA, BRONZE_TICK_SCHEMA
from utils.spark_session import (
    create_spark_session,
    checkpoint_path,
    delta_path,
    KAFKA_BOOTSTRAP_SERVERS,
)

logger = logging.getLogger("BronzeIngest")

# ─────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────
KAFKA_TOPICS_OHLCV  = "raw-ohlcv"
KAFKA_TOPICS_TICKS  = "raw-crypto-ticks"
TRIGGER_INTERVAL    = "30 seconds"
WATERMARK_DELAY     = "2 minutes"

# Hàm này tạo stream đọc dữ liệu OHLCV (candlestick) từ Kafka.
# 
def build_ohlcv_stream(spark: SparkSession):    # build_ohlcv_stream = đọc dữ liệu đã được tổng hợp theo candle
    """
    Read raw OHLCV JSON messages from Kafka, parse, validate,
    and write to Bronze Delta table in append mode.
    """
    # ── 1. Read from Kafka ────────────────────────────────────────────────────
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPICS_OHLCV)
        .option("startingOffsets", "latest")
        # Spark sẽ bỏ qua toàn bộ dữ liệu cũ đã có trong topic trước thời điểm job start
        # Và chỉ đọc những message được gửi vào Kafka sau khi Spark bắt đầu chạy

        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 50_000)     # Back-pressure
        .load()
    )

    # ── 2. Parse JSON payload ─────────────────────────────────────────────────
    parsed_df = (
        raw_df
        .select(
            F.col("key").cast(StringType()).alias("_kafka_key"),
            F.col("partition").alias("_kafka_partition"),
            F.col("offset").alias("_kafka_offset"),
            F.col("timestamp").alias("_kafka_ts"),
            F.from_json(
                F.col("value").cast(StringType()),
                BRONZE_OHLCV_SCHEMA
            ).alias("data")
        )
        .select("_kafka_key", "_kafka_partition", "_kafka_offset", "_kafka_ts", "data.*")
    )

    # ── 3. Schema validation – drop rows missing critical fields ──────────────
    valid_df = parsed_df.filter(
        F.col("symbol").isNotNull()
        & F.col("open_time").isNotNull()
        & F.col("close").isNotNull()
        & F.col("close").cast("double").isNotNull()
    )

    # ── 4. Add watermark on event_time for late data handling ─────────────────
    watermarked_df = valid_df.withWatermark("open_time", WATERMARK_DELAY)

    # ── 5. Enrich with partition_date if not present ──────────────────────────
    enriched_df = watermarked_df.withColumn(
        "partition_date",
        F.coalesce(
            F.col("partition_date"),
            F.to_date(F.col("open_time"))
        )
    )

    # ── 6. Write to Bronze Delta Lake ─────────────────────────────────────────
    query = (
        enriched_df
        .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path("bronze_ohlcv"))
        .option("path", delta_path("bronze", "ohlcv"))
        .trigger(processingTime=TRIGGER_INTERVAL)
        .partitionBy("symbol", "partition_date")
        .queryName("bronze_ohlcv_ingest")
        .start()
    )
    return query

# Hàm này tạo stream đọc dữ liệu tick (real-time trade) từ Kafka.
# Tick data là gì? Là dữ liệu từng giao dịch nhỏ nhất
# =>lấy dữ liệu raw để xử lý sâu 
def build_ticks_stream(spark: SparkSession):    #build_ticks_stream = đọc dòng dữ liệu nhanh, nhỏ, liên tục
    """
    Read raw tick (trade) messages from Kafka → Bronze tick table.
    """
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPICS_TICKS)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 100_000)
        .load()
    )

    parsed_df = (
        raw_df
        .select(
            F.from_json(
                F.col("value").cast(StringType()),
                BRONZE_TICK_SCHEMA,
            ).alias("data")
        )
        .select("data.*")
        .filter(
            F.col("symbol").isNotNull()
            & F.col("price").isNotNull()
        )
        .withWatermark("event_time", WATERMARK_DELAY)
    )

    query = (
        parsed_df
        .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path("bronze_ticks"))
        .option("path", delta_path("bronze", "ticks"))
        .trigger(processingTime=TRIGGER_INTERVAL)
        .partitionBy("symbol")
        .queryName("bronze_ticks_ingest")
        .start()
    )
    return query


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────
def main():
    spark = create_spark_session(
        app_name="CryptoAnalytics-Bronze-Ingest",
        extra_configs={
            # Delta auto-merge schema for Bronze (schema may evolve)
            "spark.databricks.delta.schema.autoMerge.enabled": "true",
        }
    )

    logger.info("Starting Bronze ingestion streams...")
    q_ohlcv = build_ohlcv_stream(spark)
    q_ticks = build_ticks_stream(spark)

    logger.info("Streaming queries started: [%s, %s]", q_ohlcv.name, q_ticks.name)

    # Block until both streams terminate
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    )
    main()
