"""
jobs/silver_clean.py
Job 2 – Bronze → Silver: Data Cleaning + Technical Indicators

Pipeline 6 stages:
  Stage 1 – Ingest & Parse   : Đọc Bronze Delta table
  Stage 2 – Clean            : Dedup, null handling, normalize timestamp
  Stage 3 – Enrich           : Technical indicators (RSI, MACD, Bollinger)
  Stage 4 – Aggregate        : Window aggregations (MA, ATR, volume ratio)
  Stage 5 – Signal           : Candle pattern classification
  Stage 6 – Persist          : Ghi Silver Delta table

Chạy:
    spark-submit spark/jobs/silver_clean.py \
        --mode batch        # hoặc streaming
        --date 2024-01-15   # chỉ dùng khi mode=batch
"""

import argparse
import logging
import os
import sys
from datetime import date, timedelta

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.spark_session import create_spark_session, checkpoint_path, delta_path
from udfs.indicator_udfs import (
    udf_classify_candle,
    udf_calculate_atr,
)

logger = logging.getLogger("SilverClean")

TRIGGER_INTERVAL = "5 minutes"
WATERMARK_DELAY  = "3 minutes"


# ═══════════════════════════════════════════════════════════════════════════════
# STAGE 2 – CLEAN
# ═══════════════════════════════════════════════════════════════════════════════

def clean(df: DataFrame) -> DataFrame:
    """
    - Deduplicate on (symbol, open_time)
    - Drop rows with null critical columns
    - Normalize all timestamps to UTC
    - Cast price columns to DoubleType
    - Filter obviously invalid data (negative price / volume)
    """
    logger.info("Stage 2: Cleaning...")

    # Column pruning – only keep what Silver needs
    df = df.select(
        "symbol", "open_time", "close_time", "interval",
        "open", "high", "low", "close",
        "volume", "quote_volume", "trade_count",
        "taker_buy_base_vol",
        "partition_date", "ingest_time",
    )

    # Remove rows with null mandatory fields
    df = df.filter(
        F.col("symbol").isNotNull()
        & F.col("open_time").isNotNull()
        & F.col("close").isNotNull()
        & F.col("volume").isNotNull()
    )

    # Cast all OHLCV columns to DoubleType (defensive)
    for col in ("open", "high", "low", "close", "volume", "quote_volume",
                "taker_buy_base_vol"):
        df = df.withColumn(col, F.col(col).cast(DoubleType()))

    # Sanity filters – prices and volume must be positive
    df = df.filter(
        (F.col("close") > 0)
        & (F.col("volume") >= 0)
        & (F.col("high") >= F.col("low"))
    )

    # Deduplicate: keep the latest ingest for each (symbol, open_time)
    window_dedup = Window.partitionBy("symbol", "open_time").orderBy(F.col("ingest_time").desc())
    df = (
        df
        .withColumn("_rank", F.row_number().over(window_dedup))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )

    # Normalize partition_date
    df = df.withColumn("partition_date", F.to_date(F.col("open_time")))

    logger.info("Stage 2 complete.")
    return df


# ═══════════════════════════════════════════════════════════════════════════════
# STAGE 3 – ENRICH  (window-based indicators)
# ═══════════════════════════════════════════════════════════════════════════════

def enrich_indicators(df: DataFrame) -> DataFrame:
    """
    Compute technical indicators using Spark Window functions:
      - Moving Averages: MA7, MA25, MA99
      - Bollinger Bands: BB Upper/Lower (MA20 ± 2σ)
      - Lag close (prev_close) for ATR and price change
      - RSI-14 and MACD using pandas_udf
    """
    logger.info("Stage 3: Enriching indicators...")

    sym_time_win = (
        Window
        .partitionBy("symbol", "interval")
        .orderBy("open_time")
    )

    # ── Moving Averages (rangeBetween in row units) ───────────────────────────
    ma7_win  = sym_time_win.rowsBetween(-6,   0)
    ma25_win = sym_time_win.rowsBetween(-24,  0)
    ma99_win = sym_time_win.rowsBetween(-98,  0)

    df = (
        df
        .withColumn("ma7",  F.avg("close").over(ma7_win))
        .withColumn("ma25", F.avg("close").over(ma25_win))
        .withColumn("ma99", F.avg("close").over(ma99_win))
    )

    # ── Bollinger Bands (20-period) ───────────────────────────────────────────
    bb_win = sym_time_win.rowsBetween(-19, 0)
    df = (
        df
        .withColumn("bb_middle", F.avg("close").over(bb_win))
        .withColumn("_std20",    F.stddev_samp("close").over(bb_win))
        .withColumn("bb_upper",  F.col("bb_middle") + 2 * F.col("_std20"))
        .withColumn("bb_lower",  F.col("bb_middle") - 2 * F.col("_std20"))
        .drop("_std20")
    )

    # ── Prev close for ATR & price change ────────────────────────────────────
    df = df.withColumn("prev_close", F.lag("close", 1).over(sym_time_win))

    # ── ATR True Range (per-row UDF), then 14-period rolling avg ─────────────
    df = df.withColumn(
        "_tr",
        udf_calculate_atr(F.col("high"), F.col("low"), F.col("prev_close"))
    )
    atr_win = sym_time_win.rowsBetween(-13, 0)
    df = df.withColumn("atr_14", F.avg("_tr").over(atr_win)).drop("_tr")

    # ── Price change % ────────────────────────────────────────────────────────
    df = df.withColumn(
        "price_change_pct",
        F.when(
            F.col("prev_close").isNotNull() & (F.col("prev_close") != 0),
            (F.col("close") - F.col("prev_close")) / F.col("prev_close") * 100,
        ).otherwise(None)
    )

    # ── RSI-14 (SMA-based approximation using window functions) ────────────
    # Avoid using UDFs inside Window expressions (unsupported). Compute an
    # approximate RSI using simple moving averages of gains/losses over 14 periods.
    delta = (F.col("close") - F.col("prev_close"))
    df = df.withColumn("_delta", delta)
    df = df.withColumn("_gain", F.when(F.col("_delta") > 0, F.col("_delta")).otherwise(0.0))
    df = df.withColumn("_loss", F.when(F.col("_delta") < 0, -F.col("_delta")).otherwise(0.0))
    rsi_win = sym_time_win.rowsBetween(-13, 0)
    df = (
        df
        .withColumn("avg_gain_14", F.avg("_gain").over(rsi_win))
        .withColumn("avg_loss_14", F.avg("_loss").over(rsi_win))
    )
    df = df.withColumn(
        "rsi_14",
        F.when(
            F.col("avg_loss_14").isNull() | F.col("avg_gain_14").isNull(),
            None
        ).when(
            F.col("avg_loss_14") == 0,
            F.lit(100.0)
        ).otherwise(
            100 - (100 / (1 + (F.col("avg_gain_14") / F.col("avg_loss_14"))))
        )
    ).drop("_delta", "_gain", "_loss", "avg_gain_14", "avg_loss_14")

    # ── MACD (SMA-based approximation) ──────────────────────────────────────
    # Compute 12/26-period simple moving averages and a 9-period SMA signal line.
    sma12_win = sym_time_win.rowsBetween(-11, 0)
    sma26_win = sym_time_win.rowsBetween(-25, 0)
    df = (
        df
        .withColumn("sma12", F.avg("close").over(sma12_win))
        .withColumn("sma26", F.avg("close").over(sma26_win))
    )
    df = df.withColumn("macd", F.col("sma12") - F.col("sma26"))
    macd_signal_win = sym_time_win.rowsBetween(-8, 0)
    df = df.withColumn("macd_signal", F.avg("macd").over(macd_signal_win))
    df = df.withColumn("macd_hist", F.col("macd") - F.col("macd_signal")).drop("sma12", "sma26")

    # ── Average volume (20-period) and ratio ──────────────────────────────────
    vol_win = sym_time_win.rowsBetween(-19, 0)
    df = (
        df
        .withColumn("avg_volume_20", F.avg("volume").over(vol_win))
        .withColumn(
            "volume_ratio",
            F.when(
                F.col("avg_volume_20").isNotNull() & (F.col("avg_volume_20") != 0),
                F.col("volume") / F.col("avg_volume_20"),
            ).otherwise(None)
        )
    )

    logger.info("Stage 3 complete.")
    return df


# ═══════════════════════════════════════════════════════════════════════════════
# STAGE 4/5 – CANDLE PATTERNS + METADATA
# ═══════════════════════════════════════════════════════════════════════════════

def add_patterns_and_metadata(df: DataFrame) -> DataFrame:
    """Classify candle patterns and add processing_time."""
    logger.info("Stage 4/5: Patterns & metadata...")

    df = df.withColumn(
        "candle_pattern",
        udf_classify_candle(
            F.col("open"), F.col("high"), F.col("low"), F.col("close")
        )
    )
    df = df.withColumn("processing_time", F.current_timestamp())
    return df


# ═══════════════════════════════════════════════════════════════════════════════
# STAGE 6 – PERSIST (SILVER)
# ═══════════════════════════════════════════════════════════════════════════════

def persist_silver(df: DataFrame, mode: str = "overwrite"):
    """Write Silver Delta table partitioned by symbol and date."""
    logger.info("Stage 6: Writing Silver Delta table...")

    silver_path = delta_path("silver", "ohlcv")
    (
        df
        .write
        .format("delta")
        .mode(mode)
        .option("mergeSchema", "true")
        .partitionBy("symbol", "partition_date")
        .save(silver_path)
    )
    logger.info("Silver table written to: %s", silver_path)


# ═══════════════════════════════════════════════════════════════════════════════
# BATCH MODE
# ═══════════════════════════════════════════════════════════════════════════════

def run_batch(spark: SparkSession, process_date: str):
    """Process a specific date partition in batch mode."""
    logger.info("Batch mode | date=%s", process_date)

    # Stage 1 – Read Bronze partition (predicate pushdown)
    bronze_path = delta_path("bronze", "ohlcv")
    raw_df = (
        spark.read
        .format("delta")
        .load(bronze_path)
        .filter(F.col("partition_date") == process_date)
    )
    logger.info("Loaded %d Bronze rows for %s", raw_df.count(), process_date)

    # Stages 2–5
    df = clean(raw_df)
    df = enrich_indicators(df)
    df = add_patterns_and_metadata(df)

    # Stage 6 – persist (use overwriteSchema for batch reprocessing)
    (
        df
        .write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"partition_date = '{process_date}'")
        .option("mergeSchema", "true")
        .partitionBy("symbol", "partition_date")
        .save(delta_path("silver", "ohlcv"))
    )
    logger.info("Batch Silver processing complete for %s", process_date)


# ═══════════════════════════════════════════════════════════════════════════════
# STREAMING MODE
# ═══════════════════════════════════════════════════════════════════════════════

def run_streaming(spark: SparkSession):
    """Read Bronze streaming source and write to Silver in update mode."""
    logger.info("Streaming mode started...")

    raw_stream = (
        spark.readStream
        .format("delta")
        .option("ignoreChanges", "true")
        .load(delta_path("bronze", "ohlcv"))
        .withWatermark("open_time", WATERMARK_DELAY)
    )

    # Stages 2–5 (same functions, stateless)
    df = clean(raw_stream)
    df = enrich_indicators(df)
    df = add_patterns_and_metadata(df)

    query = (
        df
        .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path("silver_clean"))
        .option("path", delta_path("silver", "ohlcv"))
        .trigger(processingTime=TRIGGER_INTERVAL)
        .partitionBy("symbol", "partition_date")
        .queryName("silver_clean_stream")
        .start()
    )
    query.awaitTermination()


# ─────────────────────────────────────────────
# Entry Point
# ─────────────────────────────────────────────
def parse_args():
    p = argparse.ArgumentParser(description="Bronze → Silver Cleaning Job")
    p.add_argument("--mode", choices=["batch", "streaming"], default="batch")
    p.add_argument("--date", default=str(date.today() - timedelta(days=1)),
                   help="Processing date for batch mode (YYYY-MM-DD)")
    return p.parse_args()


def main():
    args  = parse_args()
    spark = create_spark_session("CryptoAnalytics-Silver-Clean")

    if args.mode == "streaming":
        run_streaming(spark)
    else:
        run_batch(spark, args.date)

    spark.stop()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    )
    main()
