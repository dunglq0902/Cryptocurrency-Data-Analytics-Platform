"""
jobs/gold_aggregate.py
Job 3 – Silver → Gold: Complex Aggregations + Window Functions + Multi-Timeframe OHLCV

Thực hiện:
  - OHLCV Resampling: 1m → 5m, 15m, 1h, 4h, 1D
  - Window Functions: MA, Bollinger, Rank, Running Total, Lag/Lead
  - VWAP computation
  - Pivot / Unpivot metric transformation
  - Broadcast join với symbol metadata
  - Sort-merge join với market cap data
  - Z-ordering + OPTIMIZE trên Gold tables

Chạy:
    spark-submit spark/jobs/gold_aggregate.py \
        --mode batch \
        --date 2024-01-15
"""

import argparse
import logging
import os
import sys
from datetime import date, timedelta

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, DoubleType, IntegerType

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.spark_session import create_spark_session, checkpoint_path, delta_path

logger = logging.getLogger("GoldAggregate")

TRIGGER_INTERVAL = "15 minutes"
WATERMARK_DELAY  = "5 minutes"

# Resample target intervals → seconds
RESAMPLE_INTERVALS = {
    "5m":  5  * 60,
    "15m": 15 * 60,
    "1h":  60 * 60,
    "4h":  4  * 60 * 60,
    "1d":  24 * 60 * 60,
}


# ═══════════════════════════════════════════════════════════════════════════════
# OHLCV RESAMPLING (1m → higher timeframes)
# ═══════════════════════════════════════════════════════════════════════════════

def resample_ohlcv(df: DataFrame, interval_name: str, interval_seconds: int) -> DataFrame:
    """
    Aggregate 1-minute OHLCV candles into a higher timeframe.
    Uses F.window() for time-based grouping (Spark Structured Streaming compatible).
    """
    duration_str = _seconds_to_spark_duration(interval_seconds)

    agg_df = (
        df
        .groupBy(
            "symbol",
            F.window(F.col("open_time"), duration_str).alias("time_window"),
        )
        .agg(
            # OHLCV
            F.first("open",    ignorenulls=True).alias("open"),
            F.max("high").alias("high"),
            F.min("low").alias("low"),
            F.last("close",   ignorenulls=True).alias("close"),
            F.sum("volume").alias("volume"),
            F.sum("quote_volume").alias("quote_volume"),
            F.sum("trade_count").cast(LongType()).alias("trade_count"),
            # Carry through latest indicators (last non-null)
            F.last("ma7",         ignorenulls=True).alias("ma7"),
            F.last("ma25",        ignorenulls=True).alias("ma25"),
            F.last("ma99",        ignorenulls=True).alias("ma99"),
            F.last("rsi_14",      ignorenulls=True).alias("rsi_14"),
            F.last("macd",        ignorenulls=True).alias("macd"),
            F.last("macd_signal", ignorenulls=True).alias("macd_signal"),
            F.last("bb_upper",    ignorenulls=True).alias("bb_upper"),
            F.last("bb_lower",    ignorenulls=True).alias("bb_lower"),
            F.last("atr_14",      ignorenulls=True).alias("atr_14"),
            F.last("candle_pattern", ignorenulls=True).alias("candle_pattern"),
            # percentile_approx for statistical analysis
            F.percentile_approx("close", 0.5).alias("median_close"),
            F.percentile_approx("volume", [0.25, 0.75]).alias("volume_quartiles"),
        )
        .select(
            "symbol",
            F.lit(interval_name).alias("timeframe"),
            F.col("time_window.start").alias("window_start"),
            F.col("time_window.end").alias("window_end"),
            "open", "high", "low", "close", "volume", "quote_volume",
            "trade_count", "ma7", "ma25", "ma99", "rsi_14", "macd",
            "macd_signal", "bb_upper", "bb_lower", "atr_14",
            "candle_pattern", "median_close", "volume_quartiles",
        )
    )
    return agg_df


def _seconds_to_spark_duration(seconds: int) -> str:
    if seconds % 3600 == 0:
        return f"{seconds // 3600} hours"
    elif seconds % 60 == 0:
        return f"{seconds // 60} minutes"
    return f"{seconds} seconds"


# ═══════════════════════════════════════════════════════════════════════════════
# WINDOW FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════

def apply_window_functions(df: DataFrame) -> DataFrame:
    """
    Apply advanced window functions on the aggregated Gold data:
      - VWAP (volume-weighted average price)
      - Price rank per timeframe window
      - Running (cumulative) volume
      - Lag/Lead for trend detection
    """
    logger.info("Applying Gold window functions...")

    sym_tf_win = (
        Window
        .partitionBy("symbol", "timeframe")
        .orderBy("window_start")
    )

    # ── VWAP ──────────────────────────────────────────────────────────────────
    # VWAP = sum(typical_price * volume) / sum(volume) over rolling window
    vwap_win = sym_tf_win.rowsBetween(-20, 0)
    df = df.withColumn(
        "_tp",   (F.col("high") + F.col("low") + F.col("close")) / 3
    )
    df = df.withColumn(
        "_tp_vol", F.col("_tp") * F.col("volume")
    )
    df = df.withColumn(
        "vwap",
        F.sum("_tp_vol").over(vwap_win) / F.sum("volume").over(vwap_win)
    ).drop("_tp", "_tp_vol")

    # ── Rank symbols by volume within each time window ─────────────────────────
    ts_win = Window.partitionBy("timeframe", "window_start").orderBy(F.col("volume").desc())
    df = df.withColumn("price_rank", F.rank().over(ts_win).cast(IntegerType()))

    # ── Dense rank ────────────────────────────────────────────────────────────
    df = df.withColumn(
        "volume_dense_rank",
        F.dense_rank().over(ts_win).cast(IntegerType())
    )

    # ── Running (cumulative) volume ────────────────────────────────────────────
    cumulative_win = sym_tf_win.rowsBetween(Window.unboundedPreceding, 0)
    df = df.withColumn("cumulative_volume", F.sum("volume").over(cumulative_win))

    # ── Lag / Lead – prior and next close for momentum signals ────────────────
    df = (
        df
        .withColumn("prev_close",  F.lag("close",  1).over(sym_tf_win))
        .withColumn("next_close",  F.lead("close", 1).over(sym_tf_win))
        .withColumn("prev2_close", F.lag("close",  2).over(sym_tf_win))
    )

    # ── Volume ratio ─────────────────────────────────────────────────────────
    vol_win20 = sym_tf_win.rowsBetween(-19, 0)
    df = (
        df
        .withColumn("avg_volume_20", F.avg("volume").over(vol_win20))
        .withColumn(
            "volume_ratio",
            F.when(
                F.col("avg_volume_20").isNotNull() & (F.col("avg_volume_20") != 0),
                F.col("volume") / F.col("avg_volume_20"),
            ).otherwise(None)
        )
    )

    return df


# ═══════════════════════════════════════════════════════════════════════════════
# PIVOT / UNPIVOT
# ═══════════════════════════════════════════════════════════════════════════════

def pivot_metrics(df: DataFrame) -> DataFrame:
    """
    Pivot: Tạo wide-format DataFrame với mỗi metric là một cột per symbol.
    Useful for multi-symbol comparison dashboards.
    Returns a new DataFrame; original Gold table stays unpivoted.
    """
    logger.info("Pivoting metrics by symbol...")

    pivoted = (
        df
        .filter(F.col("timeframe") == "1h")   # Pivot hourly for dashboard
        .groupBy("window_start")
        .pivot("symbol")                        # Tạo cột cho mỗi symbol
        .agg(F.last("close").alias("close"))
    )
    return pivoted


def unpivot_metrics(df: DataFrame) -> DataFrame:
    """
    Unpivot: Chuyển wide-format (col per metric) sang long-format
    (metric, value) để dễ filter và aggregate.
    """
    logger.info("Unpivoting metrics to long format...")

    metric_cols = ["open", "high", "low", "close", "volume", "rsi_14", "macd"]

    # Build stack expression:  stack(N, 'name1', col1, 'name2', col2, ...)
    stack_expr = f"stack({len(metric_cols)}, " + ", ".join(
        f"'{c}', `{c}`" for c in metric_cols
    ) + ") as (metric_name, metric_value)"

    long_df = df.select(
        "symbol", "timeframe", "window_start",
        F.expr(stack_expr),
    ).filter(F.col("metric_value").isNotNull())

    return long_df


# ═══════════════════════════════════════════════════════════════════════════════
# JOIN OPTIMIZATIONS
# ═══════════════════════════════════════════════════════════════════════════════

def build_symbol_metadata(spark: SparkSession) -> DataFrame:
    """
    Small reference table for symbol metadata.
    In production this would come from a DB / API; here we build inline.
    """
    data = [
        ("BTCUSDT",  "Bitcoin",  "BTC", "Proof-of-Work", 1),
        ("ETHUSDT",  "Ethereum", "ETH", "Proof-of-Stake", 2),
        ("BNBUSDT",  "BNB",      "BNB", "BNB Chain", 3),
        ("SOLUSDT",  "Solana",   "SOL", "PoH+PoS", 4),
        ("XRPUSDT",  "XRP",      "XRP", "Federated Consensus", 5),
    ]
    return spark.createDataFrame(data, ["symbol", "name", "base_asset", "consensus", "cmc_rank"])


def join_with_metadata(spark: SparkSession, gold_df: DataFrame) -> DataFrame:
    """
    Broadcast join: Gold (large) ⋈ symbol_metadata (small < 10 MB).
    Spark AQE will also auto-broadcast; this is explicit for documentation.
    """
    logger.info("Broadcast join with symbol metadata...")

    metadata_df = build_symbol_metadata(spark)
    # Explicit broadcast hint
    result = gold_df.join(F.broadcast(metadata_df), on="symbol", how="left")
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# PERSIST GOLD
# ═══════════════════════════════════════════════════════════════════════════════

def persist_gold(df: DataFrame, process_date: str, mode: str = "overwrite"):
    """Write Gold Delta table with Z-ordering on symbol + window_start."""
    gold_path = delta_path("gold", "ohlcv")

    logger.info("Writing Gold Delta table → %s", gold_path)

    # Add partition date
    df_out = (
        df
        .withColumn("partition_date", F.to_date(F.col("window_start")))
        .withColumn("aggregated_at",  F.current_timestamp())
    )

    (
        df_out
        .write
        .format("delta")
        .mode(mode)
        .option("replaceWhere", f"partition_date = '{process_date}'")
        .option("mergeSchema", "true")
        .partitionBy("symbol", "timeframe", "partition_date")
        .save(gold_path)
    )

    # Z-order for selective queries on symbol + window_start
    spark_ref = SparkSession.getActiveSession()
    if spark_ref:
        try:
            spark_ref.sql(f"""
                OPTIMIZE delta.`{gold_path}`
                ZORDER BY (symbol, window_start)
            """)
            logger.info("OPTIMIZE + ZORDER applied to Gold table.")
        except Exception as exc:            # pylint: disable=broad-except
            logger.warning("OPTIMIZE failed (non-critical): %s", exc)


# ═══════════════════════════════════════════════════════════════════════════════
# BATCH ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════

def run_batch(spark: SparkSession, process_date: str):
    logger.info("Gold Aggregation | batch | date=%s", process_date)

    # Stage 1 – Read Silver for this date
    silver_df = (
        spark.read
        .format("delta")
        .load(delta_path("silver", "ohlcv"))
        .filter(F.col("partition_date") == process_date)
        # Column pruning – only columns needed for Gold
        .select(
            "symbol", "open_time", "open", "high", "low", "close",
            "volume", "quote_volume", "trade_count",
            "ma7", "ma25", "ma99", "rsi_14",
            "macd", "macd_signal", "bb_upper", "bb_lower", "atr_14",
            "candle_pattern", "volume_ratio",
        )
        # Predicate pushdown already applied above; cache here as it's used 5×
        .cache()
    )

    silver_count = silver_df.count()
    logger.info("Silver rows loaded: %d", silver_count)

    all_frames = []

    # Stage 2 – Resample to each higher timeframe
    for interval_name, interval_seconds in RESAMPLE_INTERVALS.items():
        logger.info("Resampling to %s...", interval_name)
        resampled = resample_ohlcv(silver_df, interval_name, interval_seconds)
        all_frames.append(resampled)

    # Union all timeframes
    from functools import reduce
    gold_df = reduce(DataFrame.unionByName, all_frames, spark.createDataFrame([], all_frames[0].schema))

    # Stage 3 – Window functions
    gold_df = apply_window_functions(gold_df)

    # Stage 4 – Broadcast join with metadata
    gold_df = join_with_metadata(spark, gold_df)

    # Unpersist Silver cache
    silver_df.unpersist()

    # Stage 5 – Persist
    persist_gold(gold_df, process_date)

    # Generate pivot for dashboard (side write)
    pivoted = pivot_metrics(gold_df)
    (
        pivoted
        .write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"date(window_start) = '{process_date}'")
        .save(delta_path("gold", "ohlcv_pivot"))
    )

    logger.info("Gold batch aggregation complete for %s", process_date)


# ═══════════════════════════════════════════════════════════════════════════════
# STREAMING ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════

def run_streaming(spark: SparkSession):
    """
    Stream Silver data continuously and aggregate to Gold.
    Uses complete output mode for aggregations.
    """
    logger.info("Gold Aggregation | streaming mode...")

    silver_stream = (
        spark.readStream
        .format("delta")
        .option("ignoreChanges", "true")
        .load(delta_path("silver", "ohlcv"))
        .withWatermark("open_time", WATERMARK_DELAY)
    )

    # Resample to 5m and 1h in streaming mode (append-friendly)
    agg_5m = resample_ohlcv(silver_stream, "5m", RESAMPLE_INTERVALS["5m"])
    agg_1h = resample_ohlcv(silver_stream, "1h", RESAMPLE_INTERVALS["1h"])

    def start_sink(agg_df: DataFrame, name: str):
        return (
            agg_df
            .withColumn("partition_date", F.to_date(F.col("window_start")))
            .withColumn("aggregated_at",  F.current_timestamp())
            .writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpoint_path(f"gold_{name}"))
            .option("path", delta_path("gold", "ohlcv"))
            .trigger(processingTime=TRIGGER_INTERVAL)
            .queryName(f"gold_{name}_stream")
            .start()
        )

    q5m = start_sink(agg_5m, "5m")
    q1h = start_sink(agg_1h, "1h")

    logger.info("Gold streaming queries active: %s, %s", q5m.name, q1h.name)
    spark.streams.awaitAnyTermination()


# ─────────────────────────────────────────────
# Entry Point
# ─────────────────────────────────────────────
def parse_args():
    p = argparse.ArgumentParser(description="Silver → Gold Aggregation Job")
    p.add_argument("--mode", choices=["batch", "streaming"], default="batch")
    p.add_argument("--date", default=str(date.today() - timedelta(days=1)))
    return p.parse_args()


def main():
    args  = parse_args()
    spark = create_spark_session("CryptoAnalytics-Gold-Aggregate")

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
