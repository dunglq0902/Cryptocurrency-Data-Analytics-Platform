"""
jobs/alert_evaluator.py
Job 4 – Gold → Alert Signals (Structured Streaming)

Luồng xử lý:
  1. Đọc Gold layer streaming data mỗi 30 giây
  2. Load active alert rules từ MongoDB (broadcast variable)
  3. Join Gold data với alert rules theo symbol + timeframe
  4. Evaluate conditions cho mỗi rule
  5. Apply cooldown check
  6. Generate alert events → Kafka topic alert-events

Alert rule condition example (stored in MongoDB):
  {
    "rule_id": "rule_001",
    "user_id": "user_001",
    "symbol":  "BTCUSDT",
    "timeframe": "1h",
    "logic": "AND",
    "conditions": [
        {"field": "rsi_14",       "operator": "<",  "value": 30},
        {"field": "volume_ratio", "operator": ">",  "value": 1.5},
        {"field": "candle_pattern", "operator": "==", "value": "HAMMER"}
    ],
    "action": "BUY",
    "cooldown_seconds": 300,
    "is_active": true
  }
"""

import json
import logging
import os
import sys
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, BooleanType, StructType, StructField, DoubleType

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.spark_session import create_spark_session, checkpoint_path, delta_path, KAFKA_BOOTSTRAP_SERVERS
from schemas.bronze_schema import ALERT_EVENT_SCHEMA
from udfs.indicator_udfs import udf_format_signal

logger = logging.getLogger("AlertEvaluator")

TRIGGER_INTERVAL   = "30 seconds"
WATERMARK_DELAY    = "2 minutes"
ALERT_TOPIC        = "alert-events"

# MongoDB connection
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB  = os.getenv("MONGO_DB",  "crypto_analytics")
MONGO_COLLECTION = "alert_rules"


# ═══════════════════════════════════════════════════════════════════════════════
# CONDITION EVALUATOR
# ═══════════════════════════════════════════════════════════════════════════════

SUPPORTED_OPERATORS = {">", "<", ">=", "<=", "==", "!=", "crosses_above", "crosses_below"}

SUPPORTED_FIELDS = {
    "close", "open", "high", "low", "volume",
    "rsi_14", "macd", "macd_signal", "macd_hist",
    "ma7", "ma25", "ma99",
    "bb_upper", "bb_lower",
    "atr_14", "volume_ratio",
    "candle_pattern",
    "price_change_pct",
    "vwap",
}


def _evaluate_single_condition(
    row_data: Dict[str, Any],
    condition: Dict[str, Any],
    prev_row:  Optional[Dict[str, Any]] = None,
) -> bool:
    """Evaluate one condition against a row dict."""
    field    = condition.get("field")
    operator = condition.get("operator")
    value    = condition.get("value")

    if field not in SUPPORTED_FIELDS:
        logger.warning("Unknown condition field: %s", field)
        return False
    if operator not in SUPPORTED_OPERATORS:
        logger.warning("Unknown operator: %s", operator)
        return False

    current_val = row_data.get(field)
    if current_val is None:
        return False

    try:
        if operator == ">":
            return float(current_val) > float(value)
        elif operator == "<":
            return float(current_val) < float(value)
        elif operator == ">=":
            return float(current_val) >= float(value)
        elif operator == "<=":
            return float(current_val) <= float(value)
        elif operator == "==":
            # Handle string comparison for candle_pattern
            return str(current_val) == str(value)
        elif operator == "!=":
            return str(current_val) != str(value)
        elif operator == "crosses_above":
            # Requires prev_row
            prev_val = (prev_row or {}).get(field)
            if prev_val is None:
                return False
            return float(prev_val) <= float(value) and float(current_val) > float(value)
        elif operator == "crosses_below":
            prev_val = (prev_row or {}).get(field)
            if prev_val is None:
                return False
            return float(prev_val) >= float(value) and float(current_val) < float(value)
    except (ValueError, TypeError) as exc:
        logger.debug("Condition eval error: %s", exc)
        return False
    return False


def evaluate_rule(
    rule:     Dict[str, Any],
    row_data: Dict[str, Any],
) -> bool:
    """
    Evaluate all conditions of a rule against a data row.
    Supports AND / OR logic between conditions.
    """
    conditions = rule.get("conditions", [])
    logic      = rule.get("logic", "AND").upper()

    if not conditions:
        return False

    results = [_evaluate_single_condition(row_data, cond) for cond in conditions]

    if logic == "AND":
        return all(results)
    elif logic == "OR":
        return any(results)
    return False


# ═══════════════════════════════════════════════════════════════════════════════
# UDF: Rule Evaluation (scalar UDF applied per row after explode)
# ═══════════════════════════════════════════════════════════════════════════════

def _make_rule_eval_udf():
    """
    Factory returning a UDF that evaluates a serialized rule JSON
    against a row's indicator values.
    """
    def _eval(
        rule_json:      Optional[str],
        close:          Optional[float],
        rsi:            Optional[float],
        macd:           Optional[float],
        macd_signal:    Optional[float],
        volume_ratio:   Optional[float],
        candle_pattern: Optional[str],
        ma7:            Optional[float],
        ma25:           Optional[float],
        bb_upper:       Optional[float],
        bb_lower:       Optional[float],
        vwap:           Optional[float],
    ) -> bool:
        if not rule_json:
            return False
        try:
            rule = json.loads(rule_json)
        except json.JSONDecodeError:
            return False

        row_data = {
            "close":          close,
            "rsi_14":         rsi,
            "macd":           macd,
            "macd_signal":    macd_signal,
            "volume_ratio":   volume_ratio,
            "candle_pattern": candle_pattern,
            "ma7":            ma7,
            "ma25":           ma25,
            "bb_upper":       bb_upper,
            "bb_lower":       bb_lower,
            "vwap":           vwap,
        }
        return evaluate_rule(rule, row_data)

    return F.udf(_eval, BooleanType())


rule_eval_udf = _make_rule_eval_udf()


# ═══════════════════════════════════════════════════════════════════════════════
# LOAD ALERT RULES (broadcast variable)
# ═══════════════════════════════════════════════════════════════════════════════

def load_alert_rules(spark: SparkSession) -> DataFrame:
    """
    Load active alert rules from MongoDB.
    Returns a Spark DataFrame suitable for broadcasting.
    """
    logger.info("Loading active alert rules from MongoDB...")

    rules_df = (
        spark.read
        .format("mongodb")
        .option("spark.mongodb.read.connection.uri", MONGO_URI)
        .option("spark.mongodb.read.database",       MONGO_DB)
        .option("spark.mongodb.read.collection",     MONGO_COLLECTION)
        .load()
        .filter(F.col("is_active") == True)        # noqa: E712
        .select(
            F.col("rule_id"),
            F.col("user_id"),
            F.col("symbol"),
            F.col("timeframe"),
            F.col("action"),
            F.col("cooldown_seconds"),
            F.col("last_triggered_at"),
            # Serialize conditions + logic into a single JSON string for UDF
            F.to_json(
                F.struct(
                    F.col("conditions"),
                    F.col("logic"),
                )
            ).alias("rule_json"),
        )
    )

    rule_count = rules_df.count()
    logger.info("Loaded %d active alert rules.", rule_count)
    return rules_df


# ═══════════════════════════════════════════════════════════════════════════════
# ALERT EVALUATION PIPELINE
# ═══════════════════════════════════════════════════════════════════════════════

def evaluate_alerts(
    spark:     SparkSession,
    gold_df:   DataFrame,
    rules_df:  DataFrame,
) -> DataFrame:
    """
    Join Gold data with alert rules, evaluate conditions, apply cooldown,
    and return triggered alert events.
    """
    # Cache rules DataFrame (small) for the duration of the micro-batch
    rules_cached = F.broadcast(rules_df)

    # ── Join Gold data with matching rules (symbol + timeframe) ───────────────
    joined = gold_df.join(
        rules_cached,
        on=["symbol", "timeframe"],
        how="inner",
    )

    # ── Evaluate conditions via UDF ────────────────────────────────────────────
    evaluated = joined.withColumn(
        "condition_met",
        rule_eval_udf(
            F.col("rule_json"),
            F.col("close"),
            F.col("rsi_14"),
            F.col("macd"),
            F.col("macd_signal"),
            F.col("volume_ratio"),
            F.col("candle_pattern"),
            F.col("ma7"),
            F.col("ma25"),
            F.col("bb_upper"),
            F.col("bb_lower"),
            F.col("vwap"),
        )
    ).filter(F.col("condition_met") == True)   # noqa: E712

    # ── Apply cooldown: skip if rule triggered recently ────────────────────────
    now_ts = F.current_timestamp()
    with_cooldown = evaluated.filter(
        F.col("last_triggered_at").isNull()
        | (
            F.unix_timestamp(now_ts) - F.unix_timestamp(F.col("last_triggered_at"))
            > F.col("cooldown_seconds")
        )
    )

    # ── Generate alert events ─────────────────────────────────────────────────
    alert_df = with_cooldown.select(
        F.expr("uuid()").alias("alert_id"),
        F.col("rule_id"),
        F.col("user_id"),
        F.col("symbol"),
        F.col("timeframe"),
        F.col("action"),
        now_ts.alias("triggered_at"),
        F.col("close").alias("close_price"),
        F.col("rsi_14"),
        F.col("macd"),
        F.col("volume_ratio"),
        F.col("candle_pattern"),
        udf_format_signal(
            F.col("symbol"),
            F.col("action"),
            F.col("close"),
            F.col("rsi_14"),
            F.col("macd"),
            F.col("volume_ratio"),
            F.col("candle_pattern"),
            F.col("rule_id"),
        ).alias("message"),
    )

    return alert_df


# ═══════════════════════════════════════════════════════════════════════════════
# KAFKA SINK
# ═══════════════════════════════════════════════════════════════════════════════

def write_alerts_to_kafka(alert_df: DataFrame):
    """Serialize alert events to JSON and write to Kafka alert-events topic."""
    return (
        alert_df
        .select(
            F.col("symbol").alias("key"),
            F.to_json(F.struct(
                "alert_id", "rule_id", "user_id", "symbol", "timeframe",
                "action", "triggered_at", "close_price",
                "rsi_14", "macd", "volume_ratio", "candle_pattern", "message",
            )).alias("value"),
        )
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", ALERT_TOPIC)
        .option("checkpointLocation", checkpoint_path("alert_evaluator_kafka"))
        .trigger(processingTime=TRIGGER_INTERVAL)
        .outputMode("append")
        .queryName("alert_kafka_sink")
        .start()
    )


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN STREAMING JOB
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    spark = create_spark_session(
        "CryptoAnalytics-Alert-Evaluator",
        extra_configs={
            "spark.mongodb.read.connection.uri": MONGO_URI,
        }
    )

    # Load rules once at startup (re-loaded each micro-batch via foreachBatch)
    logger.info("Loading alert rules for broadcast...")
    rules_df = load_alert_rules(spark)

    # Read Gold streaming data
    gold_stream = (
        spark.readStream
        .format("delta")
        .option("ignoreChanges", "true")
        .load(delta_path("gold", "ohlcv"))
        .withWatermark("window_start", WATERMARK_DELAY)
    )

    # Use foreachBatch to reload rules periodically and evaluate
    refresh_counter = [0]

    def process_batch(batch_df: DataFrame, batch_id: int):
        """Called for each micro-batch."""
        nonlocal rules_df

        # Reload rules every 10 batches (≈ 5 minutes) to pick up new rules
        refresh_counter[0] += 1
        if refresh_counter[0] % 10 == 0:
            logger.info("Refreshing alert rules from MongoDB (batch %d)...", batch_id)
            rules_df = load_alert_rules(spark)

        if batch_df.isEmpty():
            return

        # Evaluate alert conditions
        alert_batch = evaluate_alerts(spark, batch_df, rules_df)

        if alert_batch.isEmpty():
            logger.debug("Batch %d: No alerts triggered.", batch_id)
            return

        alert_count = alert_batch.count()
        logger.info("Batch %d: %d alerts triggered.", batch_id, alert_count)

        # Write to Kafka
        (
            alert_batch
            .select(
                F.col("symbol").alias("key"),
                F.to_json(F.struct(
                    "alert_id", "rule_id", "user_id", "symbol", "timeframe",
                    "action", "triggered_at", "close_price",
                    "rsi_14", "macd", "volume_ratio", "candle_pattern", "message",
                )).alias("value"),
            )
            .write
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("topic", ALERT_TOPIC)
            .save()
        )

        # Also write to Delta for audit trail
        (
            alert_batch
            .write
            .format("delta")
            .mode("append")
            .save(delta_path("gold", "alert_events"))
        )

    query = (
        gold_stream
        .writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", checkpoint_path("alert_evaluator"))
        .trigger(processingTime=TRIGGER_INTERVAL)
        .queryName("alert_evaluator")
        .start()
    )

    logger.info("Alert Evaluator streaming query started: %s", query.name)
    query.awaitTermination()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    )
    main()
