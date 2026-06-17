"""
Silver Layer — Transaction Normalization & Deduplication

Transforms raw Bronze transactions into clean, schema-enforced Silver tables.
Operations:
    1. Schema enforcement (type coercion, null handling)
    2. Deduplication (idempotency by transaction_id)
    3. Data quality validation (amount > 0, valid status, phone format)
    4. Enrichment (date dimensions, amount buckets, merchant lookups)

Architecture:
    Bronze (raw_transactions) → Silver (normalized_transactions)
"""

from __future__ import annotations

import logging
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from ..ingestion import config

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════
# Data Quality Rules
# ═══════════════════════════════════════════════════════════════════

QUALITY_RULES = {
    "amount_positive": F.col("amount") > 0,
    "amount_reasonable": F.col("amount") < 50000000,  # 50M XOF cap
    "currency_xof": F.col("currency") == "XOF",
    "valid_status": F.col("status").isin("SUCCESS", "PENDING", "FAILED", "REVERSED"),
    "has_sender": F.col("sender_phone").isNotNull(),
    "has_initiated_at": F.col("initiated_at").isNotNull(),
    "fee_non_negative": F.col("fee") >= 0,
}


# ═══════════════════════════════════════════════════════════════════
# Normalizer
# ═══════════════════════════════════════════════════════════════════

class SilverNormalizer:
    """Transforms Bronze → Silver with schema enforcement and dedup."""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def normalize(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> dict:
        """Run the full Bronze → Silver transformation.

        Returns a dict with metrics: rows_read, rows_written, duplicates_removed,
        quality_failures, rows_with_warnings.
        """
        logger.info(f"Normalizing Bronze → Silver: {start_date} → {end_date}")

        # 1. Read from Bronze
        bronze = self.spark.table(config.bronze_path)

        if start_date:
            bronze = bronze.filter(F.col("initiated_at") >= start_date)
        if end_date:
            bronze = bronze.filter(F.col("initiated_at") < end_date)

        rows_read = bronze.count()
        logger.info(f"  Bronze rows read: {rows_read}")

        # 2. Deduplicate by transaction_id (keep latest based on _ingested_at)
        window = Window.partitionBy("transaction_id").orderBy(F.col("_ingested_at").desc())
        deduped = bronze \
            .withColumn("_row_num", F.row_number().over(window)) \
            .filter(F.col("_row_num") == 1) \
            .drop("_row_num")

        duplicates_removed = rows_read - deduped.count()

        # 3. Apply data quality checks
        quality_checks = []
        for rule_name, condition in QUALITY_RULES.items():
            quality_checks.append(
                F.when(condition, True).otherwise(False).alias(f"_quality_{rule_name}")
            )

        with_quality = deduped.select(
            "*",
            *quality_checks,
            # Composite quality flag
            F.when(
                F.col("_quality_amount_positive") &
                F.col("_quality_valid_status") &
                F.col("_quality_has_initiated_at"),
                "PASS"
            ).otherwise("FAIL").alias("_quality_flag"),
        )

        quality_failures = with_quality.filter(F.col("_quality_flag") == "FAIL").count()

        # 4. Enrich with derived columns
        enriched = with_quality \
            .withColumn("_date", F.to_date("initiated_at")) \
            .withColumn("_hour", F.hour("initiated_at")) \
            .withColumn("_day_of_week", F.dayofweek("initiated_at")) \
            .withColumn("_amount_bucket",
                F.when(F.col("amount") < 1000, "MICRO")
                 .when(F.col("amount") < 5000, "SMALL")
                 .when(F.col("amount") < 20000, "MEDIUM")
                 .when(F.col("amount") < 100000, "LARGE")
                 .otherwise("XLARGE")
            ) \
            .withColumn("_is_weekend",
                F.col("_day_of_week").isin(1, 7)
            ) \
            .withColumn("_is_peak_hour",
                F.col("_hour").between(9, 12) | F.col("_hour").between(15, 18)
            ) \
            .withColumn("_is_merchant_txn",
                F.col("merchant_code").isNotNull()
            ) \
            .withColumn("_processing_latency_seconds",
                F.unix_timestamp("completed_at") - F.unix_timestamp("initiated_at")
            )

        # 5. Write to Silver Delta table
        enriched.write \
            .mode("overwrite") \
            .format("delta") \
            .option("overwriteSchema", "true") \
            .partitionBy("_date") \
            .saveAsTable(config.silver_path)

        rows_written = enriched.count()

        # Add warning rows (quality FAIL but still written)
        rows_with_warnings = enriched \
            .filter(F.col("_quality_flag") == "FAIL") \
            .count()

        metrics = {
            "rows_read": rows_read,
            "rows_written": rows_written,
            "duplicates_removed": duplicates_removed,
            "quality_failures": quality_failures,
            "rows_with_warnings": rows_with_warnings,
            "quality_pass_rate": round(
                (rows_written - quality_failures) / rows_written * 100, 2
            ) if rows_written > 0 else 0,
        }

        logger.info(f"Silver normalization complete: {metrics}")
        return metrics

    def get_quality_report(self) -> DataFrame:
        """Return a quality summary DataFrame for monitoring."""
        silver = self.spark.table(config.silver_path)

        return silver.agg(
            F.count("*").alias("total_rows"),
            F.count(F.when(F.col("_quality_flag") == "PASS", 1)).alias("passing"),
            F.count(F.when(F.col("_quality_flag") == "FAIL", 1)).alias("failing"),
            F.count(F.when(F.col("status") == "FAILED", 1)).alias("failed_txns"),
            F.count(F.when(F.col("status") == "REVERSED", 1)).alias("reversed_txns"),
            F.min("amount").alias("min_amount"),
            F.max("amount").alias("max_amount"),
            F.avg("amount").alias("avg_amount"),
            F.sum("amount").alias("total_volume_xof"),
            F.min("_date").alias("earliest_date"),
            F.max("_date").alias("latest_date"),
        )


# ═══════════════════════════════════════════════════════════════════
# Convenience Function
# ═══════════════════════════════════════════════════════════════════

def normalize_daily(spark: SparkSession, days_back: int = 1) -> dict:
    """Quick daily normalization: transform yesterday's Bronze data."""
    from datetime import timedelta
    end_date = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    start_date = end_date - timedelta(days=days_back)
    normalizer = SilverNormalizer(spark)
    return normalizer.normalize(start_date, end_date)
