"""
Gold Layer — Daily Aggregates

Pre-computed business aggregates from Silver normalized transactions.
Designed for executive dashboards and operational monitoring.

Tables:
    gold.daily_summary       — Daily KPIs by date, region, channel
    gold.merchant_activity   — Merchant performance metrics
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ..ingestion import config

logger = logging.getLogger(__name__)


class GoldAggregator:
    """Builds Gold-layer aggregate tables from Silver normalized data."""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def build_daily_summary(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> int:
        """Build or refresh the daily summary table.

        Returns row count.
        """
        silver = self._read_silver(start_date, end_date)

        daily = (
            silver.filter(F.col("_quality_flag") == "PASS")
            .groupBy("_date", "region", "channel", "_is_weekend", "_is_peak_hour")
            .agg(
                F.count("*").alias("transaction_count"),
                F.sum("amount").alias("total_volume_xof"),
                F.avg("amount").alias("avg_transaction_xof"),
                F.median("amount").alias("median_transaction_xof"),
                F.sum("fee").alias("total_fees_xof"),
                F.count(F.when(F.col("_is_merchant_txn"), 1)).alias("merchant_txn_count"),
                F.count(F.when(F.col("status") == "FAILED", 1)).alias("failed_count"),
                F.count(F.when(F.col("status") == "REVERSED", 1)).alias("reversed_count"),
                F.countDistinct("merchant_code").alias("unique_merchants"),
                F.sum(F.when(F.col("_is_merchant_txn"), F.col("amount"))).alias(
                    "merchant_volume_xof"
                ),
                F.avg("_processing_latency_seconds").alias("avg_latency_seconds"),
            )
            .withColumn(
                "success_rate",
                F.round(
                    (F.col("transaction_count") - F.col("failed_count"))
                    / F.col("transaction_count")
                    * 100,
                    2,
                ),
            )
            .withColumn(
                "merchant_share_pct",
                F.round(F.col("merchant_txn_count") / F.col("transaction_count") * 100, 2),
            )
            .withColumn("_updated_at", F.current_timestamp())
        )

        daily.write.mode("overwrite").format("delta").partitionBy("_date").saveAsTable(
            config.gold_daily_path
        )

        row_count = daily.count()
        logger.info(f"Gold daily summary: {row_count} rows written")
        return row_count

    def build_merchant_activity(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> int:
        """Build or refresh the merchant activity table.

        Returns row count.
        """
        silver = self._read_silver(start_date, end_date)

        merchant = (
            silver.filter(F.col("_is_merchant_txn") & (F.col("_quality_flag") == "PASS"))
            .groupBy("merchant_code", "merchant_name", "merchant_category", "_date")
            .agg(
                F.count("*").alias("transaction_count"),
                F.sum("amount").alias("total_volume_xof"),
                F.avg("amount").alias("avg_transaction_xof"),
                F.sum("fee").alias("total_fees_xof"),
                F.countDistinct("sender_phone").alias("unique_customers"),
                F.count(F.when(F.col("status") == "FAILED", 1)).alias("failed_count"),
            )
            .withColumn(
                "success_rate",
                F.round(
                    (F.col("transaction_count") - F.col("failed_count"))
                    / F.col("transaction_count")
                    * 100,
                    2,
                ),
            )
            .withColumn(
                "avg_txn_per_customer",
                F.round(F.col("transaction_count") / F.col("unique_customers"), 2),
            )
            .withColumn("_updated_at", F.current_timestamp())
        )

        merchant.write.mode("overwrite").format("delta").partitionBy("_date").saveAsTable(
            config.gold_merchant_path
        )

        row_count = merchant.count()
        logger.info(f"Gold merchant activity: {row_count} rows written")
        return row_count

    def build_all(self, days_back: int = 30) -> dict:
        """Build all Gold tables for the last N days."""
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=days_back)

        daily_rows = self.build_daily_summary(start_date, end_date)
        merchant_rows = self.build_merchant_activity(start_date, end_date)

        return {
            "daily_summary_rows": daily_rows,
            "merchant_activity_rows": merchant_rows,
        }

    def _read_silver(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> DataFrame:
        """Read from Silver with optional date filtering."""
        df = self.spark.table(config.silver_path)
        if start_date:
            df = df.filter(F.col("_date") >= start_date.date())
        if end_date:
            df = df.filter(F.col("_date") <= end_date.date())
        return df


# ═══════════════════════════════════════════════════════════════════
# Convenience Function
# ═══════════════════════════════════════════════════════════════════


def aggregate_daily(spark: SparkSession, days_back: int = 30) -> dict:
    """Quick daily aggregation: rebuild Gold tables."""
    aggregator = GoldAggregator(spark)
    return aggregator.build_all(days_back)
