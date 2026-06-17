"""
Gold Layer — Fraud & Anomaly Pattern Detection

Identifies suspicious transaction patterns in the Silver layer.
Designed as a scheduled job that runs after daily normalization.

Patterns detected:
    1. Velocity anomalies — unusual transaction frequency per sender
    2. Amount spikes — transactions far above sender's moving average
    3. Rapid transfers — multiple transfers to same recipient in short window
    4. Off-hours anomalies — large transactions outside peak hours
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)

# Thresholds (configurable)
VELOCITY_THRESHOLD = 20       # transactions per day per sender
AMOUNT_SPIKE_FACTOR = 5.0     # multiplier over 7-day moving average
OFF_HOURS_LARGE_AMOUNT = 100000  # XOF — large transaction during off-hours
RAPID_TRANSFER_WINDOW_MINUTES = 10


class FraudDetector:
    """Detects anomalous transaction patterns in Silver data."""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def detect_all(self, target_date: datetime | None = None) -> DataFrame:
        """Run all fraud detection patterns for a target date.

        Returns a DataFrame of flagged transactions with pattern labels.
        """
        if target_date is None:
            target_date = datetime.utcnow() - timedelta(days=1)

        silver = self.spark.table("orange_money.silver.normalized_transactions")
        day_data = silver.filter(
            (F.col("_date") == target_date.date()) &
            (F.col("_quality_flag") == "PASS")
        )

        # Run each pattern
        velocity_flags = self._detect_velocity_anomaly(day_data, silver)
        amount_flags = self._detect_amount_spikes(day_data, silver)
        rapid_flags = self._detect_rapid_transfers(day_data)
        off_hours_flags = self._detect_off_hours_large(day_data)

        # Combine all flags
        all_flags = velocity_flags \
            .unionByName(amount_flags, allowMissingColumns=True) \
            .unionByName(rapid_flags, allowMissingColumns=True) \
            .unionByName(off_hours_flags, allowMissingColumns=True) \
            .dropDuplicates(["transaction_id"]) \
            .withColumn("_detected_at", F.current_timestamp())

        count = all_flags.count()
        logger.info(f"Fraud detection complete: {count} flags raised for {target_date.date()}")
        return all_flags

    def _detect_velocity_anomaly(
        self, day_data: DataFrame, full_silver: DataFrame
    ) -> DataFrame:
        """Detect senders with unusually high transaction volume."""
        sender_counts = day_data.groupBy("sender_phone").agg(
            F.count("*").alias("daily_txn_count")
        ).filter(F.col("daily_txn_count") > VELOCITY_THRESHOLD)

        return day_data.join(sender_counts, "sender_phone") \
            .select(
                "transaction_id", "sender_phone", "recipient_phone",
                "amount", "initiated_at", "channel", "region",
                F.lit("VELOCITY_ANOMALY").alias("flag_pattern"),
                F.col("daily_txn_count").alias("flag_detail_1"),
                F.lit(None).cast("double").alias("flag_detail_2"),
            )

    def _detect_amount_spikes(
        self, day_data: DataFrame, full_silver: DataFrame
    ) -> DataFrame:
        """Detect transactions significantly above sender's 7-day average."""
        seven_days_ago = datetime.utcnow() - timedelta(days=7)
        historical = full_silver.filter(
            (F.col("initiated_at") >= seven_days_ago) &
            (F.col("_quality_flag") == "PASS")
        )

        sender_avg = historical.groupBy("sender_phone").agg(
            F.avg("amount").alias("avg_amount_7d")
        )

        return day_data \
            .join(sender_avg, "sender_phone") \
            .filter(F.col("amount") > F.col("avg_amount_7d") * AMOUNT_SPIKE_FACTOR) \
            .select(
                "transaction_id", "sender_phone", "recipient_phone",
                "amount", "initiated_at", "channel", "region",
                F.lit("AMOUNT_SPIKE").alias("flag_pattern"),
                F.round(F.col("amount"), 2).cast("string").alias("flag_detail_1"),
                F.round(F.col("avg_amount_7d"), 2).alias("flag_detail_2"),
            )

    def _detect_rapid_transfers(self, day_data: DataFrame) -> DataFrame:
        """Detect multiple transfers to same recipient within a short window."""
        window_spec = Window.partitionBy(
            "sender_phone", "recipient_phone"
        ).orderBy("initiated_at")

        rapid = day_data \
            .filter(F.col("transaction_type") == "TRANSFER") \
            .withColumn("prev_txn_time", F.lag("initiated_at").over(window_spec)) \
            .withColumn(
                "minutes_since_prev",
                F.when(
                    F.col("prev_txn_time").isNotNull(),
                    (F.unix_timestamp("initiated_at") -
                     F.unix_timestamp("prev_txn_time")) / 60
                )
            ) \
            .filter(F.col("minutes_since_prev") < RAPID_TRANSFER_WINDOW_MINUTES)

        return rapid.select(
            "transaction_id", "sender_phone", "recipient_phone",
            "amount", "initiated_at", "channel", "region",
            F.lit("RAPID_TRANSFER").alias("flag_pattern"),
            F.round(F.col("minutes_since_prev"), 1).cast("string").alias("flag_detail_1"),
            F.lit(None).cast("double").alias("flag_detail_2"),
        )

    def _detect_off_hours_large(self, day_data: DataFrame) -> DataFrame:
        """Detect large transactions outside peak business hours."""
        off_hours = day_data \
            .filter(
                (~F.col("_is_peak_hour")) &
                (F.col("amount") >= OFF_HOURS_LARGE_AMOUNT)
            )

        return off_hours.select(
            "transaction_id", "sender_phone", "recipient_phone",
            "amount", "initiated_at", "channel", "region",
            F.lit("OFF_HOURS_LARGE").alias("flag_pattern"),
            F.col("_hour").cast("string").alias("flag_detail_1"),
            F.col("amount").alias("flag_detail_2"),
        )
