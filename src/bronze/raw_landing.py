"""
Bronze Layer — Raw Transaction Landing

Ingests raw Orange Money transaction data into the Bronze Delta table.
Append-only, schema-on-read, preserves full fidelity of source data.

Architecture:
    Orange Money API / Mock Generator
        → Transaction Pydantic models
        → Spark DataFrame
        → Delta Lake (bronze.raw_transactions)
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from ..ingestion import OrangeMoneyClient, TransactionPage, config

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════
# Schema Definition
# ═══════════════════════════════════════════════════════════════════

BRONZE_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), False),
        StructField("external_id", StringType(), True),
        StructField("transaction_type", StringType(), False),
        StructField("amount", DoubleType(), False),
        StructField("currency", StringType(), False),
        StructField("fee", DoubleType(), False),
        StructField("sender_phone", StringType(), True),  # Tokenized if CDP enabled
        StructField("recipient_phone", StringType(), True),  # Tokenized if CDP enabled
        StructField("merchant_code", StringType(), True),
        StructField("merchant_name", StringType(), True),
        StructField("merchant_category", StringType(), True),
        StructField("status", StringType(), False),
        StructField("failure_reason", StringType(), True),
        StructField("initiated_at", TimestampType(), False),
        StructField("completed_at", TimestampType(), True),
        StructField("channel", StringType(), False),
        StructField("agent_code", StringType(), True),
        StructField("region", StringType(), True),
        StructField("raw_payload", StringType(), True),  # JSON string
        # Audit columns
        StructField("_ingested_at", TimestampType(), False),
        StructField("_source_file", StringType(), False),
        StructField("_batch_id", StringType(), False),
    ]
)


# ═══════════════════════════════════════════════════════════════════
# Bronze Ingestion
# ═══════════════════════════════════════════════════════════════════


class BronzeIngestor:
    """Ingests Orange Money transactions into the Bronze Delta table.

    Usage:
        spark = SparkSession.builder.getOrCreate()
        ingestor = BronzeIngestor(spark)
        ingestor.ingest_date_range(start_date, end_date)
    """

    def __init__(self, spark: SparkSession, client: OrangeMoneyClient | None = None):
        self.spark = spark
        self.client = client or OrangeMoneyClient()
        self._batch_id: str | None = None

    def ingest_date_range(
        self,
        start_date: datetime,
        end_date: datetime,
        batch_size: int | None = None,
    ) -> int:
        """Ingest transactions for a date range into Bronze.

        Returns the number of rows ingested.
        """
        batch_size = batch_size or config.batch_size
        total_rows = 0

        logger.info(f"Ingesting {start_date.date()} → {end_date.date()} into Bronze")

        for page in self.client.fetch_transactions(start_date, end_date, batch_size):
            if not page.transactions:
                continue

            batch_id = self._get_batch_id()
            df = self._transactions_to_dataframe(page, batch_id)

            # Write to Delta — append only, Bronze never updates
            df.write.mode("append").format("delta").option("mergeSchema", "true").saveAsTable(
                config.bronze_path
            )

            total_rows += len(page.transactions)
            logger.debug(f"  Batch {batch_id}: {len(page.transactions)} rows written")

        logger.info(f"Bronze ingestion complete: {total_rows} total rows")
        return total_rows

    def _get_batch_id(self) -> str:
        """Generate a unique batch ID for lineage tracking."""
        import uuid

        return f"bronze-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:8]}"

    def _transactions_to_dataframe(self, page: TransactionPage, batch_id: str) -> DataFrame:
        """Convert a page of Transaction models to a Spark DataFrame."""
        import json as _json

        rows = []
        now = datetime.utcnow()

        for tx in page.transactions:
            rows.append(
                {
                    "transaction_id": tx.transaction_id,
                    "external_id": tx.external_id,
                    "transaction_type": tx.transaction_type,
                    "amount": tx.amount,
                    "currency": tx.currency,
                    "fee": tx.fee,
                    "sender_phone": tx.sender_phone,
                    "recipient_phone": tx.recipient_phone,
                    "merchant_code": tx.merchant_code,
                    "merchant_name": tx.merchant_name,
                    "merchant_category": tx.merchant_category,
                    "status": tx.status,
                    "failure_reason": tx.failure_reason,
                    "initiated_at": tx.initiated_at,
                    "completed_at": tx.completed_at,
                    "channel": tx.channel,
                    "agent_code": tx.agent_code,
                    "region": tx.region,
                    "raw_payload": _json.dumps(tx.raw_payload) if tx.raw_payload else None,
                    "_ingested_at": now,
                    "_source_file": (
                        f"orange-money-api/mock-{tx.initiated_at.strftime('%Y%m%d')}.json"
                    ),
                    "_batch_id": batch_id,
                }
            )

        return self.spark.createDataFrame(rows, schema=BRONZE_SCHEMA)

    def get_latest_ingestion_date(self) -> datetime | None:
        """Get the most recent transaction date in Bronze.

        Used for incremental ingestion: fetch only data after this date.
        """
        try:
            table = self.spark.table(config.bronze_path)
            latest = table.select(F.max("initiated_at").alias("max_date")).collect()[0]
            return latest["max_date"] if latest["max_date"] else None
        except Exception:
            return None


# ═══════════════════════════════════════════════════════════════════
# Convenience Function
# ═══════════════════════════════════════════════════════════════════


def ingest_daily(
    spark: SparkSession,
    days_back: int = 1,
    client: OrangeMoneyClient | None = None,
) -> int:
    """Quick daily ingestion: ingest yesterday's transactions into Bronze."""
    end_date = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    start_date = end_date - timedelta(days=days_back)
    ingestor = BronzeIngestor(spark, client)
    return ingestor.ingest_date_range(start_date, end_date)
