"""Tests for Bronze ingestion layer."""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from src.bronze.raw_landing import BRONZE_SCHEMA, BronzeIngestor
from src.ingestion.orange_money_client import Transaction, TransactionPage


class TestBronzeSchema:
    """Schema validation tests."""

    def test_schema_fields(self):
        field_names = [f.name for f in BRONZE_SCHEMA.fields]
        assert "transaction_id" in field_names
        assert "amount" in field_names
        assert "initiated_at" in field_names
        assert "sender_phone" in field_names
        assert "status" in field_names
        # Audit columns
        assert "_ingested_at" in field_names
        assert "_source_file" in field_names
        assert "_batch_id" in field_names


class TestBronzeIngestor:
    """Bronze ingestion tests (with mocked Spark)."""

    @pytest.fixture
    def mock_spark(self):
        spark = MagicMock()
        df = MagicMock()
        spark.createDataFrame.return_value = df
        spark.table.return_value = df
        return spark

    @pytest.fixture
    def mock_client(self):
        client = MagicMock()
        tx = Transaction(
            transaction_id="OM20250115120000000001",
            transaction_type="PAYMENT",
            amount=5000.0,
            currency="XOF",
            fee=50.0,
            sender_phone="abc123def4567890",
            recipient_phone="xyz987abc6543210",
            status="SUCCESS",
            initiated_at=datetime(2025, 1, 15, 12, 0, 0),
            completed_at=datetime(2025, 1, 15, 12, 0, 5),
            channel="USSD",
            region="Dakar",
        )
        page = TransactionPage(
            transactions=[tx],
            page=1,
            page_size=1,
            total_count=1,
            has_more=False,
        )
        client.fetch_transactions.return_value = iter([page])
        return client

    def test_ingest_date_range(self, mock_spark, mock_client):
        ingestor = BronzeIngestor(mock_spark, mock_client)
        rows = ingestor.ingest_date_range(
            datetime(2025, 1, 15),
            datetime(2025, 1, 15),
        )

        assert rows == 1
        mock_spark.createDataFrame.assert_called_once()

    def test_ingest_date_range_empty(self, mock_spark, mock_client):
        mock_client.fetch_transactions.return_value = iter([])
        ingestor = BronzeIngestor(mock_spark, mock_client)
        rows = ingestor.ingest_date_range(
            datetime(2025, 1, 15),
            datetime(2025, 1, 15),
        )

        assert rows == 0
