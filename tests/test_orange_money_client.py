"""Tests for Orange Money API client and mock data generator."""

from datetime import datetime

import pytest

from src.ingestion.config import OrangeMoneyConfig
from src.ingestion.orange_money_client import (
    MockTransactionGenerator,
    OrangeMoneyClient,
    PhoneTokenizer,
    Transaction,
    TransactionPage,
)


class TestOrangeMoneyConfig:
    """Configuration tests."""

    def test_default_mock_mode(self):
        cfg = OrangeMoneyConfig()
        assert cfg.mock_mode is True
        assert cfg.mock_transactions_per_day == 5000
        assert cfg.cdp_tokenize_phone_numbers is True

    def test_validate_mock_mode(self):
        cfg = OrangeMoneyConfig(mock_mode=True)
        issues = cfg.validate()
        assert issues == []

    def test_validate_production_missing_creds(self):
        cfg = OrangeMoneyConfig(
            mock_mode=False,
            api_client_id="",
            api_client_secret="",
        )
        issues = cfg.validate()
        assert len(issues) > 0

    def test_path_properties(self):
        cfg = OrangeMoneyConfig()
        assert cfg.bronze_path == "orange_money.bronze.raw_transactions"
        assert cfg.silver_path == "orange_money.silver.normalized_transactions"
        assert cfg.gold_daily_path == "orange_money.gold.daily_summary"
        assert cfg.gold_merchant_path == "orange_money.gold.merchant_activity"


class TestMockTransactionGenerator:
    """Mock generator tests."""

    def test_generate_transaction(self):
        gen = MockTransactionGenerator(seed=42)
        tx = gen.generate_transaction(datetime(2025, 1, 15))

        assert isinstance(tx, Transaction)
        assert tx.transaction_id.startswith("OM")
        assert tx.amount > 0
        assert tx.currency == "XOF"
        assert tx.status in ("SUCCESS", "PENDING", "FAILED", "REVERSED")
        assert tx.channel in ("USSD", "APP", "WEB", "AGENT")
        assert tx.initiated_at is not None
        assert tx.completed_at is not None

    def test_generate_day(self):
        gen = MockTransactionGenerator(seed=42)
        count = 100
        txns = list(gen.generate_day(datetime(2025, 1, 15), count))

        assert len(txns) == count
        # All should be on the same day
        for tx in txns:
            assert tx.initiated_at.date() == datetime(2025, 1, 15).date()

    def test_reproducibility(self):
        gen1 = MockTransactionGenerator(seed=42)
        gen2 = MockTransactionGenerator(seed=42)

        tx1 = gen1.generate_transaction(datetime(2025, 1, 15))
        tx2 = gen2.generate_transaction(datetime(2025, 1, 15))

        assert tx1.transaction_id == tx2.transaction_id
        assert tx1.amount == tx2.amount

    def test_senegal_phone_format(self):
        gen = MockTransactionGenerator(seed=42)
        txns = list(gen.generate_day(datetime(2025, 1, 15), 50))

        for tx in txns:
            assert tx.sender_phone[:2] in ("77", "78", "76", "70", "75")
            assert len(tx.sender_phone) == 9  # 2-digit prefix + 7 digits


class TestPhoneTokenizer:
    """CDP phone tokenization tests."""

    def test_tokenize_deterministic(self):
        tokenizer = PhoneTokenizer(secret="test-secret")
        phone = "771234567"

        t1 = tokenizer.tokenize(phone)
        t2 = tokenizer.tokenize(phone)

        assert t1 == t2
        assert t1 != phone
        assert len(t1) == 16  # HMAC-SHA256 truncated to 16 chars

    def test_different_phones_different_tokens(self):
        tokenizer = PhoneTokenizer()

        t1 = tokenizer.tokenize("771234567")
        t2 = tokenizer.tokenize("781234567")

        assert t1 != t2

    def test_mask_display(self):
        tokenizer = PhoneTokenizer()
        masked = tokenizer.mask_display("771234567")

        assert masked == "77XXXXX567"
        assert "123" not in masked  # Middle digits hidden


class TestOrangeMoneyClient:
    """API client tests (mock mode)."""

    def test_client_mock_mode(self):
        client = OrangeMoneyClient()
        assert client.cfg.mock_mode is True

    def test_fetch_transactions(self):
        client = OrangeMoneyClient()
        start = datetime(2025, 1, 15)
        end = datetime(2025, 1, 15)  # Single day

        pages = list(client.fetch_transactions(start, end, page_size=100))

        assert len(pages) > 0
        all_txns = []
        for page in pages:
            assert isinstance(page, TransactionPage)
            assert page.page_size <= 100
            all_txns.extend(page.transactions)

        assert len(all_txns) == client.cfg.mock_transactions_per_day

    def test_fetch_date_range(self):
        client = OrangeMoneyClient()
        start = datetime(2025, 1, 15)
        end = datetime(2025, 1, 17)  # 3 days

        pages = list(client.fetch_transactions(start, end))
        all_txns = []
        for page in pages:
            all_txns.extend(page.transactions)

        expected = client.cfg.mock_transactions_per_day * 3
        assert len(all_txns) == expected

    def test_cdp_tokenization_applied(self):
        client = OrangeMoneyClient()
        pages = list(
            client.fetch_transactions(
                datetime(2025, 1, 15),
                datetime(2025, 1, 15),
                page_size=10,
            )
        )

        for tx in pages[0].transactions:
            # Tokenized phones should be 16-char hex, not 9-digit raw phone numbers
            assert len(tx.sender_phone) == 16
            assert len(tx.recipient_phone) == 16
            # Should not match raw phone format (9 digits)
            assert not tx.sender_phone.isdigit()
            assert not tx.recipient_phone.isdigit()

    def test_transaction_validation(self):
        """Ensure Pydantic validation catches invalid transactions."""
        with pytest.raises(Exception):
            Transaction(
                transaction_id="TEST",
                transaction_type="INVALID_TYPE",
                amount=-100,  # Negative amount should fail
                currency="XOF",
                sender_phone="771234567",
                recipient_phone="781234567",
                initiated_at=datetime(2025, 1, 15),
            )
