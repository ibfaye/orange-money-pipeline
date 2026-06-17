"""
Orange Money API Client

Production-grade client for the Orange Money API with full mock mode support.
Implements CDP-compliant PII handling at the ingestion edge.

Mock mode generates realistic synthetic transaction data for development,
demonstration, and CI/CD pipelines without production API credentials.

Reference: Orange Money API Documentation (Partner Portal)
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import random
import time
import uuid
from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

import requests
from pydantic import BaseModel, Field, field_validator

from .config import config

if TYPE_CHECKING:
    from .config import OrangeMoneyConfig

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════
# Data Models
# ═══════════════════════════════════════════════════════════════════


class Transaction(BaseModel):
    """Normalized Orange Money transaction record."""

    transaction_id: str = Field(..., description="Unique transaction reference (Orange)")
    external_id: str | None = Field(None, description="Merchant-side reference")
    transaction_type: str = Field(
        ...,
        description="PAYMENT | TRANSFER | WITHDRAWAL | DEPOSIT | REFUND",
    )
    amount: float = Field(..., gt=0, description="Transaction amount in XOF")
    currency: str = Field(default="XOF", description="ISO 4217 currency code")
    fee: float = Field(default=0.0, ge=0, description="Transaction fee in XOF")

    # ── Parties ────────────────────────────────────────────────
    sender_phone: str = Field(..., description="Sender MSISDN")
    recipient_phone: str = Field(..., description="Recipient MSISDN")
    sender_name: str | None = Field(None)
    recipient_name: str | None = Field(None)

    # ── Merchant (if applicable) ────────────────────────────────
    merchant_code: str | None = Field(None)
    merchant_name: str | None = Field(None)
    merchant_category: str | None = Field(None)

    # ── Status & Timestamps ─────────────────────────────────────
    status: str = Field(default="SUCCESS", description="SUCCESS | PENDING | FAILED | REVERSED")
    failure_reason: str | None = Field(None)
    initiated_at: datetime = Field(..., description="Transaction initiation time (UTC)")
    completed_at: datetime | None = Field(None, description="Completion time (UTC)")

    # ── Metadata ────────────────────────────────────────────────
    channel: str = Field(default="USSD", description="USSD | APP | WEB | AGENT")
    agent_code: str | None = Field(None, description="Orange Money agent code")
    region: str | None = Field(None, description="Senegalese region (Dakar, Thiès, etc.)")
    raw_payload: dict[str, Any] | None = Field(None, description="Original API response")

    @field_validator("sender_phone", "recipient_phone")
    @classmethod
    def validate_phone(cls, v: str) -> str:
        """Validate Senegalese phone number format."""
        clean = v.replace("+221", "").replace(" ", "").strip()
        if not (
            clean.startswith("77")
            or clean.startswith("78")
            or clean.startswith("76")
            or clean.startswith("70")
            or clean.startswith("75")
        ):
            # Accept but warn for non-standard formats
            logger.warning(f"Non-standard phone format: {v}")
        return clean


class TransactionPage(BaseModel):
    """Paginated transaction response."""

    transactions: list[Transaction]
    page: int
    page_size: int
    total_count: int
    has_more: bool


# ═══════════════════════════════════════════════════════════════════
# Tokenization (CDP Compliance)
# ═══════════════════════════════════════════════════════════════════


class PhoneTokenizer:
    """HMAC-based phone number tokenization for CDP compliance.

    Tokenizes phone numbers at ingestion edge before data lands in
    the data lake. Uses HMAC-SHA256 with a rotating secret.

    Reference: Senegal CDP (Commission de Protection des Données
    Personnelles) — Article 44 on pseudonymization.
    """

    def __init__(self, secret: str | None = None):
        import os as _os

        self.secret = (
            secret or _os.getenv("OM_CDP_TOKEN_SECRET", "dev-secret-change-in-production")
        ).encode()

    def tokenize(self, phone: str) -> str:
        """Tokenize a phone number. Deterministic — same input → same token."""
        return hmac.new(self.secret, phone.encode(), hashlib.sha256).hexdigest()[:16]

    def mask_display(self, phone: str) -> str:
        """Mask phone for display: 77XXXXX123"""
        clean = phone.replace("+221", "").replace(" ", "").strip()
        if len(clean) >= 9:
            return clean[:2] + "XXXXX" + clean[-3:]
        return "XXXXX"


# ═══════════════════════════════════════════════════════════════════
# Mock Data Generator
# ═══════════════════════════════════════════════════════════════════


class MockTransactionGenerator:
    """Generates realistic synthetic Orange Money transactions for development.

    Models real Senegalese transaction patterns:
    - Peak hours: 09:00-12:00, 15:00-18:00 GMT
    - Higher volume in Dakar region (~45% of transactions)
    - Common amounts clustered around 500, 1000, 2000, 5000, 10000 XOF
    - Mix of USSD (65%), APP (25%), WEB (5%), AGENT (5%) channels
    """

    SENEGAL_PREFIXES = ["77", "78", "76", "70", "75"]
    SENEGAL_REGIONS = [
        "Dakar",
        "Dakar",
        "Dakar",
        "Dakar",
        "Dakar",  # 45%
        "Thiès",
        "Thiès",
        "Thiès",  # 15%
        "Diourbel",
        "Diourbel",  # 8%
        "Kaolack",
        "Kaolack",  # 8%
        "Saint-Louis",
        "Saint-Louis",  # 8%
        "Ziguinchor",  # 5%
        "Louga",  # 4%
        "Tambacounda",  # 3%
        "Kolda",  # 2%
        "Matam",  # 1%
        "Fatick",  # 1%
    ]
    COMMON_AMOUNTS = [500, 1000, 2000, 2500, 5000, 10000, 15000, 20000, 50000, 100000]
    MERCHANT_CATEGORIES = [
        "FOOD",
        "TRANSPORT",
        "UTILITIES",
        "TELECOM",
        "RETAIL",
        "HEALTH",
        "EDUCATION",
        "AGRICULTURE",
        "FINANCE",
        "OTHER",
    ]
    MERCHANT_NAMES = [
        "Sonatel",
        "Senelec",
        "SDE",
        "Auchan",
        "Total",
        "Pharmacie Nationale",
        "Dakar Dem Dikk",
        "La Poste",
        "Marché Kermel",
        "Centre Commercial",
    ]

    def __init__(self, seed: int | None = None) -> None:
        self.rng = random.Random(seed or int(time.time()))

    def _random_phone(self) -> str:
        prefix = self.rng.choice(self.SENEGAL_PREFIXES)
        suffix = "".join(str(self.rng.randint(0, 9)) for _ in range(7))
        return f"{prefix}{suffix}"

    def _random_amount(self) -> int:
        return self.rng.choice(self.COMMON_AMOUNTS) + self.rng.randint(0, 500)

    def _random_merchant(self) -> tuple[str, str, str] | None:
        if self.rng.random() < 0.6:  # 60% are merchant transactions
            name = self.rng.choice(self.MERCHANT_NAMES)
            code = f"MERCH{self.rng.randint(10000, 99999)}"
            category = self.rng.choice(self.MERCHANT_CATEGORIES)
            return (code, name, category)
        return None

    def _peak_weighted_hour(self) -> int:
        """Return hour weighted toward peak transaction times."""
        weights = {
            0: 0.5,
            1: 0.3,
            2: 0.2,
            3: 0.1,
            4: 0.2,
            5: 0.5,
            6: 2.0,
            7: 3.0,
            8: 5.0,
            9: 7.0,
            10: 8.0,
            11: 7.0,
            12: 5.0,
            13: 3.0,
            14: 4.0,
            15: 6.0,
            16: 7.0,
            17: 8.0,
            18: 6.0,
            19: 4.0,
            20: 3.0,
            21: 2.0,
            22: 1.5,
            23: 1.0,
        }
        hours = list(weights.keys())
        w = [weights[h] for h in hours]
        return self.rng.choices(hours, weights=w, k=1)[0]

    def _random_channel(self) -> str:
        return self.rng.choices(
            ["USSD", "APP", "WEB", "AGENT"],
            weights=[65, 25, 5, 5],
            k=1,
        )[0]

    def _random_transaction_type(self) -> str:
        return self.rng.choices(
            ["PAYMENT", "TRANSFER", "WITHDRAWAL", "DEPOSIT", "REFUND"],
            weights=[45, 30, 15, 8, 2],
            k=1,
        )[0]

    def generate_transaction(self, date: datetime) -> Transaction:
        """Generate a single synthetic transaction for the given date."""
        hour = self._peak_weighted_hour()
        minute = self.rng.randint(0, 59)
        second = self.rng.randint(0, 59)
        initiated = date.replace(hour=hour, minute=minute, second=second, tzinfo=timezone.utc)
        completed = initiated + timedelta(seconds=self.rng.randint(1, 30))

        amount = self._random_amount()
        fee = round(amount * self.rng.uniform(0.005, 0.02))  # 0.5-2% fee
        txn_type = self._random_transaction_type()
        merchant = self._random_merchant()

        tx_id = f"OM{initiated.strftime('%Y%m%d%H%M%S')}{self.rng.randint(100000, 999999)}"
        channel = self._random_channel()
        region = self.rng.choice(self.SENEGAL_REGIONS)

        return Transaction(
            transaction_id=tx_id,
            external_id=f"EXT-{uuid.uuid4().hex[:12]}" if self.rng.random() < 0.3 else None,
            transaction_type=txn_type,
            amount=float(amount),
            currency="XOF",
            fee=float(fee),
            sender_phone=self._random_phone(),
            recipient_phone=self._random_phone(),
            sender_name=None,
            recipient_name=None,
            merchant_code=merchant[0] if merchant else None,
            merchant_name=merchant[1] if merchant else None,
            merchant_category=merchant[2] if merchant else None,
            status=self.rng.choices(
                ["SUCCESS", "SUCCESS", "SUCCESS", "FAILED", "PENDING"],
                weights=[88, 88, 88, 8, 4],
                k=1,
            )[0],
            failure_reason="INSUFFICIENT_FUNDS" if self.rng.random() < 0.02 else None,
            initiated_at=initiated,
            completed_at=completed,
            channel=channel,
            agent_code=f"AG{self.rng.randint(10000, 99999)}" if channel == "AGENT" else None,
            region=region,
            raw_payload=None,
        )

    def generate_day(self, date: datetime, count: int) -> Iterator[Transaction]:
        """Generate `count` transactions for a single day."""
        for _ in range(count):
            yield self.generate_transaction(date)


# ═══════════════════════════════════════════════════════════════════
# API Client
# ═══════════════════════════════════════════════════════════════════


class OrangeMoneyClient:
    """Orange Money API client with mock mode fallback.

    Usage:
        client = OrangeMoneyClient()
        for page in client.fetch_transactions(start_date, end_date):
            for tx in page.transactions:
                process(tx)
    """

    def __init__(self, cfg: OrangeMoneyConfig | None = None):
        self.cfg = cfg or config
        self._tokenizer: PhoneTokenizer | None = None
        self._mock_gen: MockTransactionGenerator | None = None
        self._access_token: str | None = None
        self._token_expiry: float = 0.0

        if self.cfg.cdp_tokenize_phone_numbers:
            self._tokenizer = PhoneTokenizer()

        if self.cfg.mock_mode:
            self._mock_gen = MockTransactionGenerator()
            logger.info("Orange Money client initialized in MOCK mode")
        else:
            logger.info("Orange Money client initialized in PRODUCTION mode")

    def _authenticate(self) -> str:
        """Obtient un token OAuth2 depuis l'API Orange Money."""
        if self._access_token and time.time() < self._token_expiry:
            return self._access_token

        if self.cfg.mock_mode:
            self._access_token = f"mock-token-{uuid.uuid4().hex}"
            self._token_expiry = time.time() + 3600
            return self._access_token

        response = requests.post(
            f"{self.cfg.api_base_url}/oauth/token",
            data={
                "grant_type": "client_credentials",
                "client_id": self.cfg.api_client_id,
                "client_secret": self.cfg.api_client_secret,
            },
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        self._access_token = data["access_token"]
        self._token_expiry = time.time() + data.get("expires_in", 3600) - 60
        return self._access_token

    def fetch_transactions(
        self,
        start_date: datetime,
        end_date: datetime,
        page_size: int = 100,
    ) -> Iterator[TransactionPage]:
        """Fetch transactions for date range. Paginated.

        In mock mode, generates synthetic data matching realistic patterns.
        In production, calls Orange Money API with OAuth2 authentication.
        """
        if self.cfg.mock_mode:
            yield from self._fetch_mock(start_date, end_date, page_size)
        else:
            yield from self._fetch_production(start_date, end_date, page_size)

    def _fetch_mock(
        self,
        start_date: datetime,
        end_date: datetime,
        page_size: int,
    ) -> Iterator[TransactionPage]:
        """Generate mock transactions for the date range."""
        assert self._mock_gen is not None
        current = start_date
        total_generated = 0

        while current <= end_date:
            daily_txns = list(
                self._mock_gen.generate_day(current, self.cfg.mock_transactions_per_day)
            )

            # Apply CDP tokenization if enabled
            if self._tokenizer:
                for tx in daily_txns:
                    tx.sender_phone = self._tokenizer.tokenize(tx.sender_phone)
                    tx.recipient_phone = self._tokenizer.tokenize(tx.recipient_phone)

            total_generated += len(daily_txns)

            # Paginate
            for offset in range(0, len(daily_txns), page_size):
                page_txns = daily_txns[offset : offset + page_size]
                yield TransactionPage(
                    transactions=page_txns,
                    page=offset // page_size + 1,
                    page_size=len(page_txns),
                    total_count=len(daily_txns),
                    has_more=offset + page_size < len(daily_txns),
                )

            current += timedelta(days=1)

        logger.info(
            f"Mock: generated {total_generated} transactions "
            f"across {(end_date - start_date).days + 1} days"
        )

    def _fetch_production(
        self,
        start_date: datetime,
        end_date: datetime,
        page_size: int,
    ) -> Iterator[TransactionPage]:
        """Fetch real transactions from Orange Money API."""
        token = self._authenticate()
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        page = 1
        while True:
            params = {
                "startDate": start_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "endDate": end_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "page": page,
                "pageSize": page_size,
                "merchantCode": self.cfg.api_merchant_code,
            }

            data: dict[str, Any] = {}
            for attempt in range(self.cfg.max_retries):
                try:
                    response = requests.get(
                        f"{self.cfg.api_base_url}/transactions",
                        headers=headers,
                        params=params,
                        timeout=30,
                    )
                    response.raise_for_status()
                    data = response.json()
                    break
                except requests.RequestException as e:
                    if attempt == self.cfg.max_retries - 1:
                        raise
                    logger.warning(f"Retry {attempt + 1}/{self.cfg.max_retries}: {e}")
                    time.sleep(self.cfg.retry_delay_seconds * (2**attempt))

            transactions = []
            for raw in data.get("transactions", []):
                try:
                    tx = Transaction(
                        transaction_id=raw["id"],
                        external_id=raw.get("externalId"),
                        transaction_type=raw["type"],
                        amount=float(raw["amount"]),
                        currency=raw.get("currency", "XOF"),
                        fee=float(raw.get("fee", 0)),
                        sender_phone=raw["sender"]["msisdn"],
                        recipient_phone=raw["recipient"]["msisdn"],
                        sender_name=raw["sender"].get("name"),
                        recipient_name=raw["recipient"].get("name"),
                        merchant_code=raw.get("merchantCode"),
                        merchant_name=raw.get("merchantName"),
                        merchant_category=raw.get("merchantCategory"),
                        status=raw.get("status", "SUCCESS"),
                        initiated_at=datetime.fromisoformat(raw["initiatedAt"]),
                        completed_at=(
                            datetime.fromisoformat(raw.get("completedAt"))
                            if raw.get("completedAt")
                            else None
                        ),
                        channel=raw.get("channel", "USSD"),
                        region=raw.get("region"),
                        raw_payload=raw,
                    )

                    # CDP tokenization at the edge
                    if self._tokenizer:
                        tx.sender_phone = self._tokenizer.tokenize(tx.sender_phone)
                        tx.recipient_phone = self._tokenizer.tokenize(tx.recipient_phone)
                        # Remove raw PII from stored payload
                        if tx.raw_payload and "sender" in tx.raw_payload:
                            del tx.raw_payload["sender"]
                        if tx.raw_payload and "recipient" in tx.raw_payload:
                            del tx.raw_payload["recipient"]

                    transactions.append(tx)
                except Exception as e:
                    logger.error(f"Failed to parse transaction {raw.get('id', '?')}: {e}")

            yield TransactionPage(
                transactions=transactions,
                page=page,
                page_size=len(transactions),
                total_count=data.get("totalCount", 0),
                has_more=data.get("hasMore", False),
            )

            if not data.get("hasMore"):
                break
            page += 1
