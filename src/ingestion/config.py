"""
Orange Money Pipeline — Configuration

All configuration for the Orange Money → Databricks Medallion pipeline.
Supports mock mode for development/demonstration without production API credentials.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field


@dataclass
class OrangeMoneyConfig:
    """Orange Money API and pipeline configuration.

    All sensitive values are sourced from environment variables.
    For development, set OM_MOCK_MODE=true to use synthetic data.
    """

    # ── API Configuration ──────────────────────────────────────────
    api_base_url: str = field(
        default_factory=lambda: os.getenv(
            "OM_API_BASE_URL", "https://api.orange.com/orange-money/v1"
        )
    )
    api_client_id: str = field(
        default_factory=lambda: os.getenv("OM_CLIENT_ID", "")
    )
    api_client_secret: str = field(
        default_factory=lambda: os.getenv("OM_CLIENT_SECRET", "")
    )
    api_merchant_code: str = field(
        default_factory=lambda: os.getenv("OM_MERCHANT_CODE", "")
    )

    # ── Mock Mode ──────────────────────────────────────────────────
    mock_mode: bool = field(
        default_factory=lambda: os.getenv("OM_MOCK_MODE", "true").lower() == "true"
    )
    mock_transactions_per_day: int = field(
        default_factory=lambda: int(os.getenv("OM_MOCK_TX_PER_DAY", "5000"))
    )
    mock_date_range_days: int = field(
        default_factory=lambda: int(os.getenv("OM_MOCK_DATE_RANGE", "30"))
    )

    # ── CDP Compliance (Senegal) ───────────────────────────────────
    cdp_tokenize_phone_numbers: bool = field(
        default_factory=lambda: os.getenv("OM_CDP_TOKENIZE_PHONES", "true").lower() == "true"
    )
    cdp_retention_days: int = field(
        default_factory=lambda: int(os.getenv("OM_CDP_RETENTION_DAYS", "365"))
    )
    cdp_consent_required: bool = True

    # ── Databricks / Delta Lake ────────────────────────────────────
    catalog_name: str = field(
        default_factory=lambda: os.getenv("OM_CATALOG", "orange_money")
    )
    bronze_schema: str = "bronze"
    silver_schema: str = "silver"
    gold_schema: str = "gold"
    bronze_table: str = "raw_transactions"
    silver_table: str = "normalized_transactions"
    gold_daily_table: str = "daily_summary"
    gold_merchant_table: str = "merchant_activity"

    # ── Pipeline ───────────────────────────────────────────────────
    batch_size: int = 1000
    max_retries: int = 3
    retry_delay_seconds: int = 5

    @property
    def bronze_path(self) -> str:
        return f"{self.catalog_name}.{self.bronze_schema}.{self.bronze_table}"

    @property
    def silver_path(self) -> str:
        return f"{self.catalog_name}.{self.silver_schema}.{self.silver_table}"

    @property
    def gold_daily_path(self) -> str:
        return f"{self.catalog_name}.{self.gold_schema}.{self.gold_daily_table}"

    @property
    def gold_merchant_path(self) -> str:
        return f"{self.catalog_name}.{self.gold_schema}.{self.gold_merchant_table}"

    def validate(self) -> list[str]:
        """Validate configuration. Returns list of issues (empty = valid)."""
        issues = []
        if not self.mock_mode:
            if not self.api_client_id:
                issues.append("OM_CLIENT_ID is required when mock_mode is disabled")
            if not self.api_client_secret:
                issues.append("OM_CLIENT_SECRET is required when mock_mode is disabled")
            if not self.api_merchant_code:
                issues.append("OM_MERCHANT_CODE is required when mock_mode is disabled")
        if self.cdp_retention_days < 30:
            issues.append("CDP retention must be at least 30 days")
        return issues


# Singleton
config = OrangeMoneyConfig()
