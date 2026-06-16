"""Gold layer package."""

from .daily_aggregates import GoldAggregator, aggregate_daily
from .fraud_patterns import FraudDetector

__all__ = ["GoldAggregator", "FraudDetector", "aggregate_daily"]
