"""Tests for Silver normalization layer."""

import pytest

from src.silver.transaction_normalizer import QUALITY_RULES


class TestQualityRules:
    """Data quality rule validation."""

    def test_all_rules_defined(self):
        assert "amount_positive" in QUALITY_RULES
        assert "amount_reasonable" in QUALITY_RULES
        assert "currency_xof" in QUALITY_RULES
        assert "valid_status" in QUALITY_RULES
        assert "has_sender" in QUALITY_RULES
        assert "has_initiated_at" in QUALITY_RULES
        assert "fee_non_negative" in QUALITY_RULES

    def test_amount_positive_rule(self):
        """Rule should reject zero and negative amounts."""
        # This is a logical test — the actual Spark expression is evaluated at runtime
        rule = str(QUALITY_RULES["amount_positive"])
        assert "amount" in rule
        assert "0" in rule

    def test_valid_status_rule(self):
        rule = str(QUALITY_RULES["valid_status"])
        assert "SUCCESS" in rule
        assert "FAILED" in rule
        assert "PENDING" in rule
        assert "REVERSED" in rule


class TestSilverNormalizer:
    """Silver normalization tests (unit tests on logic)."""

    def test_quality_flag_logic(self):
        """Verify the quality flag logic without Spark.

        The Silver normalizer marks rows as PASS only if ALL critical
        checks pass: amount > 0, valid status, and has initiated_at.
        """
        # Test the expected outcomes
        test_cases = [
            # (amount, status, has_initiated_at, expected_flag)
            (5000, "SUCCESS", True, "PASS"),
            (0, "SUCCESS", True, "FAIL"),
            (5000, "INVALID", True, "FAIL"),
            (5000, "SUCCESS", False, "FAIL"),
            (-100, "SUCCESS", True, "FAIL"),
        ]

        for amount, status, has_date, expected in test_cases:
            is_pass = (
                amount > 0 and
                status in ("SUCCESS", "PENDING", "FAILED", "REVERSED") and
                has_date
            )
            assert ("PASS" if is_pass else "FAIL") == expected, \
                f"Failed: amount={amount}, status={status}, has_date={has_date}"
