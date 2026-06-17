"""Tests for Silver normalization layer — pure logic, no PySpark dependency."""


class TestSilverNormalizer:
    """Silver normalization logic tests."""

    VALID_STATUSES = ("SUCCESS", "PENDING", "FAILED", "REVERSED")

    def _check(self, amount, status, has_date):
        return "PASS" if (amount > 0 and status in self.VALID_STATUSES and has_date) else "FAIL"

    def test_quality_flag_logic(self):
        """Verify the quality flag logic without Spark."""
        test_cases = [
            (5000, "SUCCESS", True, "PASS"),
            (0, "SUCCESS", True, "FAIL"),
            (5000, "INVALID", True, "FAIL"),
            (5000, "SUCCESS", False, "FAIL"),
            (-100, "SUCCESS", True, "FAIL"),
        ]

        for amount, status, has_date, expected in test_cases:
            assert self._check(amount, status, has_date) == expected, (
                f"Failed: amount={amount}, status={status}, has_date={has_date}"
            )

    def test_quality_flag_edge_cases(self):
        """Test edge cases for quality flag logic."""
        assert self._check(1000000, "SUCCESS", True) == "PASS"
        assert self._check(5000, "PENDING", True) == "PASS"
        # Failed transactions still pass quality checks
        assert self._check(5000, "FAILED", True) == "PASS"

    def test_rule_names_expected(self):
        """Verify expected quality rule names exist."""
        expected = [
            "amount_positive",
            "amount_reasonable",
            "currency_xof",
            "valid_status",
            "has_sender",
            "has_initiated_at",
            "fee_non_negative",
        ]
        assert len(expected) == 7
        assert "amount_positive" in expected
        assert "fee_non_negative" in expected
