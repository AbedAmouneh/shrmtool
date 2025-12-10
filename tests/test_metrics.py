import pytest

from utils.metrics import parse_k_number, compute_eng_total


class TestParseKNumber:
    def test_parses_k_suffix(self):
        assert parse_k_number("64.5K") == 64500

    def test_parses_m_suffix(self):
        assert parse_k_number("1.2M") == 1200000

    def test_parses_plain_int_string(self):
        assert parse_k_number("1234") == 1234

    def test_parses_int(self):
        assert parse_k_number(1234) == 1234

    def test_returns_none_for_na(self):
        assert parse_k_number("N/A") is None
        assert parse_k_number("n/a") is None

    def test_returns_none_for_none(self):
        assert parse_k_number(None) is None

    def test_returns_none_for_invalid(self):
        assert parse_k_number("not-a-number") is None
        assert parse_k_number("12X") is None


class TestComputeEngTotal:
    def test_computes_sum_when_all_numeric(self):
        assert compute_eng_total(10, 5, 2) == 17
        assert compute_eng_total("10", "5", "2") == 17

    def test_returns_none_if_any_missing(self):
        assert compute_eng_total(10, None, 2) is None
        assert compute_eng_total(None, None, None) is None
        assert compute_eng_total("N/A", 5, 1) is None

