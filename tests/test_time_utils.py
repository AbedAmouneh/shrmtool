"""
Tests for utils.time_utils module.
"""

import pytest
from datetime import datetime
import pytz
from utils import time_utils


UTC = pytz.UTC
EASTERN = pytz.timezone("US/Eastern")


class TestParseIsoDate:
    """Tests for parse_iso_date function."""
    
    def test_parse_iso_date_creates_midnight_et(self, mock_config):
        """Test that parse_iso_date creates datetime at midnight ET."""
        result = time_utils.parse_iso_date("2025-12-05")
        
        assert result.tzinfo == EASTERN
        assert result.hour == 0
        assert result.minute == 0
        assert result.second == 0
        assert result.date() == datetime(2025, 12, 5).date()
    
    def test_parse_iso_date_different_dates(self, mock_config):
        """Test parse_iso_date with different date strings."""
        result1 = time_utils.parse_iso_date("2025-01-01")
        result2 = time_utils.parse_iso_date("2025-12-31")
        
        assert result1.date() == datetime(2025, 1, 1).date()
        assert result2.date() == datetime(2025, 12, 31).date()


class TestGetVerdictDate:
    """Tests for get_verdict_date function."""
    
    def test_get_verdict_date_returns_correct_date(self, mock_config):
        """Test that get_verdict_date returns verdict date from config."""
        result = time_utils.get_verdict_date()
        
        assert result.tzinfo == EASTERN
        assert result.hour == 0
        assert result.minute == 0
        assert result.date() == datetime(2025, 12, 5).date()


class TestUtcToEastern:
    """Tests for utc_to_eastern function."""
    
    def test_utc_to_eastern_converts_timezone(self, mock_config):
        """Test UTC to Eastern timezone conversion."""
        utc_dt = UTC.localize(datetime(2025, 12, 5, 15, 30, 0))
        result = time_utils.utc_to_eastern(utc_dt)
        
        assert result.tzinfo == EASTERN
        # 15:30 UTC = 10:30 ET (EST, UTC-5) or 11:30 ET (EDT, UTC-4)
        # December is EST, so should be 10:30
        assert result.hour == 10
        assert result.minute == 30
    
    def test_utc_to_eastern_with_naive_datetime(self, mock_config):
        """Test that naive datetime is assumed to be UTC."""
        naive_dt = datetime(2025, 12, 5, 15, 30, 0)
        result = time_utils.utc_to_eastern(naive_dt)
        
        assert result.tzinfo == EASTERN
        assert result.hour == 10  # EST is UTC-5


class TestFormatDateMmddyyyy:
    """Tests for format_date_mmddyyyy function."""
    
    def test_format_date_mmddyyyy_utc_input(self, mock_config):
        """Test formatting UTC datetime to MM/DD/YYYY."""
        utc_dt = UTC.localize(datetime(2025, 12, 6, 10, 30, 0))
        result = time_utils.format_date_mmddyyyy(utc_dt)
        
        assert result == "12/06/2025"
    
    def test_format_date_mmddyyyy_eastern_input(self, mock_config):
        """Test formatting Eastern datetime to MM/DD/YYYY."""
        et_dt = EASTERN.localize(datetime(2025, 12, 6, 10, 30, 0))
        result = time_utils.format_date_mmddyyyy(et_dt)
        
        assert result == "12/06/2025"
    
    def test_format_date_mmddyyyy_naive_input(self, mock_config):
        """Test formatting naive datetime (assumed UTC)."""
        naive_dt = datetime(2025, 12, 6, 10, 30, 0)
        result = time_utils.format_date_mmddyyyy(naive_dt)
        
        assert result == "12/06/2025"
    
    def test_format_date_mmddyyyy_single_digit_month_day(self, mock_config):
        """Test formatting with single-digit month and day."""
        utc_dt = UTC.localize(datetime(2025, 1, 5, 10, 30, 0))
        result = time_utils.format_date_mmddyyyy(utc_dt)
        
        assert result == "01/05/2025"


class TestParseRedditDate:
    """Tests for parse_reddit_date function."""
    
    def test_parse_reddit_date_with_timezone(self, mock_config):
        """Test parsing Reddit date with timezone offset."""
        date_str = "2025-12-05T10:30:00+00:00"
        result = time_utils.parse_reddit_date(date_str)
        
        assert result.tzinfo == UTC
        assert result.year == 2025
        assert result.month == 12
        assert result.day == 5
        assert result.hour == 10
        assert result.minute == 30
    
    def test_parse_reddit_date_with_microseconds(self, mock_config):
        """Test parsing Reddit date with microseconds."""
        date_str = "2025-12-05T10:30:00.123456+00:00"
        result = time_utils.parse_reddit_date(date_str)
        
        assert result.tzinfo == UTC
        assert result.microsecond == 123456
    
    def test_parse_reddit_date_without_timezone(self, mock_config):
        """Test parsing Reddit date without timezone (assumed UTC)."""
        date_str = "2025-12-05T10:30:00"
        result = time_utils.parse_reddit_date(date_str)
        
        assert result.tzinfo == UTC
        assert result.hour == 10
        assert result.minute == 30
    
    def test_parse_reddit_date_invalid_format(self, mock_config):
        """Test that invalid date format raises ValueError."""
        with pytest.raises(ValueError, match="Unable to parse date string"):
            time_utils.parse_reddit_date("invalid-date")


class TestParseNewsapiDate:
    """Tests for parse_newsapi_date function."""
    
    def test_parse_newsapi_date_with_z_suffix(self, mock_config):
        """Test parsing NewsAPI date with Z suffix."""
        date_str = "2025-12-05T10:30:00Z"
        result = time_utils.parse_newsapi_date(date_str)
        
        assert result.tzinfo == UTC
        assert result.year == 2025
        assert result.month == 12
        assert result.day == 5
        assert result.hour == 10
        assert result.minute == 30
    
    def test_parse_newsapi_date_with_milliseconds(self, mock_config):
        """Test parsing NewsAPI date with milliseconds."""
        date_str = "2025-12-05T10:30:00.123Z"
        result = time_utils.parse_newsapi_date(date_str)
        
        assert result.tzinfo == UTC
        assert result.microsecond == 123000


class TestIsAfterVerdictDate:
    """Tests for is_after_verdict_date function."""
    
    def test_is_after_verdict_date_on_verdict_date(self, mock_config):
        """Test that verdict date itself returns True."""
        # Verdict date is 2025-12-05, so 2025-12-05 00:00 ET should return True
        verdict_dt = EASTERN.localize(datetime(2025, 12, 5, 0, 0, 0))
        result = time_utils.is_after_verdict_date(verdict_dt)
        
        assert result is True
    
    def test_is_after_verdict_date_after_verdict(self, mock_config):
        """Test that dates after verdict date return True."""
        after_dt = UTC.localize(datetime(2025, 12, 6, 10, 0, 0))
        result = time_utils.is_after_verdict_date(after_dt)
        
        assert result is True
    
    def test_is_after_verdict_date_before_verdict(self, mock_config):
        """Test that dates before verdict date return False."""
        before_dt = UTC.localize(datetime(2025, 12, 4, 23, 59, 59))
        result = time_utils.is_after_verdict_date(before_dt)
        
        assert result is False
    
    def test_is_after_verdict_date_with_naive_datetime(self, mock_config):
        """Test that naive datetime is assumed UTC and converted."""
        # 2025-12-05 00:00 UTC = 2025-12-04 19:00 ET (EST), so should be False
        naive_before = datetime(2025, 12, 4, 19, 0, 0)
        result_before = time_utils.is_after_verdict_date(naive_before)
        assert result_before is False
        
        # 2025-12-05 12:00 UTC = 2025-12-05 07:00 ET, so should be True
        naive_after = datetime(2025, 12, 5, 12, 0, 0)
        result_after = time_utils.is_after_verdict_date(naive_after)
        assert result_after is True
