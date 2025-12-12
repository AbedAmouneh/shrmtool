"""
Tests for integrations.linkedin_google_collector module.
"""

import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime
import pytz

from integrations.linkedin_google_collector import (
    LinkedInGoogleCollector,
    _clean_title,
    _extract_linkedin_profile,
    _is_verdict_relevant,
)
from utils.time_utils import EASTERN


class FakeResponse:
    """Fake requests.Response for mocking Google Custom Search API calls."""

    def __init__(self, status_code=200, json_data=None, text=""):
        self.status_code = status_code
        self._json_data = json_data or {}
        self.text = text

    def json(self):
        return self._json_data
    
    def raise_for_status(self):
        """Mock raise_for_status for compatibility."""
        if self.status_code >= 400:
            raise Exception(f"HTTP {self.status_code}")


@pytest.fixture(autouse=True)
def reset_env(monkeypatch):
    """Ensure API keys are set during tests unless overridden."""
    monkeypatch.setenv("GOOGLE_API_KEY", "test-api-key")
    monkeypatch.setenv("GOOGLE_CSE_ID", "test-cse-id")
    yield
    # Cleanup handled by monkeypatch


class TestLinkedInGoogleCollector:
    """Tests for LinkedInGoogleCollector class."""

    def test_happy_path_normalization(self, monkeypatch):
        """Test that LinkedIn items are normalized correctly."""
        fake_item = {
            "title": "SHRM Verdict Discussion | LinkedIn",
            "link": "https://www.linkedin.com/posts/johndoe-activity-1234567890",
            "snippet": "The SHRM verdict has sparked discussion in the HR community...",
        }

        fake_response = FakeResponse(
            200,
            {
                "items": [fake_item],
            },
        )

        with patch("requests.get", return_value=fake_response):
            collector = LinkedInGoogleCollector()
            results = collector.collect()

        assert len(results) == 1
        item = results[0]
        assert item["platform"] == "LinkedIn-Google"
        assert item["post_link"] == "https://www.linkedin.com/posts/johndoe-activity-1234567890"
        assert item["title"] == "SHRM Verdict Discussion"  # " | LinkedIn" removed
        assert item["summary"] == "The SHRM verdict has sparked discussion in the HR community..."
        assert item["description"] == item["summary"]  # Same as snippet
        assert item["profile_link"] == "https://www.linkedin.com/in/johndoe/"
        assert item["followers"] == "N/A"
        assert item["likes"] == "N/A"
        assert item["comments"] == "N/A"
        assert item["shares"] == "N/A"
        assert item["eng_total"] == "N/A"
        assert item["tone"] == "N/A"
        assert item["category"] == ""
        assert item["notes"] == ""
        # Date should be today's date in MM/DD/YYYY format
        assert item["date_posted"]  # Should be non-empty
        assert "/" in item["date_posted"]  # MM/DD/YYYY format

    def test_missing_api_keys(self, monkeypatch):
        """Test that missing API keys return empty list."""
        monkeypatch.delenv("GOOGLE_API_KEY", raising=False)
        monkeypatch.delenv("GOOGLE_CSE_ID", raising=False)

        collector = LinkedInGoogleCollector()
        results = collector.collect()
        assert results == []

    def test_missing_one_api_key(self, monkeypatch):
        """Test that missing one API key returns empty list."""
        monkeypatch.delenv("GOOGLE_CSE_ID", raising=False)

        collector = LinkedInGoogleCollector()
        results = collector.collect()
        assert results == []

    def test_http_error(self, monkeypatch):
        """Test that HTTP errors are handled gracefully."""
        fake_response = FakeResponse(500, {}, "Server error")

        with patch("requests.get", return_value=fake_response):
            collector = LinkedInGoogleCollector()
            results = collector.collect()

        assert results == []

    def test_empty_response(self, monkeypatch):
        """Test that empty responses return empty list."""
        fake_response = FakeResponse(200, {"items": []})

        with patch("requests.get", return_value=fake_response):
            collector = LinkedInGoogleCollector()
            results = collector.collect()

        assert results == []

    def test_network_error(self, monkeypatch):
        """Test that network errors are handled gracefully."""
        with patch("requests.get", side_effect=Exception("Network error")):
            collector = LinkedInGoogleCollector()
            results = collector.collect()

        assert results == []

    def test_title_cleaning(self, monkeypatch):
        """Test that ' | LinkedIn' suffix is removed from titles."""
        fake_item = {
            "title": "SHRM Trial Update | LinkedIn",
            "link": "https://www.linkedin.com/posts/user-activity-123",
            "snippet": "Test snippet",
        }

        fake_response = FakeResponse(200, {"items": [fake_item]})

        with patch("requests.get", return_value=fake_response):
            collector = LinkedInGoogleCollector()
            results = collector.collect()

        assert len(results) == 1
        assert results[0]["title"] == "SHRM Trial Update"

    def test_title_cleaning_case_insensitive(self, monkeypatch):
        """Test that title cleaning is case-insensitive."""
        fake_item = {
            "title": "SHRM Verdict Discussion | linkedin",
            "link": "https://www.linkedin.com/posts/user-activity-123",
            "snippet": "The verdict was announced",
        }

        fake_response = FakeResponse(200, {"items": [fake_item]})

        with patch("requests.get", return_value=fake_response):
            collector = LinkedInGoogleCollector()
            results = collector.collect()

        assert len(results) == 1
        assert results[0]["title"] == "SHRM Verdict Discussion"

    def test_title_without_linkedin_suffix(self, monkeypatch):
        """Test that titles without LinkedIn suffix are preserved."""
        fake_item = {
            "title": "SHRM Verdict Discussion",
            "link": "https://www.linkedin.com/posts/user-activity-123",
            "snippet": "Test",
        }

        fake_response = FakeResponse(200, {"items": [fake_item]})

        with patch("requests.get", return_value=fake_response):
            collector = LinkedInGoogleCollector()
            results = collector.collect()

        assert len(results) == 1
        assert results[0]["title"] == "SHRM Verdict Discussion"

    def test_multiple_keywords(self, monkeypatch):
        """Test that multiple keywords are processed."""
        keywords = ["SHRM verdict", "Johnny C. Taylor"]

        def side_effect(url, params=None, timeout=None):
            keyword = params.get("q") if params else ""
            if keyword == "SHRM verdict":
                return FakeResponse(
                    200,
                    {
                        "items": [
                            {
                                "title": "SHRM Verdict | LinkedIn",
                                "link": "https://www.linkedin.com/posts/user1-activity-1",
                                "snippet": "Verdict snippet",
                            }
                        ]
                    },
                )
            else:
                return FakeResponse(
                    200,
                    {
                        "items": [
                            {
                                "title": "Johnny C. Taylor Verdict | LinkedIn",
                                "link": "https://www.linkedin.com/posts/user2-activity-2",
                                "snippet": "Johnny C. Taylor verdict discussion",
                            }
                        ]
                    },
                )

        with patch("requests.get", side_effect=side_effect):
            collector = LinkedInGoogleCollector()
            results = collector.collect(keywords=keywords)

        assert len(results) == 2
        urls = [r["post_link"] for r in results]
        assert "https://www.linkedin.com/posts/user1-activity-1" in urls
        assert "https://www.linkedin.com/posts/user2-activity-2" in urls

    def test_per_run_deduplication(self, monkeypatch):
        """Test that duplicate URLs within a run are deduplicated."""
        fake_item = {
            "title": "SHRM Verdict Post | LinkedIn",
            "link": "https://www.linkedin.com/posts/user-activity-123",
            "snippet": "The verdict was announced by the jury",
        }

        # Same item returned twice for same keyword
        fake_response = FakeResponse(200, {"items": [fake_item, fake_item]})

        with patch("requests.get", return_value=fake_response):
            collector = LinkedInGoogleCollector()
            results = collector.collect()

        # Should only have one result
        assert len(results) == 1

    def test_validation_failure_filtered(self, monkeypatch):
        """Test that items failing validation are filtered out."""
        # Item with missing link (will fail validation)
        fake_item = {
            "title": "SHRM Post",
            # Missing link
            "snippet": "Test",
        }

        fake_response = FakeResponse(200, {"items": [fake_item]})

        with patch("requests.get", return_value=fake_response):
            collector = LinkedInGoogleCollector()
            results = collector.collect()

        # Should be filtered out
        assert len(results) == 0

    def test_invalid_url_filtered(self, monkeypatch):
        """Test that items with invalid URLs are filtered out."""
        fake_item = {
            "title": "SHRM Post | LinkedIn",
            "link": "not-a-valid-url",
            "snippet": "Test",
        }

        fake_response = FakeResponse(200, {"items": [fake_item]})

        with patch("requests.get", return_value=fake_response):
            collector = LinkedInGoogleCollector()
            results = collector.collect()

        # Should be filtered out
        assert len(results) == 0

    def test_schema_compliance(self, monkeypatch):
        """Test that returned items match 17-column schema when converted to rows."""
        from utils.schema import build_row, validate_row

        fake_item = {
            "title": "SHRM Verdict | LinkedIn",
            "link": "https://www.linkedin.com/posts/user-activity-123",
            "snippet": "The SHRM verdict discussion...",
        }

        fake_response = FakeResponse(200, {"items": [fake_item]})

        with patch("requests.get", return_value=fake_response):
            collector = LinkedInGoogleCollector()
            results = collector.collect()

        assert len(results) == 1
        item = results[0]

        # Convert to row and validate
        row = build_row(item)
        assert len(row) == 17
        assert validate_row(row)

    def test_default_keywords(self, monkeypatch):
        """Test that default keywords are used when none provided."""
        fake_response = FakeResponse(200, {"items": []})

        call_count = [0]

        def side_effect(url, params=None, timeout=None):
            call_count[0] += 1
            return fake_response

        with patch("requests.get", side_effect=side_effect):
            collector = LinkedInGoogleCollector()
            collector.collect()  # No keywords provided

        # Should have called with default keywords
        assert call_count[0] == 4  # Default: ["SHRM verdict", "Johnny C. Taylor verdict", "SHRM CEO verdict", "SHRM discrimination"]

    def test_custom_topic(self, monkeypatch):
        """Test that custom topic is used."""
        fake_item = {
            "title": "SHRM Verdict Post | LinkedIn",
            "link": "https://www.linkedin.com/posts/user-activity-123",
            "snippet": "The trial verdict was announced",
        }

        fake_response = FakeResponse(200, {"items": [fake_item]})

        with patch("requests.get", return_value=fake_response):
            collector = LinkedInGoogleCollector()
            results = collector.collect(topic="Custom Topic")

        assert len(results) == 1
        assert results[0]["topic"] == "Custom Topic"


class TestHelperFunctions:
    """Tests for helper functions."""

    def test_clean_title_removes_suffix(self):
        """Test _clean_title removes ' | LinkedIn' suffix."""
        assert _clean_title("SHRM Post | LinkedIn") == "SHRM Post"
        assert _clean_title("SHRM Post | linkedin") == "SHRM Post"  # Case insensitive
        assert _clean_title("SHRM Post") == "SHRM Post"  # No suffix
        assert _clean_title("") == "N/A"  # Empty
        assert _clean_title(None) == "N/A"  # None

    def test_extract_linkedin_profile_from_posts(self):
        """Test _extract_linkedin_profile extracts profile from posts URL."""
        url = "https://www.linkedin.com/posts/johndoe-activity-1234567890"
        assert _extract_linkedin_profile(url) == "https://www.linkedin.com/in/johndoe/"

    def test_extract_linkedin_profile_no_match(self):
        """Test _extract_linkedin_profile returns N/A when no match."""
        url = "https://www.linkedin.com/feed/update/123"
        assert _extract_linkedin_profile(url) == "N/A"

    def test_extract_linkedin_profile_non_linkedin(self):
        """Test _extract_linkedin_profile returns N/A for non-LinkedIn URLs."""
        url = "https://example.com/post"
        assert _extract_linkedin_profile(url) == "N/A"

    def test_extract_linkedin_profile_empty(self):
        """Test _extract_linkedin_profile handles empty input."""
        assert _extract_linkedin_profile("") == "N/A"
        assert _extract_linkedin_profile(None) == "N/A"

    def test_is_verdict_relevant_with_verdict_keyword(self):
        """Test that items with verdict keywords pass relevance check."""
        item = {
            "title": "SHRM Verdict Discussion",
            "snippet": "The jury found SHRM liable for discrimination",
        }
        assert _is_verdict_relevant(item) is True

    def test_is_verdict_relevant_with_trial_keyword(self):
        """Test that items with trial keyword pass relevance check."""
        item = {
            "title": "SHRM Trial Update",
            "snippet": "The trial continues",
        }
        assert _is_verdict_relevant(item) is True

    def test_is_verdict_relevant_with_11_5_keyword(self):
        """Test that items with 11.5 million keyword pass relevance check."""
        item = {
            "title": "SHRM Discrimination Case",
            "snippet": "SHRM ordered to pay 11.5 million in damages",
        }
        assert _is_verdict_relevant(item) is True

    def test_is_verdict_relevant_missing_keywords(self):
        """Test that items without verdict keywords fail relevance check."""
        item = {
            "title": "SHRM General Discussion",
            "snippet": "SHRM is a professional organization",
        }
        assert _is_verdict_relevant(item) is False

    def test_is_verdict_relevant_excluded_robby_starbuck(self):
        """Test that items mentioning Robby Starbuck are excluded."""
        item = {
            "title": "Robby Starbuck and SHRM",
            "snippet": "Discussion about Robby Starbuck and SHRM discrimination",
        }
        assert _is_verdict_relevant(item) is False

    def test_is_verdict_relevant_excluded_starbuck(self):
        """Test that items mentioning Starbuck are excluded."""
        item = {
            "title": "SHRM and Starbuck Case",
            "snippet": "The Starbuck case involves SHRM",
        }
        assert _is_verdict_relevant(item) is False

    def test_is_verdict_relevant_excluded_old_dates(self):
        """Test that items with old dates are excluded."""
        item = {
            "title": "SHRM Discussion",
            "snippet": "SHRM discrimination case from Sep 2025",
        }
        assert _is_verdict_relevant(item) is False

    def test_is_verdict_relevant_excluded_inclusion_conference(self):
        """Test that items mentioning inclusion conference are excluded."""
        item = {
            "title": "SHRM Inclusion Conference",
            "snippet": "SHRM inclusion conference discussion",
        }
        assert _is_verdict_relevant(item) is False

    def test_is_verdict_relevant_case_insensitive(self):
        """Test that relevance check is case-insensitive."""
        item = {
            "title": "SHRM VERDICT Discussion",
            "snippet": "The VERDICT was announced",
        }
        assert _is_verdict_relevant(item) is True

    def test_is_verdict_relevant_keyword_in_snippet_only(self):
        """Test that keywords in snippet are detected."""
        item = {
            "title": "SHRM Discussion",
            "snippet": "The jury found SHRM liable for 11.5 million in damages",
        }
        assert _is_verdict_relevant(item) is True

    def test_relevance_filtering_in_collect(self, monkeypatch):
        """Test that relevance filtering is applied during collection."""
        # Item without verdict keywords - should be filtered
        irrelevant_item = {
            "title": "SHRM General Discussion | LinkedIn",
            "link": "https://www.linkedin.com/posts/user1-activity-1",
            "snippet": "General discussion about SHRM",
        }
        
        # Item with verdict keywords - should pass
        relevant_item = {
            "title": "SHRM Verdict Update | LinkedIn",
            "link": "https://www.linkedin.com/posts/user2-activity-2",
            "snippet": "The jury found SHRM liable for discrimination",
        }

        fake_response = FakeResponse(200, {"items": [irrelevant_item, relevant_item]})

        with patch("requests.get", return_value=fake_response):
            collector = LinkedInGoogleCollector()
            results = collector.collect()

        # Only relevant item should be included
        assert len(results) == 1
        assert results[0]["post_link"] == "https://www.linkedin.com/posts/user2-activity-2"

    def test_relevance_filtering_excludes_robby_starbuck(self, monkeypatch):
        """Test that Robby Starbuck posts are filtered out."""
        starbuck_item = {
            "title": "Robby Starbuck SHRM Case | LinkedIn",
            "link": "https://www.linkedin.com/posts/user-activity-1",
            "snippet": "Discussion about Robby Starbuck and SHRM discrimination",
        }

        fake_response = FakeResponse(200, {"items": [starbuck_item]})

        with patch("requests.get", return_value=fake_response):
            collector = LinkedInGoogleCollector()
            results = collector.collect()

        # Should be filtered out
        assert len(results) == 0

