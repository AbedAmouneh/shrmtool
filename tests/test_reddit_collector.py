"""
Tests for collectors.reddit_collector module (RSS-based).
"""

import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime
import pytz

from collectors.reddit_collector import (
    RedditCollector,
    collect_reddit_posts,
    _strip_html,
    _parse_rss_date,
    _extract_profile_link,
    _clean_reddit_summary,
    DEFAULT_SEARCH_TERMS,
)
from utils.time_utils import UTC, EASTERN


class FakeFeedParser(dict):
    """Fake feedparser.parse() return value (dict-like)."""

    def __init__(self, entries=None):
        super().__init__()
        self["entries"] = entries or []
        self["feed"] = {}


class FakeEntry(dict):
    """Fake RSS entry for testing (dict-like)."""

    def __init__(self, **kwargs):
        super().__init__()
        self["link"] = kwargs.get("link", "")
        self["title"] = kwargs.get("title", "")
        self["summary"] = kwargs.get("summary", "")
        self["description"] = kwargs.get("description", "")
        self["updated"] = kwargs.get("updated", "")
        self["published"] = kwargs.get("published", "")
        self["date"] = kwargs.get("date", "")
        self["author"] = kwargs.get("author", "")
        self["author_detail"] = kwargs.get("author_detail", {})


class FakeResponse:
    """Fake requests.Response for mocking HTTP calls."""

    def __init__(self, status_code=200, content=b""):
        self.status_code = status_code
        self.content = content
        self.text = content.decode("utf-8", errors="ignore")


@pytest.fixture
def sample_rss_entry():
    """Sample RSS entry for testing."""
    return FakeEntry(
        link="https://www.reddit.com/r/HR/comments/abc123/shrm_verdict_discussion/",
        title="SHRM Verdict Discussion",
        summary="<p>The jury found SHRM liable for discrimination. This is a test post.</p>",
        updated="2025-12-12T10:30:00+00:00",
        author="/u/testuser",
    )


class TestRedditCollector:
    """Tests for RedditCollector class."""

    def test_happy_path_collection(self, sample_rss_entry, monkeypatch):
        """Test that RSS entries are collected and normalized correctly."""
        fake_feed = FakeFeedParser(entries=[sample_rss_entry])

        def mock_feedparser_parse(content):
            return fake_feed

        def mock_requests_get(url, headers=None, timeout=None):
            return FakeResponse(200, b"<rss>...</rss>")

        with patch(
            "collectors.reddit_collector.feedparser.parse",
            side_effect=mock_feedparser_parse,
        ), patch(
            "collectors.reddit_collector.requests.get", side_effect=mock_requests_get
        ):
            collector = RedditCollector()
            results = collector.collect()

        assert len(results) == 1
        item = results[0]
        assert item["platform"] == "Reddit-RSS"
        assert (
            item["post_link"]
            == "https://www.reddit.com/r/HR/comments/abc123/shrm_verdict_discussion/"
        )
        assert item["title"] == "SHRM Verdict Discussion"
        assert "jury found SHRM liable" in item["summary"]  # HTML stripped
        assert item["profile_link"] == "https://www.reddit.com/user/testuser"
        assert item["likes"] == "0"
        assert item["comments"] == "0"
        assert item["shares"] == "0"
        assert item["eng_total"] == "0"

    def test_empty_response(self, monkeypatch):
        """Test that empty RSS feeds return empty list."""
        fake_feed = FakeFeedParser(entries=[])

        def mock_feedparser_parse(content):
            return fake_feed

        def mock_requests_get(url, headers=None, timeout=None):
            return FakeResponse(200, b"<rss>...</rss>")

        with patch(
            "collectors.reddit_collector.feedparser.parse",
            side_effect=mock_feedparser_parse,
        ), patch(
            "collectors.reddit_collector.requests.get", side_effect=mock_requests_get
        ):
            collector = RedditCollector()
            results = collector.collect()

        assert results == []

    def test_http_error(self, monkeypatch):
        """Test that HTTP errors are handled gracefully."""

        def mock_requests_get(url, headers=None, timeout=None):
            return FakeResponse(500, b"Server error")

        with patch(
            "collectors.reddit_collector.requests.get", side_effect=mock_requests_get
        ):
            collector = RedditCollector()
            results = collector.collect()

        assert results == []

    def test_network_error(self, monkeypatch):
        """Test that network errors are handled gracefully."""
        with patch(
            "collectors.reddit_collector.requests.get",
            side_effect=Exception("Network error"),
        ):
            collector = RedditCollector()
            results = collector.collect()

        assert results == []

    def test_per_run_deduplication(self, sample_rss_entry, monkeypatch):
        """Test that duplicate URLs within a run are deduplicated."""
        fake_feed = FakeFeedParser(entries=[sample_rss_entry, sample_rss_entry])

        def mock_feedparser_parse(content):
            return fake_feed

        def mock_requests_get(url, headers=None, timeout=None):
            return FakeResponse(200, b"<rss>...</rss>")

        with patch(
            "collectors.reddit_collector.feedparser.parse",
            side_effect=mock_feedparser_parse,
        ), patch(
            "collectors.reddit_collector.requests.get", side_effect=mock_requests_get
        ):
            collector = RedditCollector()
            results = collector.collect()

        # Should only have one result
        assert len(results) == 1

    def test_missing_title_filtered(self, monkeypatch):
        """Test that entries with empty titles and no content are filtered out."""
        entry_no_title = FakeEntry(
            link="https://www.reddit.com/r/test/post1",
            title="",  # Empty title
            summary="",  # No summary either
            updated="2025-12-12T10:30:00+00:00",
        )

        fake_feed = FakeFeedParser(entries=[entry_no_title])

        def mock_feedparser_parse(content):
            return fake_feed

        def mock_requests_get(url, headers=None, timeout=None):
            return FakeResponse(200, b"<rss>...</rss>")

        with patch(
            "collectors.reddit_collector.feedparser.parse",
            side_effect=mock_feedparser_parse,
        ), patch(
            "collectors.reddit_collector.requests.get", side_effect=mock_requests_get
        ):
            collector = RedditCollector()
            results = collector.collect()

        assert len(results) == 0

    def test_missing_date_filtered(self, monkeypatch):
        """Test that entries with missing dates are filtered out."""
        entry_no_date = FakeEntry(
            link="https://www.reddit.com/r/test/post1",
            title="Test Post",
            summary="Test summary",
            updated="",  # Missing date
        )

        fake_feed = FakeFeedParser(entries=[entry_no_date])

        def mock_feedparser_parse(content):
            return fake_feed

        def mock_requests_get(url, headers=None, timeout=None):
            return FakeResponse(200, b"<rss>...</rss>")

        with patch(
            "collectors.reddit_collector.feedparser.parse",
            side_effect=mock_feedparser_parse,
        ), patch(
            "collectors.reddit_collector.requests.get", side_effect=mock_requests_get
        ):
            collector = RedditCollector()
            results = collector.collect()

        assert len(results) == 0

    def test_invalid_url_filtered(self, monkeypatch):
        """Test that entries with invalid URLs are filtered out."""
        entry_invalid_url = FakeEntry(
            link="not-a-valid-url",
            title="Test Post",
            summary="Test summary",
            updated="2025-12-12T10:30:00+00:00",
        )

        fake_feed = FakeFeedParser(entries=[entry_invalid_url])

        def mock_feedparser_parse(content):
            return fake_feed

        def mock_requests_get(url, headers=None, timeout=None):
            return FakeResponse(200, b"<rss>...</rss>")

        with patch(
            "collectors.reddit_collector.feedparser.parse",
            side_effect=mock_feedparser_parse,
        ), patch(
            "collectors.reddit_collector.requests.get", side_effect=mock_requests_get
        ):
            collector = RedditCollector()
            results = collector.collect()

        assert len(results) == 0

    def test_multiple_keywords(self, sample_rss_entry, monkeypatch):
        """Test that multiple keywords are processed."""
        keywords = ["SHRM verdict", "SHRM trial"]

        call_count = [0]

        def mock_feedparser_parse(content):
            return FakeFeedParser(entries=[sample_rss_entry])

        def mock_requests_get(url, headers=None, timeout=None):
            call_count[0] += 1
            return FakeResponse(200, b"<rss>...</rss>")

        with patch(
            "collectors.reddit_collector.feedparser.parse",
            side_effect=mock_feedparser_parse,
        ), patch(
            "collectors.reddit_collector.requests.get", side_effect=mock_requests_get
        ):
            collector = RedditCollector()
            results = collector.collect(keywords=keywords)

        # Should have called requests.get twice (once per keyword)
        assert call_count[0] == 2

    def test_default_keywords(self, monkeypatch):
        """Test that default keywords are used when none provided."""
        call_count = [0]

        def mock_feedparser_parse(content):
            return FakeFeedParser(entries=[])

        def mock_requests_get(url, headers=None, timeout=None):
            call_count[0] += 1
            return FakeResponse(200, b"<rss>...</rss>")

        with patch(
            "collectors.reddit_collector.feedparser.parse",
            side_effect=mock_feedparser_parse,
        ), patch(
            "collectors.reddit_collector.requests.get", side_effect=mock_requests_get
        ):
            collector = RedditCollector()
            collector.collect()  # No keywords provided

        # Should have called with default keywords
        assert call_count[0] == len(DEFAULT_SEARCH_TERMS)

    def test_custom_topic(self, sample_rss_entry, monkeypatch):
        """Test that custom topic is used."""
        fake_feed = FakeFeedParser(entries=[sample_rss_entry])

        def mock_feedparser_parse(content):
            return fake_feed

        def mock_requests_get(url, headers=None, timeout=None):
            return FakeResponse(200, b"<rss>...</rss>")

        with patch(
            "collectors.reddit_collector.feedparser.parse",
            side_effect=mock_feedparser_parse,
        ), patch(
            "collectors.reddit_collector.requests.get", side_effect=mock_requests_get
        ):
            collector = RedditCollector()
            results = collector.collect(topic="Custom Topic")

        assert len(results) == 1
        assert results[0]["topic"] == "Custom Topic"

    def test_schema_compliance(self, sample_rss_entry, monkeypatch):
        """Test that returned items match 17-column schema when converted to rows."""
        from utils.schema import build_row, validate_row

        fake_feed = FakeFeedParser(entries=[sample_rss_entry])

        def mock_feedparser_parse(content):
            return fake_feed

        def mock_requests_get(url, headers=None, timeout=None):
            return FakeResponse(200, b"<rss>...</rss>")

        with patch(
            "collectors.reddit_collector.feedparser.parse",
            side_effect=mock_feedparser_parse,
        ), patch(
            "collectors.reddit_collector.requests.get", side_effect=mock_requests_get
        ):
            collector = RedditCollector()
            results = collector.collect()

        assert len(results) == 1
        item = results[0]

        # Convert to row and validate
        row = build_row(item)
        assert len(row) == 17
        assert validate_row(row)

    def test_user_agent_header(self, sample_rss_entry, monkeypatch):
        """Test that custom User-Agent header is sent."""
        fake_feed = FakeFeedParser(entries=[sample_rss_entry])

        def mock_feedparser_parse(content):
            return fake_feed

        captured_headers = {}

        def mock_requests_get(url, headers=None, timeout=None):
            if headers:
                captured_headers.update(headers)
            return FakeResponse(200, b"<rss>...</rss>")

        with patch(
            "collectors.reddit_collector.feedparser.parse",
            side_effect=mock_feedparser_parse,
        ), patch(
            "collectors.reddit_collector.requests.get", side_effect=mock_requests_get
        ):
            collector = RedditCollector()
            collector.collect()

        assert "User-Agent" in captured_headers
        assert "Mozilla" in captured_headers["User-Agent"]

    def test_backward_compatibility_function(self, sample_rss_entry, monkeypatch):
        """Test that collect_reddit_posts() function still works."""
        fake_feed = FakeFeedParser(entries=[sample_rss_entry])

        def mock_feedparser_parse(content):
            return fake_feed

        def mock_requests_get(url, headers=None, timeout=None):
            return FakeResponse(200, b"<rss>...</rss>")

        with patch(
            "collectors.reddit_collector.feedparser.parse",
            side_effect=mock_feedparser_parse,
        ), patch(
            "collectors.reddit_collector.requests.get", side_effect=mock_requests_get
        ):
            results = collect_reddit_posts()

        assert len(results) == 1
        assert results[0]["platform"] == "Reddit-RSS"


class TestHelperFunctions:
    """Tests for helper functions."""

    def test_strip_html(self):
        """Test HTML stripping."""
        assert _strip_html("<p>Test</p>") == "Test"
        assert _strip_html("<p>Test <b>bold</b> text</p>") == "Test bold text"
        assert _strip_html("&amp; &lt; &gt;") == "& < >"
        assert _strip_html("") == ""
        assert _strip_html(None) == ""

    def test_parse_rss_date(self):
        """Test RSS date parsing."""
        # RFC 3339 format
        dt = _parse_rss_date("2025-12-12T10:30:00+00:00")
        assert dt is not None
        assert dt.tzinfo == UTC

        # ISO format with Z
        dt = _parse_rss_date("2025-12-12T10:30:00Z")
        assert dt is not None

        # Invalid format
        dt = _parse_rss_date("invalid-date")
        assert dt is None

        # Empty string
        dt = _parse_rss_date("")
        assert dt is None

    def test_extract_profile_link(self):
        """Test profile link extraction."""
        assert (
            _extract_profile_link("/u/username")
            == "https://www.reddit.com/user/username"
        )
        assert (
            _extract_profile_link("u/username")
            == "https://www.reddit.com/user/username"
        )
        assert (
            _extract_profile_link("username") == "https://www.reddit.com/user/username"
        )
        assert _extract_profile_link("") == "N/A"
        assert _extract_profile_link(None) == "N/A"

    def test_summary_fallback_to_title(self, monkeypatch):
        """Test that summary falls back to title if summary is empty."""
        entry_no_summary = FakeEntry(
            link="https://www.reddit.com/r/test/post1",
            title="Test Post Title",
            summary="",  # Empty summary
            updated="2025-12-12T10:30:00+00:00",
        )

        fake_feed = FakeFeedParser(entries=[entry_no_summary])

        def mock_feedparser_parse(content):
            return fake_feed

        def mock_requests_get(url, headers=None, timeout=None):
            return FakeResponse(200, b"<rss>...</rss>")

        with patch(
            "collectors.reddit_collector.feedparser.parse",
            side_effect=mock_feedparser_parse,
        ), patch(
            "collectors.reddit_collector.requests.get", side_effect=mock_requests_get
        ):
            collector = RedditCollector()
            results = collector.collect()

        assert len(results) == 1
        # Summary should fallback to title (truncated to 300 chars)
        assert "Test Post Title" in results[0]["summary"]


class TestCleanRedditSummary:
    """Tests for _clean_reddit_summary function."""

    def test_cleans_basic_boilerplate(self):
        """Test that basic Reddit RSS boilerplate is removed."""
        entry = {
            "summary": "submitted by /u/testuser to r/HR [link] [comments]",
        }
        result = _clean_reddit_summary(entry, "Test Title")
        # Should return title since cleaned summary is too short
        assert result == "Test Title"

    def test_cleans_boilerplate_with_real_content(self):
        """Test that boilerplate is removed but real content is preserved."""
        entry = {
            "summary": "submitted by /u/testuser to r/HR This is the actual content about SHRM verdict [link] [comments]",
        }
        result = _clean_reddit_summary(entry, "Test Title")
        # Real content should remain
        assert "actual content about SHRM verdict" in result
        # Boilerplate should be removed
        assert "submitted by" not in result.lower()
        assert "[link]" not in result.lower()
        assert "[comments]" not in result.lower()

    def test_prefers_content_over_summary(self):
        """Test that entry.content is preferred over entry.summary."""
        entry = {
            "summary": "submitted by /u/user to r/subreddit [link] [comments]",
            "content": [
                {"value": "<p>This is the real content from the post body</p>"}
            ],
        }
        result = _clean_reddit_summary(entry, "Test Title")
        # Should use content, not summary
        assert "real content from the post body" in result

    def test_strips_html_from_content(self):
        """Test that HTML tags are stripped from content."""
        entry = {
            "content": [{"value": "<p>This is <b>bold</b> and <i>italic</i> text</p>"}],
        }
        result = _clean_reddit_summary(entry, "Test Title")
        # HTML should be stripped
        assert "<p>" not in result
        assert "<b>" not in result
        assert "bold" in result
        assert "italic" in result

    def test_removes_standalone_user_mention(self):
        """Test that standalone /u/username mentions are removed."""
        entry = {
            "summary": "Check out /u/someuser's post about this topic which is interesting",
        }
        result = _clean_reddit_summary(entry, "Test Title")
        # User mention should be removed
        assert "/u/someuser" not in result
        assert "Check out" in result  # Other content preserved

    def test_removes_subreddit_mention(self):
        """Test that standalone r/subreddit mentions are removed."""
        entry = {
            "summary": "Discussion on r/humanresources about this important topic in detail",
        }
        result = _clean_reddit_summary(entry, "Test Title")
        # Subreddit mention should be removed
        assert "r/humanresources" not in result
        assert "important topic" in result  # Other content preserved

    def test_uses_title_when_cleaned_summary_too_short(self):
        """Test that title is used when cleaned summary is under 20 chars."""
        entry = {
            "summary": "[link] [comments]",  # Just boilerplate, nothing else
        }
        result = _clean_reddit_summary(entry, "This Is A Good Title For The Post")
        # Should fall back to title
        assert result == "This Is A Good Title For The Post"

    def test_handles_empty_entry(self):
        """Test that empty entries fall back to title."""
        entry = {}
        result = _clean_reddit_summary(entry, "Fallback Title")
        assert result == "Fallback Title"

    def test_handles_missing_content_value(self):
        """Test that malformed content falls back correctly."""
        entry = {
            "content": [{}],  # Content exists but no value
            "summary": "Real summary text that should be used here for the post",
        }
        result = _clean_reddit_summary(entry, "Test Title")
        # Should fall back to summary
        assert "Real summary text" in result

    def test_normalizes_whitespace(self):
        """Test that extra whitespace is normalized."""
        entry = {
            "summary": "This   has    multiple     spaces   and should be normalized properly",
        }
        result = _clean_reddit_summary(entry, "Test Title")
        # Multiple spaces should be collapsed
        assert "   " not in result
        assert "multiple spaces" in result

    def test_integration_in_collector(self, monkeypatch):
        """Test that boilerplate cleaning works in the full collection flow."""
        entry_with_boilerplate = FakeEntry(
            link="https://www.reddit.com/r/HR/comments/abc123",
            title="SHRM Verdict Discussion - Important Update",
            summary="submitted by /u/testuser to r/HR [link] [comments]",  # Just boilerplate
            updated="2025-12-12T10:30:00+00:00",
        )

        fake_feed = FakeFeedParser(entries=[entry_with_boilerplate])

        def mock_feedparser_parse(content):
            return fake_feed

        def mock_requests_get(url, headers=None, timeout=None):
            return FakeResponse(200, b"<rss>...</rss>")

        with patch(
            "collectors.reddit_collector.feedparser.parse",
            side_effect=mock_feedparser_parse,
        ), patch(
            "collectors.reddit_collector.requests.get", side_effect=mock_requests_get
        ):
            collector = RedditCollector()
            results = collector.collect()

        assert len(results) == 1
        # Summary should be the title since boilerplate was cleaned out
        assert results[0]["summary"] == "SHRM Verdict Discussion - Important Update"
        # Boilerplate should NOT be in summary
        assert "submitted by" not in results[0]["summary"].lower()
        assert "[link]" not in results[0]["summary"].lower()
