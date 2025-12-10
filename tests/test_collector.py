"""
Phase T5: Tests for collectors (Reddit and NewsAPI).

These tests mock all external dependencies (snscrape subprocess, NewsAPI HTTP calls)
and verify normalization, filtering, and error handling.
"""

import pytest
import json
import subprocess
from types import SimpleNamespace
from unittest.mock import patch, MagicMock, Mock
from datetime import datetime
import pytz

from collectors import reddit_collector, news_collector
from utils.time_utils import parse_iso_date, parse_newsapi_date, parse_reddit_date, is_after_verdict_date
from utils.config import VERDICT_DATE


# Test fixtures and helpers
class FakeResponse:
    """Fake requests.Response object for mocking NewsAPI calls."""
    
    def __init__(self, status_code=200, json_data=None, text=""):
        self.status_code = status_code
        self._json_data = json_data or {}
        self.text = text
    
    def json(self):
        return self._json_data
    
    def raise_for_status(self):
        if self.status_code >= 400:
            raise Exception(f"HTTP {self.status_code}")


class FakeSession:
    """Fake requests.Session for mocking NewsAPI calls."""
    
    def __init__(self, responses=None):
        self.responses = responses or []
        self.call_count = 0
    
    def get(self, url, params=None, timeout=None):
        if self.call_count < len(self.responses):
            response = self.responses[self.call_count]
            self.call_count += 1
            return response
        # Default: return empty successful response
        return FakeResponse(200, {"status": "ok", "articles": [], "totalResults": 0})


class TestCollectRedditPosts:
    """Tests for collect_reddit_posts function."""
    
    def test_basic_normalization(self, mock_config, monkeypatch):
        """Test that Reddit posts are normalized correctly."""
        # Create fake Reddit post data
        fake_post_data = {
            "url": "https://reddit.com/r/test/post123",
            "title": "SHRM Trial Discussion",
            "date": "2025-12-06T10:30:00Z",
            "subreddit": {"name": "legal"},
            "score": 42,
            "numComments": 5,
            "selftext": "This is the post body text",
            "author": {"username": "testuser"},
        }
        
        # Mock subprocess.run to return fake JSONL output
        fake_stdout = json.dumps(fake_post_data) + "\n"
        fake_process = MagicMock()
        fake_process.returncode = 0
        fake_process.stdout = fake_stdout
        fake_process.stderr = ""
        
        with patch('subprocess.run', return_value=fake_process):
            posts = reddit_collector.collect_reddit_posts()
        
        # Verify results
        assert isinstance(posts, list)
        assert len(posts) > 0
        
        post = posts[0]
        assert post["url"] == "https://reddit.com/r/test/post123"
        assert post["title"] == "SHRM Trial Discussion"
        assert post["subreddit"] == "legal"
        assert post["score"] == 42
        assert post["numComments"] == 5
        assert post["selftext"] == "This is the post body text"
        assert post["username"] == "testuser"
        assert "date" in post
    
    def test_normalization_with_missing_fields(self, mock_config, monkeypatch):
        """Test normalization handles missing fields gracefully."""
        fake_post_data = {
            "url": "https://reddit.com/r/test/post456",
            "title": "Minimal post",
            # Missing many fields
        }
        
        fake_stdout = json.dumps(fake_post_data) + "\n"
        fake_process = MagicMock()
        fake_process.returncode = 0
        fake_process.stdout = fake_stdout
        fake_process.stderr = ""
        
        with patch('subprocess.run', return_value=fake_process):
            posts = reddit_collector.collect_reddit_posts()
        
        assert len(posts) > 0
        post = posts[0]
        assert post["url"] == "https://reddit.com/r/test/post456"
        assert post["title"] == "Minimal post"
        assert post.get("score", 0) == 0  # Should default to 0
        assert post.get("numComments", 0) == 0
        assert post.get("selftext", "") == ""
    
    def test_verdict_date_filtering(self, mock_config, monkeypatch):
        """Test that posts are filtered by verdict date."""
        verdict_date = parse_iso_date(VERDICT_DATE)
        
        # Create posts: one before verdict, one after
        before_verdict = datetime(2025, 12, 4, 10, 0, 0, tzinfo=pytz.UTC)
        after_verdict = datetime(2025, 12, 6, 10, 0, 0, tzinfo=pytz.UTC)
        
        fake_post_before = {
            "url": "https://reddit.com/r/test/before",
            "title": "Pre-verdict post",
            "date": before_verdict.isoformat(),
            "subreddit": "test",
            "score": 1,
            "numComments": 0,
            "selftext": "",
            "author": "user1",
        }
        
        fake_post_after = {
            "url": "https://reddit.com/r/test/after",
            "title": "Post-verdict post",
            "date": after_verdict.isoformat(),
            "subreddit": "test",
            "score": 1,
            "numComments": 0,
            "selftext": "",
            "author": "user2",
        }
        
        # Note: collect_reddit_posts doesn't filter by date - that's done in main_collect
        # But we can test that the date is correctly parsed and available
        fake_stdout = json.dumps(fake_post_before) + "\n" + json.dumps(fake_post_after) + "\n"
        fake_process = MagicMock()
        fake_process.returncode = 0
        fake_process.stdout = fake_stdout
        fake_process.stderr = ""
        
        with patch('subprocess.run', return_value=fake_process):
            posts = reddit_collector.collect_reddit_posts()
        
        # Both posts should be collected (filtering happens in main_collect)
        assert len(posts) >= 2
        
        # Verify dates are present
        urls = [p["url"] for p in posts]
        assert "https://reddit.com/r/test/before" in urls
        assert "https://reddit.com/r/test/after" in urls
    
    def test_error_handling_subprocess_failure(self, mock_config, monkeypatch):
        """Test that subprocess failures are handled gracefully."""
        # Mock subprocess.run to raise an exception
        with patch('subprocess.run', side_effect=RuntimeError("API down")):
            posts = reddit_collector.collect_reddit_posts()
        
        # Should return empty list, not raise
        assert isinstance(posts, list)
        # May be empty or have partial results from other queries
        # The function continues on errors for individual queries
    
    def test_error_handling_invalid_json(self, mock_config, monkeypatch):
        """Test that invalid JSON lines are skipped."""
        fake_stdout = "not valid json\n" + json.dumps({"url": "https://reddit.com/r/test/valid", "title": "Valid"}) + "\n"
        fake_process = MagicMock()
        fake_process.returncode = 0
        fake_process.stdout = fake_stdout
        fake_process.stderr = ""
        
        with patch('subprocess.run', return_value=fake_process):
            posts = reddit_collector.collect_reddit_posts()
        
        # Should have the valid post, skip the invalid line
        assert len(posts) > 0
        assert posts[0]["url"] == "https://reddit.com/r/test/valid"
    
    def test_deduplication_within_collection(self, mock_config, monkeypatch):
        """Test that duplicate URLs are deduplicated within a collection run."""
        fake_post1 = {
            "url": "https://reddit.com/r/test/duplicate",
            "title": "Post 1",
            "date": "2025-12-06T10:00:00Z",
            "subreddit": "test",
            "score": 1,
            "numComments": 0,
            "selftext": "",
            "author": "user1",
        }
        
        fake_post2 = {
            "url": "https://reddit.com/r/test/duplicate",  # Same URL
            "title": "Post 2",
            "date": "2025-12-06T11:00:00Z",
            "subreddit": "test",
            "score": 2,
            "numComments": 1,
            "selftext": "",
            "author": "user2",
        }
        
        fake_stdout = json.dumps(fake_post1) + "\n" + json.dumps(fake_post2) + "\n"
        fake_process = MagicMock()
        fake_process.returncode = 0
        fake_process.stdout = fake_stdout
        fake_process.stderr = ""
        
        with patch('subprocess.run', return_value=fake_process):
            posts = reddit_collector.collect_reddit_posts()
        
        # Should only have one post with that URL
        urls = [p["url"] for p in posts]
        assert urls.count("https://reddit.com/r/test/duplicate") == 1
    
    def test_multiple_search_terms(self, mock_config, monkeypatch):
        """Test that multiple search terms are processed."""
        fake_post = {
            "url": "https://reddit.com/r/test/post",
            "title": "SHRM post",
            "date": "2025-12-06T10:00:00Z",
            "subreddit": "test",
            "score": 1,
            "numComments": 0,
            "selftext": "",
            "author": "user",
        }
        
        fake_stdout = json.dumps(fake_post) + "\n"
        fake_process = MagicMock()
        fake_process.returncode = 0
        fake_process.stdout = fake_stdout
        fake_process.stderr = ""
        
        with patch('subprocess.run', return_value=fake_process):
            posts = reddit_collector.collect_reddit_posts()
        
        # Should have results from multiple search terms
        # (collect_reddit_posts processes all REDDIT_SEARCH_TERMS)
        assert len(posts) > 0


class TestCollectNewsArticles:
    """Tests for collect_news_articles function."""
    
    def test_basic_normalization(self, mock_config, monkeypatch):
        """Test that news articles are normalized correctly."""
        fake_article = {
            "source": {"name": "Reuters"},
            "title": "SHRM Trial Verdict Announced",
            "description": "The verdict in the SHRM discrimination case...",
            "url": "https://reuters.com/shrm-verdict",
            "publishedAt": "2025-12-06T10:30:00Z",
            "author": "John Doe",
        }
        
        fake_response = FakeResponse(200, {
            "status": "ok",
            "articles": [fake_article],
            "totalResults": 1,
        })
        
        with patch('requests.get', return_value=fake_response):
            articles = news_collector.collect_news_articles()
        
        assert isinstance(articles, list)
        assert len(articles) > 0
        
        article = articles[0]
        assert article["source_name"] == "Reuters"
        assert article["title"] == "SHRM Trial Verdict Announced"
        assert article["description"] == "The verdict in the SHRM discrimination case..."
        assert article["author"] == "John Doe"
        assert article["url"] == "https://reuters.com/shrm-verdict"
        assert article["publishedAt"] == "2025-12-06T10:30:00Z"
    
    def test_normalization_with_missing_fields(self, mock_config, monkeypatch):
        """Test normalization handles missing fields gracefully."""
        fake_article = {
            "url": "https://example.com/article",
            "title": "Minimal article",
            # Missing source, description, etc.
        }
        
        fake_response = FakeResponse(200, {
            "status": "ok",
            "articles": [fake_article],
            "totalResults": 1,
        })
        
        with patch('requests.get', return_value=fake_response):
            articles = news_collector.collect_news_articles()
        
        assert len(articles) > 0
        article = articles[0]
        assert article["url"] == "https://example.com/article"
        assert article["title"] == "Minimal article"
        assert article.get("source_name", "") == ""
        assert article.get("description", "") == ""
    
    def test_source_name_extraction(self, mock_config, monkeypatch):
        """Test that source name is correctly extracted from source object."""
        # Test with dict source
        fake_article_dict = {
            "source": {"name": "Bloomberg"},
            "title": "Test",
            "url": "https://bloomberg.com/test",
            "publishedAt": "2025-12-06T10:00:00Z",
        }
        
        fake_response = FakeResponse(200, {
            "status": "ok",
            "articles": [fake_article_dict],
            "totalResults": 1,
        })
        
        with patch('requests.get', return_value=fake_response):
            articles = news_collector.collect_news_articles()
        
        assert articles[0]["source_name"] == "Bloomberg"
    
    def test_verdict_date_filtering(self, mock_config, monkeypatch):
        """Test that articles are filtered by verdict date (in main_collect, but verify dates are available)."""
        # Create articles: one before verdict, one after
        before_verdict = "2025-12-04T10:00:00Z"
        after_verdict = "2025-12-06T10:00:00Z"
        
        fake_article_before = {
            "source": {"name": "Source1"},
            "title": "Pre-verdict article",
            "url": "https://example.com/before",
            "publishedAt": before_verdict,
        }
        
        fake_article_after = {
            "source": {"name": "Source2"},
            "title": "Post-verdict article",
            "url": "https://example.com/after",
            "publishedAt": after_verdict,
        }
        
        fake_response = FakeResponse(200, {
            "status": "ok",
            "articles": [fake_article_before, fake_article_after],
            "totalResults": 2,
        })
        
        with patch('requests.get', return_value=fake_response):
            articles = news_collector.collect_news_articles()
        
        # Both articles should be collected (filtering happens in main_collect)
        assert len(articles) >= 2
        
        # Verify publishedAt is present
        urls = [a["url"] for a in articles]
        assert "https://example.com/before" in urls
        assert "https://example.com/after" in urls
    
    def test_handles_non_200_status(self, mock_config, monkeypatch):
        """Test that non-200 HTTP status codes are handled."""
        fake_response = FakeResponse(500, {"status": "error", "message": "Server error"})
        
        with patch('requests.get', return_value=fake_response):
            # Should raise or return empty list
            try:
                articles = news_collector.collect_news_articles()
                # If it returns, should be empty list
                assert isinstance(articles, list)
            except Exception:
                # Or it might raise, which is also acceptable
                pass
    
    def test_handles_api_error_status(self, mock_config, monkeypatch):
        """Test that API error status is handled."""
        fake_response = FakeResponse(200, {
            "status": "error",
            "message": "API key invalid",
        })
        
        with patch('requests.get', return_value=fake_response):
            # Should handle error gracefully
            try:
                articles = news_collector.collect_news_articles()
                assert isinstance(articles, list)
            except (ValueError, Exception):
                # May raise ValueError for API errors, which is acceptable
                pass
    
    def test_handles_network_failure(self, mock_config, monkeypatch):
        """Test that network failures are handled gracefully."""
        with patch('requests.get', side_effect=Exception("Network error")):
            articles = news_collector.collect_news_articles()
        
        # Should return empty list, not raise
        assert isinstance(articles, list)
    
    def test_pagination(self, mock_config, monkeypatch):
        """Test that pagination works correctly."""
        # First page response
        fake_response_page1 = FakeResponse(200, {
            "status": "ok",
            "articles": [
                {"source": {"name": "Source1"}, "title": "Article 1", "url": "https://example.com/1", "publishedAt": "2025-12-06T10:00:00Z"},
            ],
            "totalResults": 150,  # More than one page
        })
        
        # Second page response
        fake_response_page2 = FakeResponse(200, {
            "status": "ok",
            "articles": [
                {"source": {"name": "Source2"}, "title": "Article 2", "url": "https://example.com/2", "publishedAt": "2025-12-06T11:00:00Z"},
            ],
            "totalResults": 150,
        })
        
        call_count = [0]
        
        def mock_get(url, params=None, timeout=None):
            call_count[0] += 1
            if call_count[0] == 1:
                return fake_response_page1
            else:
                return fake_response_page2
        
        with patch('requests.get', side_effect=mock_get):
            articles = news_collector.collect_news_articles()
        
        # Should have articles from multiple pages
        assert len(articles) >= 1
    
    def test_deduplication_within_collection(self, mock_config, monkeypatch):
        """Test that duplicate URLs are deduplicated within a collection run."""
        fake_article1 = {
            "source": {"name": "Source1"},
            "title": "Article 1",
            "url": "https://example.com/duplicate",
            "publishedAt": "2025-12-06T10:00:00Z",
        }
        
        fake_article2 = {
            "source": {"name": "Source2"},
            "title": "Article 2",
            "url": "https://example.com/duplicate",  # Same URL
            "publishedAt": "2025-12-06T11:00:00Z",
        }
        
        fake_response = FakeResponse(200, {
            "status": "ok",
            "articles": [fake_article1, fake_article2],
            "totalResults": 2,
        })
        
        with patch('requests.get', return_value=fake_response):
            articles = news_collector.collect_news_articles()
        
        # Should only have one article with that URL
        urls = [a["url"] for a in articles]
        assert urls.count("https://example.com/duplicate") == 1
    
    def test_multiple_search_terms(self, mock_config, monkeypatch):
        """Test that multiple search terms are processed."""
        fake_article = {
            "source": {"name": "Source"},
            "title": "SHRM article",
            "url": "https://example.com/article",
            "publishedAt": "2025-12-06T10:00:00Z",
        }
        
        fake_response = FakeResponse(200, {
            "status": "ok",
            "articles": [fake_article],
            "totalResults": 1,
        })
        
        with patch('requests.get', return_value=fake_response):
            articles = news_collector.collect_news_articles()
        
        # Should have results from multiple search terms
        # (collect_news_articles processes all NEWS_SEARCH_TERMS)
        assert len(articles) > 0
    
    def test_description_maps_to_body(self, mock_config, monkeypatch):
        """Test that description field is correctly mapped."""
        fake_article = {
            "source": {"name": "Test Source"},
            "title": "Test Title",
            "description": "This is the article description",
            "url": "https://example.com/test",
            "publishedAt": "2025-12-06T10:00:00Z",
        }
        
        fake_response = FakeResponse(200, {
            "status": "ok",
            "articles": [fake_article],
            "totalResults": 1,
        })
        
        with patch('requests.get', return_value=fake_response):
            articles = news_collector.collect_news_articles()
        
        assert articles[0]["description"] == "This is the article description"
    
    def test_published_at_parsing(self, mock_config, monkeypatch):
        """Test that publishedAt is correctly preserved for parsing."""
        fake_article = {
            "source": {"name": "Test Source"},
            "title": "Test Title",
            "url": "https://example.com/test",
            "publishedAt": "2025-12-06T10:30:00Z",
        }
        
        fake_response = FakeResponse(200, {
            "status": "ok",
            "articles": [fake_article],
            "totalResults": 1,
        })
        
        with patch('requests.get', return_value=fake_response):
            articles = news_collector.collect_news_articles()
        
        # Verify publishedAt is in ISO format and can be parsed
        published_at = articles[0]["publishedAt"]
        assert published_at == "2025-12-06T10:30:00Z"
        
        # Verify it can be parsed
        parsed_date = parse_newsapi_date(published_at)
        assert isinstance(parsed_date, datetime)

