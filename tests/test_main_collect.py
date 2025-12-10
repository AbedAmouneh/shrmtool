"""
Phase T6: Tests for main_collect orchestrator.

Tests the complete orchestration flow: collection, normalization, filtering,
deduplication, and Google Sheets appending.
"""

import pytest
from unittest.mock import patch, MagicMock, Mock
from datetime import datetime
import pytz

import main_collect
from utils.time_utils import parse_iso_date, format_date_mmddyyyy
from utils.config import VERDICT_DATE


class TestMainCollectHappyPath:
    """T6.1: Happy path with mixed sources."""

    def test_happy_path_mixed_sources(self, mock_config, mock_canonical_dedupe):
        """Test successful collection and appending of Reddit and News items."""
        # Mock Reddit post
        reddit_post = {
            "url": "https://reddit.com/r/HR/comments/1",
            "title": "Reddit post about SHRM",
            "username": "testuser",
            "score": 10,
            "numComments": 5,
            "date": "2025-12-06T10:30:00Z",
            "selftext": "This is a post about SHRM trial",
            "subreddit": "HR",
        }

        # Mock News article
        news_article = {
            "url": "https://news.com/article/1",
            "title": "News article about SHRM",
            "description": "This is a news article about the SHRM verdict",
            "source_name": "Reuters",
            "publishedAt": "2025-12-06T11:00:00Z",
        }

        with patch(
            "main_collect.collect_reddit_posts", return_value=[reddit_post]
        ) as mock_reddit, patch(
            "main_collect.collect_news_articles", return_value=[news_article]
        ) as mock_news, patch(
            "main_collect.has_seen", return_value=False
        ), patch(
            "main_collect.has_seen_canonical", return_value=(False, None)
        ), patch(
            "main_collect.has_seen_canonical_by_platform", return_value=False
        ), patch(
            "main_collect.append_rows"
        ) as mock_append, patch(
            "main_collect.mark_seen"
        ) as mock_mark_seen, patch(
            "main_collect.mark_seen_canonical"
        ) as mock_mark_seen_canonical:

            count = main_collect.main_collect(["SHRM", "verdict"], "SHRM Trial Verdict")

        # Verify collectors were called
        mock_reddit.assert_called_once()
        mock_news.assert_called_once()

        # Verify append_rows was called with 2 rows
        assert mock_append.call_count == 1
        rows = mock_append.call_args[0][0]
        assert len(rows) == 2

        # Verify row structure (17 columns)
        for row in rows:
            assert len(row) == 17

        # Verify first row is Reddit (new schema: Date Posted, Platform, Profile Link, N° of Followers, Post Link, Topic title, Summary, Tone, Category, Views, Likes, Comments, Shares, Eng. Total, Sentiment Score, Verified, Notes)
        reddit_row = rows[0]
        assert reddit_row[1] == "Reddit"  # Platform
        assert (
            "testuser" in reddit_row[2] or "reddit.com" in reddit_row[2]
        )  # Profile Link
        assert reddit_row[5] == "SHRM Trial Verdict"  # Topic title (contains topic)
        assert "Reddit post about SHRM" in str(reddit_row[6]) or "SHRM trial" in str(
            reddit_row[6]
        )  # Summary (contains title or selftext)
        assert reddit_row[10] == "10"  # Likes
        assert reddit_row[11] == "5"  # Comments
        assert reddit_row[13] == "15"  # Eng. Total (10 + 5 + 0)

        # Verify second row is News
        news_row = rows[1]
        assert news_row[1] == "News"  # Platform
        assert news_row[5] == "SHRM Trial Verdict"  # Topic title
        assert "News article about SHRM" in str(news_row[6]) or "SHRM verdict" in str(
            news_row[6]
        )  # Summary

        # Verify mark_seen was called with both URLs
        mock_mark_seen.assert_called_once()
        marked_urls = mock_mark_seen.call_args[0][0]
        assert len(marked_urls) == 2
        assert "https://reddit.com/r/HR/comments/1" in marked_urls
        assert "https://news.com/article/1" in marked_urls

        # Verify return value
        assert count == 2


class TestMainCollectDeduplication:
    """T6.2: Deduplication tests."""

    def test_deduplication_same_url(self, mock_config, mock_canonical_dedupe):
        """Duplicates for same platform/profile are skipped."""
        reddit_post_1 = {
            "url": "https://reddit.com/r/test/post1",
            "title": "SHRM trial discussion",
            "username": "user1",
            "score": 1,
            "numComments": 0,
            "date": "2025-12-06T10:00:00Z",
            "selftext": "SHRM verdict update",
            "subreddit": "test",
        }

        reddit_post_2 = {
            "url": "https://reddit.com/r/test/post1",  # same URL/profile
            "title": "Duplicate post",
            "username": "user1",
            "score": 2,
            "numComments": 1,
            "date": "2025-12-06T11:00:00Z",
            "selftext": "Duplicate content",
            "subreddit": "test",
        }

        # Simulate first unseen, second seen for canonical dedupe
        has_seen_calls = [(False, None), (True, "https://reddit.com/r/test/post1")]

        def mock_has_seen_canonical(canonical_url, platform, profile=None):
            return has_seen_calls.pop(0) if has_seen_calls else (True, canonical_url)

        with patch(
            "main_collect.collect_reddit_posts",
            return_value=[reddit_post_1, reddit_post_2],
        ), patch("main_collect.collect_news_articles", return_value=[]), patch(
            "main_collect.has_seen_canonical", side_effect=mock_has_seen_canonical
        ), patch(
            "main_collect.has_seen_canonical_by_platform", return_value=False
        ), patch(
            "main_collect.append_rows"
        ) as mock_append, patch(
            "main_collect.mark_seen"
        ) as mock_mark_seen:

            count = main_collect.main_collect(["test"], "Test Topic")

        # Should only append one row
        assert mock_append.call_count == 1
        rows = mock_append.call_args[0][0]
        assert len(rows) == 1

        # Should mark only the first URL as seen
        mock_mark_seen.assert_called_once()
        marked_urls = mock_mark_seen.call_args[0][0]
        assert len(marked_urls) == 1
        assert "https://reddit.com/r/test/post1" in marked_urls

        assert count == 1

    def test_deduplication_already_seen(self, mock_config):
        """Test that already-seen URLs are skipped."""
        reddit_post = {
            "url": "https://reddit.com/r/test/post1",
            "title": "Post 1",
            "username": "user1",
            "score": 1,
            "numComments": 0,
            "date": "2025-12-06T10:00:00Z",
            "selftext": "",
            "subreddit": "test",
        }

        # Already seen - should skip
        with patch(
            "main_collect.collect_reddit_posts", return_value=[reddit_post]
        ), patch("main_collect.collect_news_articles", return_value=[]), patch(
            "main_collect.has_seen", return_value=True
        ), patch(
            "main_collect.append_rows"
        ) as mock_append, patch(
            "main_collect.mark_seen"
        ) as mock_mark_seen:

            count = main_collect.main_collect(["test"], "Test Topic")

        # Should not append anything
        mock_append.assert_not_called()
        mock_mark_seen.assert_not_called()

        assert count == 0


class TestMainCollectDateFiltering:
    """T6.3: Date filtering tests."""

    def test_date_filtering_before_verdict(self, mock_config, mock_canonical_dedupe):
        """Test that posts before verdict date are filtered out."""
        verdict_date = parse_iso_date(VERDICT_DATE)  # e.g., 2025-12-05

        # Post before verdict
        post_before = {
            "url": "https://reddit.com/r/test/before",
            "title": "Pre-verdict SHRM discussion",
            "username": "user1",
            "score": 1,
            "numComments": 0,
            "date": "2025-12-04T10:00:00Z",  # Before verdict
            "selftext": "",
            "subreddit": "test",
        }

        # Post after verdict
        post_after = {
            "url": "https://reddit.com/r/test/after",
            "title": "Post-verdict SHRM trial update",
            "username": "user2",
            "score": 1,
            "numComments": 0,
            "date": "2025-12-06T10:00:00Z",  # After verdict
            "selftext": "",
            "subreddit": "test",
        }

        with patch(
            "main_collect.collect_reddit_posts", return_value=[post_before, post_after]
        ), patch("main_collect.collect_news_articles", return_value=[]), patch(
            "main_collect.has_seen", return_value=False
        ), patch(
            "main_collect.append_rows"
        ) as mock_append, patch(
            "main_collect.mark_seen"
        ) as mock_mark_seen:

            count = main_collect.main_collect(["test"], "Test Topic")

        # Should only append the post after verdict
        assert mock_append.call_count == 1
        rows = mock_append.call_args[0][0]
        assert len(rows) == 1

        # Verify it's the post-verdict one
        assert rows[0][4] == "https://reddit.com/r/test/after"  # Post Link

        # Should mark only the after-verdict URL
        mock_mark_seen.assert_called_once()
        marked_urls = mock_mark_seen.call_args[0][0]
        assert len(marked_urls) == 1
        assert "https://reddit.com/r/test/after" in marked_urls

        assert count == 1


class TestMainCollectEmptyResults:
    """T6.4: Empty results handling."""

    def test_empty_results(self, mock_config):
        """Test that empty results don't call append_rows."""
        with patch("main_collect.collect_reddit_posts", return_value=[]), patch(
            "main_collect.collect_news_articles", return_value=[]
        ), patch("main_collect.append_rows") as mock_append, patch(
            "main_collect.mark_seen"
        ) as mock_mark_seen:

            count = main_collect.main_collect(["test"], "Test Topic")

        # Should not append anything
        mock_append.assert_not_called()
        mock_mark_seen.assert_not_called()

        assert count == 0


class TestMainCollectErrorHandling:
    """T6.5: Error handling tests."""

    def test_reddit_collector_raises(self, mock_config, mock_canonical_dedupe):
        """Test that Reddit collector failure doesn't stop News collection."""
        news_article = {
            "url": "https://news.com/article/1",
            "title": "SHRM news article",
            "description": "Society for Human Resource Management update",
            "source_name": "Source1",
            "publishedAt": "2025-12-06T10:00:00Z",
        }

        with patch(
            "main_collect.collect_reddit_posts",
            side_effect=RuntimeError("Reddit API down"),
        ), patch(
            "main_collect.collect_news_articles", return_value=[news_article]
        ), patch(
            "main_collect.has_seen", return_value=False
        ), patch(
            "main_collect.append_rows"
        ) as mock_append, patch(
            "main_collect.mark_seen"
        ) as mock_mark_seen:

            count = main_collect.main_collect(["test"], "Test Topic")

        # Should still append the News article
        assert mock_append.call_count == 1
        rows = mock_append.call_args[0][0]
        assert len(rows) == 1
        assert rows[0][1] == "News"  # Platform

        mock_mark_seen.assert_called_once()
        marked_urls = mock_mark_seen.call_args[0][0]
        assert "https://news.com/article/1" in marked_urls

        assert count == 1

    def test_news_collector_raises(self, mock_config, mock_canonical_dedupe):
        """Test that News collector failure doesn't stop Reddit collection."""
        reddit_post = {
            "url": "https://reddit.com/r/test/post1",
            "title": "SHRM CEO statement",
            "username": "user1",
            "score": 1,
            "numComments": 0,
            "date": "2025-12-06T10:00:00Z",
            "selftext": "",
            "subreddit": "test",
        }

        with patch(
            "main_collect.collect_reddit_posts", return_value=[reddit_post]
        ), patch(
            "main_collect.collect_news_articles",
            side_effect=RuntimeError("News API down"),
        ), patch(
            "main_collect.has_seen", return_value=False
        ), patch(
            "main_collect.append_rows"
        ) as mock_append, patch(
            "main_collect.mark_seen"
        ) as mock_mark_seen:

            count = main_collect.main_collect(["test"], "Test Topic")

        # Should still append the Reddit post
        assert mock_append.call_count == 1
        rows = mock_append.call_args[0][0]
        assert len(rows) == 1
        assert rows[0][1] == "Reddit"  # Platform

        mock_mark_seen.assert_called_once()
        marked_urls = mock_mark_seen.call_args[0][0]
        assert "https://reddit.com/r/test/post1" in marked_urls

        assert count == 1


class TestMainCollectNumericMapping:
    """T6.6: Numeric mapping and eng_total tests."""

    def test_numeric_mapping_and_eng_total(self, mock_config, mock_canonical_dedupe):
        """Test that numeric fields are correctly mapped and eng_total is computed."""
        reddit_post = {
            "url": "https://reddit.com/r/test/post1",
            "title": "SHRM post with engagement",
            "username": "user1",
            "score": 10,  # Likes
            "numComments": 5,  # Comments
            "date": "2025-12-06T10:00:00Z",
            "selftext": "SHRM post body",
            "subreddit": "test",
        }

        with patch(
            "main_collect.collect_reddit_posts", return_value=[reddit_post]
        ), patch("main_collect.collect_news_articles", return_value=[]), patch(
            "main_collect.has_seen", return_value=False
        ), patch(
            "main_collect.append_rows"
        ) as mock_append, patch(
            "main_collect.mark_seen"
        ):

            count = main_collect.main_collect(["test"], "Test Topic")

        # Verify row structure
        assert mock_append.call_count == 1
        rows = mock_append.call_args[0][0]
        assert len(rows) == 1

        row = rows[0]
        assert len(row) == 17

        # Verify numeric fields
        assert row[10] == "10"  # Likes
        assert row[11] == "5"  # Comments
        assert row[12] == "0"  # Shares
        assert row[13] == "15"  # Eng. Total (10 + 5 + 0)

        assert count == 1

    def test_eng_total_with_none_values(self, mock_config, mock_canonical_dedupe):
        """Test eng_total calculation when some values are None."""
        reddit_post = {
            "url": "https://reddit.com/r/test/post1",
            "title": "SHRM post with None values",
            "username": "user1",
            "score": None,  # None value
            "numComments": 5,
            "date": "2025-12-06T10:00:00Z",
            "selftext": "SHRM discussion",
            "subreddit": "test",
        }

        with patch(
            "main_collect.collect_reddit_posts", return_value=[reddit_post]
        ), patch("main_collect.collect_news_articles", return_value=[]), patch(
            "main_collect.has_seen", return_value=False
        ), patch(
            "main_collect.append_rows"
        ) as mock_append, patch(
            "main_collect.mark_seen"
        ):

            count = main_collect.main_collect(["test"], "Test Topic")

        rows = mock_append.call_args[0][0]
        row = rows[0]

        # Should handle None gracefully
        assert row[10] == "0"  # Likes (None becomes 0)
        assert row[11] == "5"  # Comments
        assert row[13] == "5"  # Eng. Total (0 + 5 + 0)


class TestMainCollectRowStructure:
    """Additional tests for row structure and column order."""

    def test_row_column_order(self, mock_config, mock_canonical_dedupe):
        """Test that row columns are in the exact specified order."""
        reddit_post = {
            "url": "https://reddit.com/r/test/post1",
            "title": "SHRM Test Post",
            "username": "testuser",
            "score": 1,
            "numComments": 0,
            "date": "2025-12-06T10:00:00Z",
            "selftext": "SHRM post body",
            "subreddit": "test",
        }

        with patch(
            "main_collect.collect_reddit_posts", return_value=[reddit_post]
        ), patch("main_collect.collect_news_articles", return_value=[]), patch(
            "main_collect.has_seen", return_value=False
        ), patch(
            "main_collect.append_rows"
        ) as mock_append, patch(
            "main_collect.mark_seen"
        ):

            main_collect.main_collect(["test"], "Test Topic")

        rows = mock_append.call_args[0][0]
        row = rows[0]

        # Verify exact column order
        assert row[0] is not None  # Date Posted
        assert row[1] == "Reddit"  # Platform
        assert "reddit.com/user/testuser" in row[2]  # Profile Link
        assert row[3] == "N/A"  # Nº Of Followers
        assert row[4] == "https://reddit.com/r/test/post1"  # Post Link
        assert row[5] == "Test Topic"  # Topic title
        assert "SHRM" in str(row[6])  # Summary
        assert row[7] == "N/A"  # Tone
        assert row[8] == ""  # Category
        assert row[9] == "N/A"  # Views
        assert row[10] == "1"  # Likes
        assert row[11] == "0"  # Comments
        assert row[12] == "0"  # Shares
        assert row[13] == "1"  # Eng. Total
        assert row[14] == "N/A"  # Sentiment Score
        assert row[15] == "N/A"  # Verified (Y/N)
        assert row[16] == ""  # Notes

    def test_news_item_row_structure(self, mock_config, mock_canonical_dedupe):
        """Test News item row structure."""
        news_article = {
            "url": "https://news.com/article/1",
            "title": "SHRM News Title",
            "description": "SHRM news description",
            "source_name": "Reuters",
            "publishedAt": "2025-12-06T10:00:00Z",
        }

        with patch("main_collect.collect_reddit_posts", return_value=[]), patch(
            "main_collect.collect_news_articles", return_value=[news_article]
        ), patch("main_collect.has_seen", return_value=False), patch(
            "main_collect.append_rows"
        ) as mock_append, patch(
            "main_collect.mark_seen"
        ):

            main_collect.main_collect(["test"], "Test Topic")

        rows = mock_append.call_args[0][0]
        row = rows[0]

        assert row[1] == "News"  # Platform
        assert row[2] == "N/A"  # Profile Link
        assert row[3] == "N/A"  # Followers
        assert row[4] == "https://news.com/article/1"  # Post Link
        assert row[5] == "Test Topic"  # Topic title
        assert row[6] == "SHRM news description"  # Summary
        assert row[7] == "N/A"  # Tone
        assert row[10] == "N/A"  # Likes
        assert row[11] == "N/A"  # Comments
        assert row[13] == "N/A"  # Eng. Total


class TestMainCollectTopicFiltering:
    """Tests for anchor-based topic filtering."""

    def test_off_topic_items_are_removed(self, mock_config, mock_canonical_dedupe):
        """Test that off-topic items are filtered out."""
        # Off-topic Reddit post (no SHRM anchors, no Johnny with case context)
        off_topic_reddit = {
            "url": "https://reddit.com/r/HR/comments/offtopic",
            "title": "General HR best practices",
            "username": "testuser",
            "score": 5,
            "numComments": 2,
            "date": "2025-12-06T10:30:00Z",
            "selftext": "This is about general workplace policies",
            "subreddit": "HR",
        }

        # Off-topic News article (no SHRM anchors, no Johnny with case context)
        off_topic_news = {
            "url": "https://news.com/article/offtopic",
            "title": "Workplace discrimination trends",
            "description": "General discussion about workplace issues",
            "source_name": "Reuters",
            "publishedAt": "2025-12-06T11:00:00Z",
        }

        # Johnny only, no case context - should be rejected
        johnny_only_reddit = {
            "url": "https://reddit.com/r/HR/comments/johnnyonly",
            "title": "Johnny Taylor to speak at HR leadership summit",
            "username": "testuser",
            "score": 5,
            "numComments": 2,
            "date": "2025-12-06T10:30:00Z",
            "selftext": "Johnny C. Taylor keynote at random conference",
            "subreddit": "HR",
        }

        with patch(
            "main_collect.collect_reddit_posts",
            return_value=[off_topic_reddit, johnny_only_reddit],
        ), patch(
            "main_collect.collect_news_articles", return_value=[off_topic_news]
        ), patch(
            "main_collect.has_seen", return_value=False
        ), patch(
            "main_collect.append_rows"
        ) as mock_append, patch(
            "main_collect.mark_seen"
        ):

            count = main_collect.main_collect(["test"], "Test Topic")

        # Should not append any rows (all filtered out)
        mock_append.assert_not_called()
        assert count == 0

    def test_on_topic_items_pass(self, mock_config, mock_canonical_dedupe):
        """Test that on-topic items pass the filter."""
        # On-topic Reddit post (contains "shrm")
        on_topic_reddit = {
            "url": "https://reddit.com/r/HR/comments/ontopic",
            "title": "SHRM trial verdict rocks HR world",
            "username": "testuser",
            "score": 10,
            "numComments": 5,
            "date": "2025-12-06T10:30:00Z",
            "selftext": "Discussion about SHRM trial verdict",
            "subreddit": "HR",
        }

        # On-topic News article (Johnny + case context)
        on_topic_news = {
            "url": "https://news.com/article/ontopic",
            "title": "Johnny C. Taylor found liable in harassment lawsuit",
            "description": "Johnny Taylor verdict announced in sexual harassment case",
            "source_name": "Reuters",
            "publishedAt": "2025-12-06T11:00:00Z",
        }

        # On-topic: Johnny + SHRM
        on_topic_reddit2 = {
            "url": "https://reddit.com/r/HR/comments/ontopic2",
            "title": "SHRM CEO Johnny C. Taylor responds to verdict",
            "username": "testuser2",
            "score": 15,
            "numComments": 8,
            "date": "2025-12-06T10:30:00Z",
            "selftext": "SHRM CEO addresses the trial",
            "subreddit": "HR",
        }

        with patch(
            "main_collect.collect_reddit_posts",
            return_value=[on_topic_reddit, on_topic_reddit2],
        ), patch(
            "main_collect.collect_news_articles", return_value=[on_topic_news]
        ), patch(
            "main_collect.has_seen", return_value=False
        ), patch(
            "main_collect.append_rows"
        ) as mock_append, patch(
            "main_collect.mark_seen"
        ):

            count = main_collect.main_collect(["test"], "Test Topic")

        # Should append all 3 rows (2 Reddit + 1 News)
        assert mock_append.call_count == 1
        rows = mock_append.call_args[0][0]
        assert len(rows) == 3
        assert count == 3

    def test_mixed_input_filtered_correctly(self, mock_config, mock_canonical_dedupe):
        """Test that mixed on-topic and off-topic items are filtered correctly."""
        # Mix of on-topic and off-topic items
        on_topic_reddit = {
            "url": "https://reddit.com/r/HR/comments/ontopic",
            "title": "SHRM trial verdict discussion",
            "username": "testuser",
            "score": 10,
            "numComments": 5,
            "date": "2025-12-06T10:30:00Z",
            "selftext": "Society for Human Resource Management case update",
            "subreddit": "HR",
        }

        off_topic_reddit = {
            "url": "https://reddit.com/r/HR/comments/offtopic",
            "title": "General HR policies",
            "username": "testuser2",
            "score": 5,
            "numComments": 2,
            "date": "2025-12-06T10:30:00Z",
            "selftext": "Best practices for HR departments",
            "subreddit": "HR",
        }

        # Johnny + case context (no SHRM) - should pass
        on_topic_news = {
            "url": "https://news.com/article/ontopic",
            "title": "Johnny C. Taylor found liable in harassment lawsuit",
            "description": "Johnny Taylor verdict in sexual harassment case",
            "source_name": "Reuters",
            "publishedAt": "2025-12-06T11:00:00Z",
        }

        # Off-topic: harassment without SHRM or Johnny
        off_topic_news = {
            "url": "https://news.com/article/offtopic",
            "title": "Harassment verdict at tech company",
            "description": "General discussion about workplace harassment",
            "source_name": "Reuters",
            "publishedAt": "2025-12-06T11:00:00Z",
        }

        # Johnny only, no case context - should be rejected
        johnny_only_news = {
            "url": "https://news.com/article/johnnyonly",
            "title": "Johnny Taylor to speak at HR leadership summit",
            "description": "Johnny C. Taylor keynote at random conference",
            "source_name": "Reuters",
            "publishedAt": "2025-12-06T11:00:00Z",
        }

        with patch(
            "main_collect.collect_reddit_posts",
            return_value=[on_topic_reddit, off_topic_reddit],
        ), patch(
            "main_collect.collect_news_articles",
            return_value=[on_topic_news, off_topic_news, johnny_only_news],
        ), patch(
            "main_collect.has_seen", return_value=False
        ), patch(
            "main_collect.append_rows"
        ) as mock_append, patch(
            "main_collect.mark_seen"
        ):

            count = main_collect.main_collect(["test"], "Test Topic")

        # Should append only the 2 on-topic items (1 Reddit + 1 News)
        # on_topic_reddit (SHRM) + on_topic_news (Johnny + case context)
        # off_topic_reddit, off_topic_news, and johnny_only_news should be filtered out
        assert mock_append.call_count == 1
        rows = mock_append.call_args[0][0]
        assert len(rows) == 2
        assert count == 2

        # Verify the correct items are present
        platforms = [row[1] for row in rows]  # Platform column
        assert "Reddit" in platforms
        assert "News" in platforms

        # Verify summaries contain on-topic content
        summaries = [row[6] for row in rows]  # Summary column
        assert any(
            "shrm" in s.lower() or "society for human resource management" in s.lower()
            for s in summaries
        )
        assert any(
            "harassment" in s.lower() or "johnny c. taylor" in s.lower()
            for s in summaries
        )

    def test_topic_filter_various_anchors(self, mock_config):
        """Test that all anchor terms work correctly with refined logic."""
        test_cases = [
            # (title, description, should_pass, reason)
            # SHRM-only content passes
            ("SHRM trial verdict rocks HR world", "", True, "SHRM anchor"),
            (
                "Society for Human Resource Management verdict update",
                "",
                True,
                "Full org name",
            ),
            # Johnny + SHRM passes
            (
                "SHRM CEO Johnny C. Taylor responds to verdict",
                "",
                True,
                "Johnny + SHRM",
            ),
            (
                "Johnny Taylor and SHRM face backlash",
                "",
                True,
                "Johnny + SHRM",
            ),
            # Johnny + case context but no explicit 'SHRM' still passes
            (
                "Johnny C. Taylor found liable in harassment lawsuit",
                "",
                True,
                "Johnny + case context",
            ),
            (
                "Johnny Taylor verdict announced",
                "",
                True,
                "Johnny + verdict",
            ),
            (
                "Johnny C Taylor sexual harassment trial",
                "",
                True,
                "Johnny + case context (variant spelling)",
            ),
            # Johnny only (no SHRM, no case terms) is rejected
            (
                "Johnny Taylor to speak at HR leadership summit",
                "",
                False,
                "Johnny only, no case context",
            ),
            (
                "Johnny C. Taylor keynote at random conference",
                "",
                False,
                "Johnny only, no case context",
            ),
            # Off-topic HR / harassment without SHRM or Johnny is rejected
            (
                "Harassment verdict at tech company",
                "",
                False,
                "No SHRM or Johnny",
            ),
            (
                "Workplace discrimination trial update",
                "",
                False,
                "No SHRM or Johnny",
            ),
            ("General HR discussion", "", False, "No anchors"),
            ("Workplace policies", "Best practices", False, "No anchors"),
            # Edge cases
            (
                "SHRM conference announcement",
                "",
                True,
                "SHRM anchor (even without case context)",
            ),
            (
                "Johnny Taylor discusses HR trends",
                "",
                False,
                "Johnny without case context",
            ),
            (
                "Johnny C. Taylor lawsuit settlement",
                "",
                True,
                "Johnny + lawsuit",
            ),
            (
                "Johnny Taylor allegations surface",
                "",
                True,
                "Johnny + allegations",
            ),
            (
                "Johnny C Taylor scandal investigation",
                "",
                True,
                "Johnny + scandal",
            ),
        ]

        for title, description, should_pass, reason in test_cases:
            item = {
                "url": f"https://test.com/{title.replace(' ', '-').lower()}",
                "title": title,
                "description": description,
                "source_name": "Test",
                "publishedAt": "2025-12-06T11:00:00Z",
                "selftext": "",  # For Reddit items
            }

            result = main_collect.is_on_topic(item)
            assert (
                result == should_pass
            ), f"Expected {should_pass} for '{title}' (reason: {reason}), got {result}"
