from unittest.mock import patch

import main_collect


def _normalized_news(url: str, topic: str):
    """Return a raw NewsAPI-like article dict (main_collect will normalize)."""
    return {
        "url": url,
        "title": "SHRM verdict article",
        "description": "SHRM verdict coverage",
        "source_name": "Reuters",
        "publishedAt": "2025-12-06T10:00:00Z",
    }


def _normalized_tweet(url: str, profile: str, topic: str, text: str):
    return {
        "date_posted": "01/02/2025",
        "platform": "X",
        "profile": profile,
        "profile_link": f"https://x.com/{profile.strip('@')}",
        "followers": "100",
        "post_link": url,
        "topic": topic,
        "title": text[:160],
        "summary": text,
        "tone": "N/A",
        "category": "",
        "views": "1000",
        "likes": "10",
        "comments": "2",
        "shares": "3",
        "eng_total": "15",
        "sentiment_score": "N/A",
        "verified": "N/A",
        "notes": "",
        "selftext": "",
        "description": text,
    }


def test_news_canonical_dedupe(monkeypatch, mock_config):
    topic = "Test Topic"
    article1 = _normalized_news("https://news.com/article/dup", topic)
    article2 = _normalized_news("https://news.com/article/dup?utm_source=x", topic)

    calls = [False, True]  # first unseen, second seen

    def mock_seen_by_platform(canonical, platform):
        return calls.pop(0) if calls else True

    with patch("main_collect.collect_reddit_posts", return_value=[]), patch(
        "main_collect.collect_news_articles", return_value=[article1, article2]
    ), patch("main_collect.has_seen", return_value=False), patch(
        "main_collect.has_seen_canonical_by_platform", side_effect=mock_seen_by_platform
    ), patch(
        "main_collect.has_seen_canonical", return_value=(False, None)
    ), patch(
        "main_collect.mark_seen_canonical", lambda *args, **kwargs: None
    ), patch(
        "main_collect.append_rows"
    ) as mock_append, patch(
        "main_collect.mark_seen"
    ):
        count = main_collect.main_collect(["test"], topic)

    assert mock_append.call_count == 1
    rows = mock_append.call_args[0][0]
    assert len(rows) == 1
    assert count == 1


def test_x_repost_detection(monkeypatch, mock_config):
    topic = "Test Topic"
    url = "https://twitter.com/i/web/status/123"
    tweet_a = _normalized_tweet(url, "@userA", topic, "SHRM verdict reactions")
    tweet_b = _normalized_tweet(url, "@userB", topic, "SHRM verdict reactions repost")

    seen_by_platform_calls = [False, True]  # first unseen, second seen -> repost

    def mock_seen_canonical(canonical, platform, profile=None):
        return False, None

    def mock_seen_by_platform(canonical, platform):
        return seen_by_platform_calls.pop(0) if seen_by_platform_calls else True

    with patch("main_collect.collect_reddit_posts", return_value=[]), patch(
        "main_collect.collect_news_articles", return_value=[]
    ), patch(
        "main_collect.collect_twitter_posts", return_value=[tweet_a, tweet_b]
    ), patch(
        "main_collect.has_seen", return_value=False
    ), patch(
        "main_collect.has_seen_canonical", side_effect=mock_seen_canonical
    ), patch(
        "main_collect.has_seen_canonical_by_platform", side_effect=mock_seen_by_platform
    ), patch(
        "main_collect.mark_seen_canonical", lambda *args, **kwargs: None
    ), patch(
        "main_collect.append_rows"
    ) as mock_append, patch(
        "main_collect.mark_seen"
    ):
        count = main_collect.main_collect(["test"], topic)

    assert mock_append.call_count == 1
    rows = mock_append.call_args[0][0]
    assert len(rows) == 2
    assert count == 2

    # Second row should be tagged as repost
    categories = [row[8] for row in rows]
    assert "Repost" in categories

    notes = [row[16] for row in rows]
    assert any("Repost of canonical URL" in n for n in notes)

