import pytest
from unittest.mock import patch, MagicMock

from collectors import x_collector
from utils.time_utils import parse_iso_date
from utils.config import VERDICT_DATE


class FakeResponse:
    def __init__(self, status_code=200, json_data=None, text="ok"):
        self.status_code = status_code
        self._json_data = json_data or {}
        self.text = text

    def json(self):
        return self._json_data


def _tweet(
    tid="1",
    text="SHRM verdict update",
    created_at="2025-12-06T10:00:00Z",
    author_id="u1",
    metrics=None,
):
    return {
        "id": tid,
        "text": text,
        "created_at": created_at,
        "author_id": author_id,
        "public_metrics": metrics or {
            "like_count": 3,
            "reply_count": 1,
            "retweet_count": 1,
            "quote_count": 1,
            "impression_count": 10,
        },
    }


def _user(uid="u1", username="user1", followers=50):
    return {
        "id": uid,
        "username": username,
        "public_metrics": {"followers_count": followers},
    }


@pytest.fixture(autouse=True)
def reset_token(monkeypatch):
    # Ensure token is set during tests unless overridden
    monkeypatch.setenv("X_BEARER_TOKEN", "test-token")
    # Reload module headers
    from importlib import reload
    reload(x_collector)
    yield
    reload(x_collector)


class TestXCollector:
    def test_happy_path_normalization(self, monkeypatch):
        tweets = [_tweet()]
        users = [_user()]
        fake = FakeResponse(
            200,
            {
                "data": tweets,
                "includes": {"users": users},
            },
        )
        with patch("requests.get", return_value=fake):
            res = x_collector.collect_twitter_posts(["SHRM"], "Topic")
        assert len(res) == 1
        item = res[0]
        assert item["platform"] == "X"
        assert item["profile"] == "@user1"
        assert item["profile_link"] == "https://x.com/user1"
        assert item["likes"] == "3"
        assert item["comments"] == "1"
        assert item["shares"] == "2"  # 1 retweet + 1 quote
        assert item["eng_total"] == "6"
        assert item["post_link"].endswith("/1")

    def test_missing_fields(self, monkeypatch):
        tweets = [
            {"id": "1", "text": "missing created_at"},  # missing created_at
            _tweet(tid="2", created_at="2025-12-06T10:00:00Z"),
        ]
        fake = FakeResponse(200, {"data": tweets, "includes": {"users": []}})
        with patch("requests.get", return_value=fake):
            res = x_collector.collect_twitter_posts(["SHRM"], "Topic")
        # Only one valid
        assert len(res) == 1

    def test_missing_token(self, monkeypatch):
        monkeypatch.delenv("X_BEARER_TOKEN", raising=False)
        from importlib import reload
        reload(x_collector)
        res = x_collector.collect_twitter_posts(["SHRM"], "Topic")
        assert res == []

    def test_network_error(self, monkeypatch):
        with patch("requests.get", side_effect=Exception("network")):
            res = x_collector.collect_twitter_posts(["SHRM"], "Topic")
        assert res == []

    def test_http_error(self, monkeypatch):
        fake = FakeResponse(500, {}, "server error")
        with patch("requests.get", return_value=fake):
            res = x_collector.collect_twitter_posts(["SHRM"], "Topic")
        assert res == []

    def test_empty_response(self, monkeypatch):
        fake = FakeResponse(200, {"data": []})
        with patch("requests.get", return_value=fake):
            res = x_collector.collect_twitter_posts(["SHRM"], "Topic")
        assert res == []

    def test_date_filtering(self, monkeypatch):
        before = _tweet(created_at="2025-12-04T10:00:00Z")
        after = _tweet(tid="2", created_at="2025-12-06T10:00:00Z")
        fake = FakeResponse(200, {"data": [before, after], "includes": {"users": [_user()]}})
        with patch("requests.get", return_value=fake):
            res = x_collector.collect_twitter_posts(["SHRM"], "Topic")
        assert len(res) == 1
        assert res[0]["post_link"].endswith("/2")

    def test_per_run_dedupe(self, monkeypatch):
        t1 = _tweet(tid="1")
        t2 = _tweet(tid="1")  # duplicate id
        fake = FakeResponse(200, {"data": [t1, t2], "includes": {"users": [_user()]}})
        with patch("requests.get", return_value=fake):
            res = x_collector.collect_twitter_posts(["SHRM"], "Topic")
        assert len(res) == 1

    def test_multiple_search_terms(self, monkeypatch):
        resp1 = FakeResponse(
            200, {"data": [_tweet(tid="a")], "includes": {"users": [_user()]}}
        )
        resp2 = FakeResponse(
            200,
            {
                "data": [_tweet(tid="b")],
                "includes": {"users": [_user(uid="u2", username="u2")]},
            },
        )

        def side_effect(url, headers=None, params=None, timeout=None):
            if params and params.get("query") == "one":
                return resp1
            return resp2

        with patch("requests.get", side_effect=side_effect):
            res = x_collector.collect_twitter_posts(["one", "two"], "Topic")
        urls = [r["post_link"] for r in res]
        assert len(res) == 2
        assert any(u.endswith("/a") for u in urls)
        assert any(u.endswith("/b") for u in urls)

