import pytest

from utils.schema import build_row, validate_row


def test_build_row_returns_17_values_and_defaults():
    item = {
        "date_posted": "01/01/2025",
        "platform": "News",
        "profile_link": "N/A",
        "followers": "N/A",
        "post_link": "https://example.com/article",
        "topic": "Test Topic",
        "summary": "Summary text",
        "tone": "N/A",
        "category": "",
        "views": "N/A",
        "likes": "N/A",
        "comments": "N/A",
        "shares": "N/A",
        "eng_total": "N/A",
        "sentiment_score": "N/A",
        "verified": "N/A",
        "notes": "",
    }

    row = build_row(item)
    assert len(row) == 17
    assert row[0] == "01/01/2025"
    assert row[4] == "https://example.com/article"
    assert row[6] == "Summary text"
    assert row[13] == "N/A"
    assert row[16] == ""


def test_validate_row_rejects_wrong_length():
    assert validate_row([1] * 16) is False
    assert validate_row([1] * 18) is False


def test_validate_row_requires_core_fields():
    row = build_row(
        {
            "date_posted": "01/01/2025",
            "platform": "News",
            "profile_link": "N/A",
            "followers": "N/A",
            "post_link": "",
            "topic": "",
            "summary": "",
        }
    )
    assert validate_row(row) is False


def test_validate_row_accepts_valid_row():
    row = build_row(
        {
            "date_posted": "01/01/2025",
            "platform": "News",
            "profile_link": "N/A",
            "followers": "N/A",
            "post_link": "https://example.com/article",
            "topic": "Topic",
            "summary": "Summary",
        }
    )
    assert validate_row(row) is True

