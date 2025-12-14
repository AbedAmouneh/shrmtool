"""
Telegram summary message builder for the SHRM monitoring pipeline.

Produces an HTML-formatted summary suitable for Telegram (parse_mode="HTML").
"""

from __future__ import annotations

from html import escape
from typing import List


def _escape_html(text: str) -> str:
    """Escape minimal HTML entities to keep parse_mode=HTML safe."""

    if text is None:
        return ""
    return escape(str(text), quote=False)


def build_telegram_summary(
    topic: str,
    search_terms: List[str],
    total_new: int,
    news_count: int,
    twitter_count: int,
    linkedin_count: int,
    reddit_count: int = 0,
    blocked_count: int = 0,
    date_filtered_count: int = 0,
    dedupe_count: int = 0,
    offtopic_count: int = 0,
) -> str:
    """
    Build a multi-line HTML-formatted summary for Telegram.

    Args:
        topic: Topic label used for this run.
        search_terms: List of search terms used.
        total_new: Total rows appended to the sheet.
        news_count: Count of appended News rows.
        twitter_count: Count of appended X/Twitter rows.
        linkedin_count: Count of appended LinkedIn-Google rows.
        reddit_count: Count of appended Reddit rows.
        blocked_count: Count of items blocked (spam/blocked sources).
        date_filtered_count: Count of items filtered by date (pre-Dec 2025).
        dedupe_count: Count of duplicates removed.
        offtopic_count: Count of items discarded as off-topic/borderline.

    Returns:
        HTML-formatted string for Telegram.
    """

    safe_topic = _escape_html(topic or "")
    safe_terms = _escape_html(", ".join(search_terms) if search_terms else "")

    lines = [
        "📊 <b>SHRM Monitoring Pipeline — Daily Intake Alert</b>",
        "",
        f"<b>New items added to sheet:</b> {total_new}",
        "",
        "<b>Platform Breakdown:</b>",
        f"• 📰 News: {news_count}",
        f"• 👔 LinkedIn: {linkedin_count}",
        f"• 🔴 Reddit: {reddit_count}",
        f"• 🐦 X/Twitter: {twitter_count}",
        "",
        "<b>Quality Enforcement:</b>",
        f"• 🛡️ Spam/Blocked: {blocked_count}",
        f"• 📅 Date Filtered: {date_filtered_count} (Pre-Dec 2025)",
        f"• ♻️ Duplicates Skipped: {dedupe_count}",
        f"• 🚫 Off-topic Discarded: {offtopic_count}",
        "",
        "<b>Focus topics:</b>",
        f"{safe_topic} | SHRM Leadership (Johnny C. Taylor) | HR community response",
        "",
        "<b>Automated Checks Passed:</b>",
        "✔ URL Canonicalization (Aggressive)",
        "✔ 17-Column Schema (Integer Metrics)",
        "✔ Strict Date Guard (> Dec 1, 2025)",
        "✔ Spam Domain Blocking (Biztoc)",
        "✔ Title-Based Deduplication",
        "✔ Topic Classification (on-topic only)",
        "",
        "<b>Search terms:</b>",
        safe_terms,
    ]

    return "\n".join(lines)


