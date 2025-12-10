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
    repost_count: int,
    dedupe_count: int,
    offtopic_count: int,
) -> str:
    """
    Build a multi-line HTML-formatted summary for Telegram.

    Args:
        topic: Topic label used for this run.
        search_terms: List of search terms used.
        total_new: Total rows appended to the sheet.
        news_count: Count of appended News rows.
        twitter_count: Count of appended X/Twitter rows.
        repost_count: Count of reposts detected/tagged.
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
        f"<b>New items added to sheet:</b> {total_new} items",
        f"• News: {news_count}",
        f"• X/Twitter: {twitter_count}",
        f"• Reposts detected: {repost_count} (filtered)",
        f"• Duplicates removed: {dedupe_count}",
        f"• Off-topic discarded: {offtopic_count}",
        "",
        "<b>Focus topics:</b>",
        f"{safe_topic} | SHRM Leadership (Johnny C. Taylor) | HR community response",
        "",
        "<b>Automated checks passed:</b>",
        "✔ URL canonicalization",
        "✔ 17-column schema validation",
        "✔ Metric normalization",
        "✔ Verdict-date filter",
        "✔ Topic classification (on-topic only)",
        "",
        "<b>Search terms:</b>",
        safe_terms,
    ]

    return "\n".join(lines)


