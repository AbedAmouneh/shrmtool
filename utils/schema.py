"""
Schema definition and validation for the 17-column Google Sheet format.

This module provides:
- Canonical column order definition
- Row building from normalized items
- Row validation to ensure data quality
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Sequence

logger = logging.getLogger(__name__)

# Canonical 17-column order (per new schema specification)
COLUMN_ORDER = [
    "Date Posted",          # 1
    "Platform",             # 2
    "Profile Link",         # 3
    "N° of Followers",     # 4
    "Post Link",            # 5
    "Topic title",          # 6
    "Summary",              # 7
    "Tone",                 # 8
    "Category",             # 9
    "Views",                # 10
    "Likes",                # 11
    "Comments",             # 12
    "Shares",               # 13
    "Eng. Total",           # 14
    "Sentiment Score",      # 15
    "Verified (Y/N)",       # 16
    "Notes",                # 17
]

# Required fields that must be non-empty
REQUIRED_FIELDS = {
    "Date Posted",
    "Platform",
    "Post Link",
    "Topic title",
}


def build_row(item: Dict[str, Any]) -> List[Any]:
    """
    Convert a normalized item dictionary to a row matching the 17-column schema.

    Args:
        item: Normalized item dictionary with keys matching internal schema

    Returns:
        List of exactly 17 values in the canonical column order

    The item dictionary should have these keys (internal format):
    - date_posted: MM/DD/YYYY string
    - platform: "Reddit", "News", "X", etc.
    - profile_link: URL or "N/A"
    - followers: follower count string or "N/A"
    - post_link: URL to the post/article
    - topic: topic label string
    - title: title string
    - summary: summary text
    - tone: "N/A" or sentiment value
    - category: category string (can be empty, defaults to "")
    - views: view count string or "N/A"
    - likes: like count string or "N/A"
    - comments: comment count string or "N/A"
    - shares: share count string or "N/A"
    - eng_total: engagement total string or "N/A"
    - sentiment_score: sentiment score string or "N/A"
    - verified: "Y" or "N" or "N/A"
    - notes: notes string (can be empty, defaults to "")
    """
    # Get metric values, defaulting to "0" instead of "N/A" for integer compliance
    views = item.get("views", "0")
    likes = item.get("likes", "0")
    comments = item.get("comments", "0")
    shares = item.get("shares", "0")
    eng_total = item.get("eng_total", "0")

    # Convert "N/A" to "0" for metric columns
    if views == "N/A":
        views = "0"
    if likes == "N/A":
        likes = "0"
    if comments == "N/A":
        comments = "0"
    if shares == "N/A":
        shares = "0"
    if eng_total == "N/A":
        eng_total = "0"

    return [
        item.get("date_posted", ""),           # 1 Date Posted
        item.get("platform", ""),              # 2 Platform
        item.get("profile_link", "N/A"),       # 3 Profile Link
        item.get("followers", "N/A"),         # 4 N° of Followers
        item.get("post_link", ""),             # 5 Post Link
        item.get("topic", ""),                 # 6 Topic title
        item.get("summary", ""),               # 7 Summary
        item.get("tone", "N/A"),               # 8 Tone
        item.get("category", ""),              # 9 Category
        views,                                  # 10 Views
        likes,                                  # 11 Likes
        comments,                               # 12 Comments
        shares,                                 # 13 Shares
        eng_total,                              # 14 Eng. Total
        item.get("sentiment_score", "N/A"),    # 15 Sentiment Score
        item.get("verified", "N/A"),           # 16 Verified (Y/N)
        item.get("notes", ""),                 # 17 Notes
    ]


def validate_row(row: Sequence[Any]) -> bool:
    """
    Validate that a row matches the 17-column schema requirements.

    Args:
        row: Sequence of values (list or tuple)

    Returns:
        True if valid, False otherwise

    Validation rules:
    - Must have exactly 17 columns
    - Required fields (Date Posted, Platform, Post Link, Topic title) must be non-empty
    - Metric columns (Views, Likes, Comments, Shares, Eng. Total) must be numeric or "0", not "N/A"
    - Logs errors for validation failures

    Note: This function logs errors but does not raise exceptions.
    Use the return value to decide whether to skip the row.
    """
    if len(row) != 17:
        logger.error(
            f"Row validation failed: expected 17 columns, got {len(row)}. "
            f"Row: {row[:5]}..."
        )
        return False

    # Check required fields (indices match COLUMN_ORDER)
    # Date Posted = 0, Platform = 1, Post Link = 4, Topic title = 5
    date_posted = row[0] if len(row) > 0 else ""
    platform = row[1] if len(row) > 1 else ""
    post_link = row[4] if len(row) > 4 else ""
    topic_title = row[5] if len(row) > 5 else ""

    missing_fields = []
    if not date_posted or not str(date_posted).strip():
        missing_fields.append("Date Posted")
    if not platform or not str(platform).strip():
        missing_fields.append("Platform")
    if not post_link or not str(post_link).strip():
        missing_fields.append("Post Link")
    if not topic_title or not str(topic_title).strip():
        missing_fields.append("Topic title")

    if missing_fields:
        logger.error(
            f"Row validation failed: missing required fields: {missing_fields}. "
            f"Row preview: {row[:3]}..."
        )
        return False

    # Validate metric columns (Views=9, Likes=10, Comments=11, Shares=12, Eng. Total=13)
    metric_columns = {
        9: "Views",
        10: "Likes",
        11: "Comments",
        12: "Shares",
        13: "Eng. Total",
    }

    invalid_metrics = []
    for col_idx, col_name in metric_columns.items():
        if len(row) > col_idx:
            value = str(row[col_idx]).strip().upper()
            # Reject "N/A", "NONE", "NULL" in metric columns
            if value in ("N/A", "NONE", "NULL"):
                invalid_metrics.append(f"{col_name}='{value}'")
            # Allow empty, "0", or numeric values
            elif value and value != "0":
                try:
                    float(value.replace(",", ""))
                except ValueError:
                    # Not numeric - might be narrative text (will be caught by alignment analyzer)
                    pass

    if invalid_metrics:
        logger.error(
            f"Row validation failed: invalid metric values: {', '.join(invalid_metrics)}. "
            f"Metric columns must be numeric or '0', not 'N/A'. "
            f"Row preview: {row[:3]}..."
        )
        return False

    return True


def ensure_row_length(row: List[Any]) -> List[Any]:
    """
    Ensure a row has exactly 17 columns, padding with empty strings if needed.

    This is a safety function to prevent schema mismatches. In production,
    rows should be built using build_row() which guarantees 17 columns.

    Args:
        row: List of values (may be shorter or longer than 17)

    Returns:
        List of exactly 17 values (padded or truncated as needed)
    """
    if len(row) == 17:
        return row
    elif len(row) < 17:
        logger.warning(
            f"Row has {len(row)} columns, padding to 17. "
            f"Row preview: {row[:3]}..."
        )
        return row + [""] * (17 - len(row))
    else:
        logger.warning(
            f"Row has {len(row)} columns, truncating to 17. "
            f"Row preview: {row[:3]}..."
        )
        return row[:17]

