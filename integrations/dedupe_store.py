"""
Simple SQLite-based store to track which URLs we've already processed.

Used to avoid inserting duplicate rows into the Google Sheet.

Enhanced to support:
- Canonical URL tracking for better deduplication
- Profile tracking for repost detection
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Iterable, Optional, Tuple

# IMPORTANT: This pattern allows tests to monkeypatch DB_PATH, then reload
# the module without us overwriting their patched value.
try:
    DB_PATH  # type: ignore[name-defined]
except NameError:
    DB_PATH: Path = Path("seen_urls.db")


def _get_connection() -> sqlite3.Connection:
    """
    Open a connection to the SQLite DB and ensure the table exists.
    """
    # DB_PATH is evaluated at call time so monkeypatching works
    conn = sqlite3.connect(DB_PATH)
    # Create legacy table for backward compatibility
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS seen_urls (
            url TEXT PRIMARY KEY
        )
        """
    )
    # Create enhanced table for canonical URL + profile tracking
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS seen_items (
            canonical_url TEXT NOT NULL,
            platform TEXT NOT NULL,
            profile TEXT,
            post_url TEXT NOT NULL,
            first_seen_date TEXT,
            PRIMARY KEY (canonical_url, platform, profile)
        )
        """
    )
    # Create index for faster lookups
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_canonical_url_platform 
        ON seen_items(canonical_url, platform)
        """
    )
    return conn


def has_seen(url: str) -> bool:
    """
    Return True if the URL is already stored in the DB.
    """
    if not url:
        return False

    conn = _get_connection()
    try:
        cur = conn.execute("SELECT 1 FROM seen_urls WHERE url = ?", (url,))
        return cur.fetchone() is not None
    finally:
        conn.close()


def mark_seen(urls: Iterable[str]) -> None:
    """
    Mark one or more URLs as seen.

    - Ignores empty list or falsy URLs.
    - Uses INSERT OR IGNORE so duplicates don't crash or add extra rows.
    """
    normalized = [u for u in urls if u]
    if not normalized:
        return

    conn = _get_connection()
    try:
        conn.executemany(
            "INSERT OR IGNORE INTO seen_urls (url) VALUES (?)",
            ((u,) for u in normalized),
        )
        conn.commit()
    finally:
        conn.close()


def get_seen_count() -> int:
    """
    Return how many unique URLs are stored (legacy table).
    """
    conn = _get_connection()
    try:
        cur = conn.execute("SELECT COUNT(*) FROM seen_urls")
        (count,) = cur.fetchone()
        return int(count)
    finally:
        conn.close()


def has_seen_canonical(
    canonical_url: str, platform: str, profile: Optional[str] = None
) -> Tuple[bool, Optional[str]]:
    """
    Check if a canonical URL has been seen for a given platform and profile.

    Args:
        canonical_url: Canonical URL (normalized)
        platform: Platform name ("News", "X", "Reddit", etc.)
        profile: Optional profile identifier (username, account ID, etc.)

    Returns:
        Tuple of (has_seen, existing_post_url)
        - has_seen: True if this canonical URL + platform + profile combination exists
        - existing_post_url: The original post_url if found, None otherwise

    For News platform: profile is ignored (only canonical_url + platform matter)
    For social platforms: profile is used to distinguish reposts from duplicates
    """
    if not canonical_url or not platform:
        return False, None

    conn = _get_connection()
    try:
        if platform == "News":
            # For News, ignore profile - only check canonical_url + platform
            cur = conn.execute(
                """
                SELECT post_url FROM seen_items
                WHERE canonical_url = ? AND platform = ?
                LIMIT 1
                """,
                (canonical_url, platform),
            )
        else:
            # For social platforms, check canonical_url + platform + profile
            cur = conn.execute(
                """
                SELECT post_url FROM seen_items
                WHERE canonical_url = ? AND platform = ? AND profile = ?
                LIMIT 1
                """,
                (canonical_url, platform, profile or ""),
            )

        row = cur.fetchone()
        if row:
            return True, row[0]
        return False, None
    finally:
        conn.close()


def has_seen_canonical_by_platform(canonical_url: str, platform: str) -> bool:
    """
    Check if a canonical URL has been seen for a platform (any profile).

    Used to detect reposts: same canonical URL, different profile.

    Args:
        canonical_url: Canonical URL (normalized)
        platform: Platform name

    Returns:
        True if canonical URL exists for this platform (regardless of profile)
    """
    if not canonical_url or not platform:
        return False

    conn = _get_connection()
    try:
        cur = conn.execute(
            """
            SELECT 1 FROM seen_items
            WHERE canonical_url = ? AND platform = ?
            LIMIT 1
            """,
            (canonical_url, platform),
        )
        return cur.fetchone() is not None
    finally:
        conn.close()


def mark_seen_canonical(
    canonical_url: str,
    platform: str,
    post_url: str,
    profile: Optional[str] = None,
    first_seen_date: Optional[str] = None,
) -> None:
    """
    Mark a canonical URL as seen for a platform and profile.

    Args:
        canonical_url: Canonical URL (normalized)
        platform: Platform name
        post_url: Original post URL (for reference)
        profile: Optional profile identifier
        first_seen_date: Optional date string (YYYY-MM-DD) when first seen
    """
    if not canonical_url or not platform or not post_url:
        return

    conn = _get_connection()
    try:
        conn.execute(
            """
            INSERT OR IGNORE INTO seen_items
            (canonical_url, platform, profile, post_url, first_seen_date)
            VALUES (?, ?, ?, ?, ?)
            """,
            (canonical_url, platform, profile or "", post_url, first_seen_date),
        )
        conn.commit()
    finally:
        conn.close()
