"""
Simple SQLite-based store to track which URLs we've already processed.

Used to avoid inserting duplicate rows into the Google Sheet.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Iterable

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
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS seen_urls (
            url TEXT PRIMARY KEY
        )
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
    Return how many unique URLs are stored.
    """
    conn = _get_connection()
    try:
        cur = conn.execute("SELECT COUNT(*) FROM seen_urls")
        (count,) = cur.fetchone()
        return int(count)
    finally:
        conn.close()
