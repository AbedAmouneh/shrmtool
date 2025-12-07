"""
Summary generation utilities.

Builds short summaries from title and text, truncating at ~300 characters.
"""

from __future__ import annotations

from typing import Optional
import re

MAX_SUMMARY_LENGTH = 300


def truncate_text(text: str, max_length: int = MAX_SUMMARY_LENGTH) -> str:
    """
    Truncate text to max_length characters without breaking words if possible.
    """
    text = text.strip()
    if len(text) <= max_length:
        return text

    cut = text[:max_length]
    last_space = cut.rfind(" ")
    if last_space == -1:
        return cut
    return cut[:last_space]


def _normalize_spaces(text: str) -> str:
    """Collapse multiple whitespace characters into a single space."""
    return re.sub(r"\s+", " ", text).strip()


def build_summary(title: Optional[str], body: Optional[str] = None) -> str:
    """
    Build a summary from title and optional body text.

    - Combines title and body.
    - Avoids repeating the title if the body starts with it.
    - Normalizes multiple spaces.
    - Truncates to ~300 characters.
    """
    parts = []

    clean_title = title.strip() if title else None

    if clean_title:
        parts.append(clean_title)

    if body:
        body_clean = body.strip()
        # Avoid duplicating title if body starts with it
        if clean_title and body_clean.lower().startswith(clean_title.lower()):
            body_clean = body_clean[len(clean_title):].lstrip()

        parts.append(body_clean)

    if not parts:
        return ""

    full = " ".join(parts)
    full = _normalize_spaces(full)
    return truncate_text(full)
