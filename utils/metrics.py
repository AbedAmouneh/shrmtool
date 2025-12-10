"""
Metric normalization utilities for parsing and computing engagement metrics.

Handles:
- Parsing K/M formatted numbers (e.g., "64.5K" -> 64500)
- Computing engagement totals
- Normalizing metric values to consistent formats
"""

from __future__ import annotations

import re
from typing import Optional, Union


def parse_k_number(value: Union[str, int, None]) -> Optional[int]:
    """
    Parse a number that may be in K/M format or plain integer.

    Examples:
        "64.5K" -> 64500
        "1.2M" -> 1200000
        "1234" -> 1234
        1234 -> 1234
        "N/A" -> None
        None -> None
        "" -> None

    Args:
        value: String, integer, or None

    Returns:
        Integer value, or None if value is not parseable
    """
    if value is None:
        return None

    # Handle integer input
    if isinstance(value, int):
        return value

    # Handle string input
    if not isinstance(value, str):
        return None

    value = value.strip()
    if not value or value.upper() == "N/A":
        return None

    # Try to parse as plain integer first
    try:
        return int(value)
    except ValueError:
        pass

    # Try to parse K/M format
    # Pattern: optional digits, optional decimal point, digits, then K or M (case-insensitive)
    pattern = r"^(\d+(?:\.\d+)?)\s*([KMkm])$"
    match = re.match(pattern, value)
    if match:
        number_str = match.group(1)
        suffix = match.group(2).upper()

        try:
            number = float(number_str)
            if suffix == "K":
                return int(number * 1000)
            elif suffix == "M":
                return int(number * 1000000)
        except (ValueError, OverflowError):
            return None

    return None


def compute_eng_total(
    likes: Union[str, int, None],
    comments: Union[str, int, None],
    shares: Union[str, int, None],
) -> Optional[int]:
    """
    Compute engagement total as the sum of likes, comments, and shares.

    Returns None if any of the values cannot be parsed as integers.

    Args:
        likes: Like count (string, int, or None)
        comments: Comment count (string, int, or None)
        shares: Share count (string, int, or None)

    Returns:
        Sum of the three values as an integer, or None if any value is unparseable

    Examples:
        compute_eng_total("10", "5", "2") -> 17
        compute_eng_total(10, 5, 2) -> 17
        compute_eng_total("10", "N/A", "2") -> None
        compute_eng_total(None, 5, 2) -> None
    """
    likes_val = parse_k_number(likes)
    comments_val = parse_k_number(comments)
    shares_val = parse_k_number(shares)

    # All three must be numeric to compute total
    if likes_val is None or comments_val is None or shares_val is None:
        return None

    return likes_val + comments_val + shares_val


def normalize_metric_value(value: Union[str, int, None], default: str = "N/A") -> str:
    """
    Normalize a metric value to a string representation.

    - If value is None or "N/A", returns default (usually "N/A")
    - If value is an integer, returns string representation
    - If value is a string that can be parsed as integer, returns it as-is (after stripping)
    - Otherwise, returns default

    Args:
        value: Metric value to normalize
        default: Default string to return if value is invalid (default: "N/A")

    Returns:
        String representation of the metric value
    """
    if value is None:
        return default

    if isinstance(value, int):
        return str(value)

    if isinstance(value, str):
        value = value.strip()
        if not value or value.upper() == "N/A":
            return default
        # If it's already a valid string representation, return it
        # (could be "0", "123", "64.5K", etc.)
        return value

    return default

