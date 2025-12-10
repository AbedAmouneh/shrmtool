"""
URL normalization and validation utilities.

Provides:
- Canonical URL generation (normalize scheme, strip tracking params, etc.)
- URL validation
"""

from __future__ import annotations

import logging
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode

logger = logging.getLogger(__name__)

# Common tracking parameter names to strip
TRACKING_PARAMS = {
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_term",
    "utm_content",
    "fbclid",
    "gclid",
    "ref",
    "source",
    "medium",
    "campaign",
    "_ga",
    "mc_cid",
    "mc_eid",
}


def canonical_url(url: str) -> str:
    """
    Normalize a URL to its canonical form for deduplication.

    Operations:
    1. Normalize scheme (http -> https where reasonable)
    2. Strip tracking parameters (utm_*, fbclid, gclid, etc.)
    3. Remove fragments (#...)
    4. Remove trailing slashes from path (except root)
    5. Lowercase hostname

    Args:
        url: Raw URL string

    Returns:
        Canonical URL string

    Examples:
        "https://example.com/article?utm_source=twitter#section" -> "https://example.com/article"
        "http://example.com/page/" -> "https://example.com/page"
        "https://Example.com/Path" -> "https://example.com/Path"
    """
    if not url or not url.strip():
        return ""

    url = url.strip()

    # Parse URL
    try:
        parsed = urlparse(url)
    except Exception as e:
        logger.warning(f"Failed to parse URL '{url}': {e}")
        return url  # Return original if parsing fails

    # Normalize scheme (http -> https for common domains)
    scheme = parsed.scheme.lower()
    if scheme == "http":
        # Only upgrade to https if it's a common domain pattern
        # For now, we'll upgrade all http to https
        scheme = "https"

    # Normalize hostname (lowercase)
    hostname = parsed.netloc.lower() if parsed.netloc else ""

    # Normalize path (remove trailing slash except for root)
    path = parsed.path.rstrip("/") if parsed.path != "/" else "/"

    # Strip tracking parameters from query string
    query_params = parse_qs(parsed.query, keep_blank_values=False)
    filtered_params = {
        k: v for k, v in query_params.items() if k.lower() not in TRACKING_PARAMS
    }
    query = urlencode(filtered_params, doseq=True) if filtered_params else ""

    # Remove fragment
    fragment = ""

    # Reconstruct URL
    canonical = urlunparse((scheme, hostname, path, parsed.params, query, fragment))

    return canonical


def is_valid_url(url: str) -> bool:
    """
    Check if a URL is well-formed and valid.

    A valid URL must have:
    - A scheme (http:// or https://)
    - A hostname (domain)

    Args:
        url: URL string to validate

    Returns:
        True if URL is valid, False otherwise
    """
    if not url or not url.strip():
        return False

    try:
        parsed = urlparse(url.strip())
        # Must have scheme and netloc (hostname)
        if not parsed.scheme or not parsed.netloc:
            return False
        # Scheme should be http or https
        if parsed.scheme.lower() not in ("http", "https"):
            return False
        return True
    except Exception:
        return False

