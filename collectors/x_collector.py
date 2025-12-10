"""
X (Twitter) collector using Recent Search API v2.

Fetches recent tweets for given search terms, normalizes into SHRM schema, applies
verdict-date filtering, and performs per-run deduplication (no global DB calls).
"""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Optional

import requests

from utils.time_utils import (
    parse_newsapi_date,
    format_date_mmddyyyy,
    is_after_verdict_date,
)
from utils.metrics import parse_k_number, compute_eng_total
from utils.url_utils import is_valid_url
from utils.platform_rules import apply_platform_defaults, validate_platform_item

logger = logging.getLogger(__name__)

X_BEARER_TOKEN = os.getenv("X_BEARER_TOKEN")
SEARCH_URL = "https://api.twitter.com/2/tweets/search/recent"


def _build_headers() -> Dict[str, str]:
    if not X_BEARER_TOKEN:
        return {}
    return {"Authorization": f"Bearer {X_BEARER_TOKEN}"}


def _normalize_tweet(
    tweet: Dict[str, Any],
    user_lookup: Dict[str, Dict[str, Any]],
    topic: str,
    verdict_date_override: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    try:
        tweet_id = tweet.get("id")
        if not tweet_id:
            return None

        created_at = tweet.get("created_at")
        if not created_at:
            return None

        dt = parse_newsapi_date(created_at)
        if not is_after_verdict_date(dt, verdict_date_override):
            return None

        date_posted = format_date_mmddyyyy(dt)

        author_id = tweet.get("author_id")
        user = user_lookup.get(author_id, {}) if author_id else {}
        username = user.get("username", "") or ""
        handle = f"@{username}" if username else "N/A"
        profile_link = f"https://x.com/{username}" if username else "N/A"

        # Parse followers using metrics helper
        followers_raw = user.get("public_metrics", {}).get("followers_count")
        followers_val = (
            parse_k_number(followers_raw) if followers_raw is not None else None
        )
        followers_str = str(followers_val) if followers_val is not None else "N/A"

        text = tweet.get("text", "") or ""
        title = text[:160] if len(text) > 160 else text
        if not title:
            title = "N/A"

        public_metrics = tweet.get("public_metrics", {}) or {}
        like_count_raw = public_metrics.get("like_count", 0) or 0
        reply_count_raw = public_metrics.get("reply_count", 0) or 0
        retweet_count_raw = public_metrics.get("retweet_count", 0) or 0
        quote_count_raw = public_metrics.get("quote_count", 0) or 0
        impressions_raw = public_metrics.get("impression_count")

        # Parse metrics using helper
        like_count = parse_k_number(like_count_raw) or 0
        reply_count = parse_k_number(reply_count_raw) or 0
        retweet_count = parse_k_number(retweet_count_raw) or 0
        quote_count = parse_k_number(quote_count_raw) or 0
        impressions = (
            parse_k_number(impressions_raw) if impressions_raw is not None else None
        )

        shares = retweet_count + quote_count
        eng_total_val = compute_eng_total(like_count, reply_count, shares)
        eng_total = str(eng_total_val) if eng_total_val is not None else "N/A"

        url = f"https://twitter.com/i/web/status/{tweet_id}"

        # Validate URL
        if not is_valid_url(url):
            logger.warning(f"X post has invalid URL: {url}")
            return None

        item = {
            "date_posted": date_posted,
            "platform": "X",
            "profile": handle,
            "profile_link": profile_link,
            "followers": followers_str,
            "post_link": url,
            "topic": topic,
            "title": title,
            "summary": title,
            "tone": "N/A",
            "category": "",
            "views": str(impressions) if impressions is not None else "N/A",
            "likes": str(like_count),
            "comments": str(reply_count),
            "shares": str(shares),
            "eng_total": eng_total,
            "sentiment_score": "N/A",
            "verified": "N/A",
            "notes": "",
            # Preserve fields for topic filtering
            "description": text,
            "selftext": "",
        }

        # Apply platform defaults and validate
        item = apply_platform_defaults(item)
        is_valid, error_msg = validate_platform_item(item)
        if not is_valid:
            logger.warning(f"X item failed platform validation: {error_msg}")
            return None

        return item
    except Exception as e:
        logger.error(f"Error normalizing tweet: {e}", exc_info=True)
        return None


def collect_twitter_posts(
    search_terms: List[str],
    topic: str,
    verdict_date_override: Optional[str] = None,
    max_results: int = 100,
) -> List[Dict[str, Any]]:
    """
    Collect tweets for all search terms using X Recent Search API.

    Per-run dedupe only (no DB calls). Global dedupe happens in main_collect.
    """
    results: List[Dict[str, Any]] = []

    if not X_BEARER_TOKEN:
        logger.info("Twitter Collector: Skipped because X_BEARER_TOKEN is not set")
        return results

    headers = _build_headers()
    seen_urls = set()

    for idx, query in enumerate(search_terms, start=1):
        logger.info(
            f"Twitter Collector: Processing query {idx}/{len(search_terms)}: '{query}'"
        )
        raw_count = 0
        normalized_count = 0
        filtered_date = 0
        skipped_missing = 0

        params = {
            "query": query,
            "max_results": min(max_results, 100),
            "tweet.fields": "created_at,public_metrics,text,author_id",
            "expansions": "author_id",
            "user.fields": "username,public_metrics",
        }

        try:
            resp = requests.get(SEARCH_URL, headers=headers, params=params, timeout=30)
            if resp.status_code != 200:
                logger.warning(
                    f"Twitter Collector: Query '{query}' failed with {resp.status_code}: {resp.text[:500]}"
                )
                continue

            data = resp.json() if resp.text else {}
            tweets = data.get("data", []) or []
            users = data.get("includes", {}).get("users", []) or []
            user_lookup = {u.get("id"): u for u in users}

            raw_count = len(tweets)

            for tweet in tweets:
                normalized = _normalize_tweet(
                    tweet, user_lookup, topic, verdict_date_override
                )
                if not normalized:
                    filtered_date += 1
                    continue

                url = normalized.get("post_link")
                if not url or url in seen_urls:
                    skipped_missing += 1
                    continue

                seen_urls.add(url)
                results.append(normalized)
                normalized_count += 1

            logger.info(
                f"Twitter Collector: Query '{query}': raw={raw_count}, "
                f"normalized={normalized_count}, filtered_date={filtered_date}, "
                f"skipped_dedupe_or_missing={skipped_missing}"
            )

        except Exception as e:
            logger.error(
                f"Twitter Collector: Error collecting tweets for '{query}': {e}",
                exc_info=True,
            )
            continue

    logger.info(
        f"Twitter Collector: Completed - {len(results)} unique tweets collected across {len(search_terms)} queries"
    )
    return results
