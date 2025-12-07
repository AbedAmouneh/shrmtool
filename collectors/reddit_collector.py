"""
Reddit content collector using snscrape.

This module collects SHRM-related posts from Reddit by running snscrape
search commands and parsing the JSONL output.
"""

import json
import subprocess
from typing import List, Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)

# Search terms for Reddit
REDDIT_SEARCH_TERMS = [
    "SHRM",
    "SHRM discrimination",
    "SHRM trial",
    "SHRM verdict",
    "Society for Human Resource Management verdict",
]


def run_snscrape_search(query: str, max_results: int = 100) -> List[Dict[str, Any]]:
    """
    Run snscrape reddit-search command and parse JSONL output.

    Args:
        query: Search query string
        max_results: Maximum number of results to fetch (default: 100)

    Returns:
        List of parsed Reddit post dictionaries

    Raises:
        subprocess.CalledProcessError: If snscrape command fails
        ValueError: If output cannot be parsed
    """
    results = []

    try:
        # Run snscrape command
        # Format: snscrape reddit-search "query" --jsonl
        cmd = ["snscrape", "reddit-search", query, "--jsonl"]

        logger.info(f"Running snscrape for query: {query}")
        process = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300,  # 5 minute timeout
            check=False,  # Don't raise on non-zero exit
        )

        if process.returncode != 0:
            logger.warning(
                f"snscrape returned non-zero exit code {process.returncode} "
                f"for query '{query}': {process.stderr}"
            )
            # Continue anyway, might still have partial results

        # Parse JSONL output (one JSON object per line)
        for line in process.stdout.strip().split("\n"):
            if not line.strip():
                continue

            try:
                post_data = json.loads(line)
                results.append(post_data)

                if len(results) >= max_results:
                    break

            except json.JSONDecodeError as e:
                logger.warning(f"Failed to parse JSON line: {e}")
                continue

    except subprocess.TimeoutExpired:
        logger.error(f"snscrape timed out for query: {query}")
    except FileNotFoundError:
        raise RuntimeError(
            "snscrape not found. Please install it: pip install snscrape"
        )
    except Exception as e:
        logger.error(f"Error running snscrape for query '{query}': {e}")
        raise

    return results


def normalize_reddit_post(post_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize a Reddit post from snscrape output to a standard format.

    Args:
        post_data: Raw post data from snscrape JSONL

    Returns:
        Normalized dictionary with fields: url, title, date, subreddit,
        score, numComments, selftext, username
    """
    # Extract fields with fallbacks
    url = post_data.get("url", "")
    title = post_data.get("title", "")

    # Date handling - snscrape uses different field names
    date_str = (
        post_data.get("date")
        or post_data.get("created")
        or post_data.get("created_utc")
    )
    if isinstance(date_str, (int, float)):
        # Unix timestamp - convert to ISO string
        from datetime import datetime
        import pytz

        dt = datetime.fromtimestamp(date_str, tz=pytz.UTC)
        date = dt.isoformat()
    elif isinstance(date_str, str):
        # ISO string - use as is
        date = date_str
    else:
        # Try to get from other fields or use None
        date = None

    subreddit = (
        post_data.get("subreddit", {}).get("name", "")
        if isinstance(post_data.get("subreddit"), dict)
        else post_data.get("subreddit", "")
    )
    score = post_data.get("score", 0) or 0
    num_comments = (
        post_data.get("numComments", 0) or post_data.get("commentCount", 0) or 0
    )
    selftext = post_data.get("selftext", "") or post_data.get("content", "") or ""

    # Author/username
    author = post_data.get("author") or post_data.get("user")
    if isinstance(author, dict):
        username = author.get("username", "") or author.get("name", "")
    else:
        username = author or ""

    return {
        "url": url,
        "title": title,
        "date": date,
        "subreddit": subreddit,
        "score": score,
        "numComments": num_comments,
        "selftext": selftext,
        "username": username,
    }


def collect_reddit_posts() -> List[Dict[str, Any]]:
    """
    Collect Reddit posts for all search terms.

    Returns:
        List of normalized Reddit post dictionaries
    """
    logger.info("Reddit Collector: Starting collection")
    all_posts = []
    seen_urls = set()  # Deduplicate within this collection run
    query_count = 0
    error_count = 0
    total_raw = 0
    json_parse_errors = 0

    for query in REDDIT_SEARCH_TERMS:
        query_count += 1
        logger.info(
            f"Reddit Collector: Processing query {query_count}/{len(REDDIT_SEARCH_TERMS)}: '{query}'"
        )
        try:
            posts = run_snscrape_search(query, max_results=100)
            raw_count = len(posts)
            total_raw += raw_count
            logger.info(
                f"Reddit Collector: Query '{query}' returned {raw_count} raw posts"
            )

            normalized_count = 0
            skipped_count = 0

            for post_data in posts:
                try:
                    normalized = normalize_reddit_post(post_data)

                    # Skip if URL is missing or already seen
                    if not normalized.get("url"):
                        skipped_count += 1
                        continue

                    if normalized["url"] in seen_urls:
                        skipped_count += 1
                        continue

                    seen_urls.add(normalized["url"])
                    all_posts.append(normalized)
                    normalized_count += 1
                except Exception as e:
                    json_parse_errors += 1
                    logger.warning(f"Reddit Collector: Error normalizing post: {e}")
                    continue

            if skipped_count > 0:
                logger.info(
                    f"Reddit Collector: Query '{query}': {normalized_count} normalized, {skipped_count} skipped (duplicates/missing URL)"
                )
            else:
                logger.info(
                    f"Reddit Collector: Query '{query}': {normalized_count} normalized"
                )

        except Exception as e:
            error_count += 1
            logger.error(
                f"Reddit Collector: Error collecting posts for query '{query}': {e}",
                exc_info=True,
            )
            continue

    logger.info(
        f"Reddit Collector: Completed - {len(all_posts)} unique posts collected from {query_count} queries, {total_raw} total raw posts, {json_parse_errors} parse errors, {error_count} query errors"
    )
    return all_posts
