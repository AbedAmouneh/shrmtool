"""
Main orchestrator for SHRM content collection.

Collects Reddit posts and news articles, filters by verdict date,
deduplicates, and appends to Google Sheet.
"""

import logging
import sys
from typing import List, Dict, Any, Optional
from datetime import datetime

# Import collectors
from collectors.reddit_collector import collect_reddit_posts
from collectors.news_collector import collect_news_articles
from collectors.x_collector import collect_twitter_posts

# Import integrations
from integrations.google_sheets import append_rows
from integrations.dedupe_store import (
    has_seen,
    mark_seen,
    has_seen_canonical,
    has_seen_canonical_by_platform,
    mark_seen_canonical,
)

# Import notifications
from notifications.telegram_notifier import send_telegram_message
from notifications.message_builder import build_telegram_summary

# Import utils
from utils.config import VERDICT_DATE
from utils.time_utils import (
    parse_reddit_date,
    parse_newsapi_date,
    format_date_mmddyyyy,
    is_after_verdict_date,
)
from utils.sentiment import classify_sentiment_combined
from utils.summary import build_summary
from utils.metrics import parse_k_number, compute_eng_total, normalize_metric_value
from utils.url_utils import canonical_url, is_valid_url
from utils.platform_rules import apply_platform_defaults, validate_platform_item
from utils.schema import build_row, validate_row

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Stores the last run summary for downstream consumers (e.g., notifications).
LAST_RUN_SUMMARY: Optional[Dict[str, Any]] = None


def _normalize_reddit_item(
    post: Dict[str, Any], topic: str, verdict_date_override: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    """
    Normalize a Reddit post into the unified schema.

    Args:
        post: Raw Reddit post dictionary from collector
        topic: Topic label for the sheet
        verdict_date_override: Optional verdict date override

    Returns:
        Normalized item dictionary or None if date parsing fails or URL is invalid
    """
    try:
        # Parse and format date
        if not post.get("date"):
            logger.warning(f"Reddit post missing date: {post.get('url')}")
            return None

        post_date = parse_reddit_date(post["date"])
        if not is_after_verdict_date(post_date, verdict_date_override):
            return None  # Skip posts before verdict date

        date_posted = format_date_mmddyyyy(post_date)

        # Extract fields
        username = post.get("username", "").strip()
        url = post.get("url", "")

        # Validate URL
        if not is_valid_url(url):
            logger.warning(f"Reddit post has invalid URL: {url}")
            return None

        title = post.get("title", "") or "N/A"
        selftext = post.get("selftext", "") or ""

        # Parse metrics using helper
        score = post.get("score", 0) or 0
        num_comments = post.get("numComments", 0) or 0

        likes_val = parse_k_number(score) or 0
        comments_val = parse_k_number(num_comments) or 0
        shares_val = 0  # Reddit doesn't have separate shares

        # Compute engagement total
        eng_total_val = compute_eng_total(likes_val, comments_val, shares_val)
        eng_total = str(eng_total_val) if eng_total_val is not None else "N/A"

        # Build profile
        if username:
            profile = f"u/{username}"
            profile_link = f"https://www.reddit.com/user/{username}"
        else:
            profile = "N/A"
            profile_link = "N/A"

        # Build summary
        summary = selftext if selftext else title
        if len(summary) > 400:
            summary = summary[:400].rsplit(" ", 1)[0]  # Truncate at word boundary

        item = {
            "date_posted": date_posted,
            "platform": "Reddit",
            "profile": profile,
            "profile_link": profile_link,
            "followers": "N/A",
            "post_link": url,
            "topic": topic,
            "title": title,
            "summary": summary,
            "tone": "N/A",
            "category": "",
            "views": "N/A",
            "likes": str(likes_val),
            "comments": str(comments_val),
            "shares": str(shares_val),
            "eng_total": eng_total,
            "sentiment_score": "N/A",
            "verified": "N/A",
            "notes": "",
            # Preserve original text fields for topic filtering
            "selftext": selftext,
            "description": "",
        }

        # Apply platform defaults and validate
        item = apply_platform_defaults(item)
        is_valid, error_msg = validate_platform_item(item)
        if not is_valid:
            logger.warning(f"Reddit item failed platform validation: {error_msg}")
            return None

        return item

    except Exception as e:
        logger.error(f"Error normalizing Reddit post: {e}", exc_info=True)
        return None


def _normalize_news_item(
    article: Dict[str, Any], topic: str, verdict_date_override: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    """
    Normalize a news article into the unified schema.

    Args:
        article: Raw news article dictionary from collector
        topic: Topic label for the sheet
        verdict_date_override: Optional verdict date override

    Returns:
        Normalized item dictionary or None if date parsing fails or URL is invalid
    """
    try:
        # Parse and format date
        if not article.get("publishedAt"):
            logger.warning(f"News article missing date: {article.get('url')}")
            return None

        article_date = parse_newsapi_date(article["publishedAt"])
        if not is_after_verdict_date(article_date, verdict_date_override):
            return None  # Skip articles before verdict date

        date_posted = format_date_mmddyyyy(article_date)

        # Extract fields
        source_name = article.get("source_name", "") or ""
        author = article.get("author", "") or ""
        url = article.get("url", "")

        # Validate URL
        if not is_valid_url(url):
            logger.warning(f"News article has invalid URL: {url}")
            return None

        title = article.get("title", "") or "N/A"
        description = article.get("description", "") or "N/A"

        # Compute profile/source fallback using domain if needed
        if source_name:
            profile_value = source_name
        else:
            try:
                from urllib.parse import urlparse

                parsed = urlparse(url)
                profile_value = parsed.netloc or "N/A"
            except Exception:
                profile_value = "N/A"

        # Build summary with optional source/author suffix
        base_summary = description or ""
        suffix = ""
        if source_name and author:
            suffix = f" (Source: {source_name} – by {author})"
        elif source_name:
            suffix = f" (Source: {source_name})"
        elif author:
            suffix = f" (By {author})"
        post_summary = (base_summary + suffix).strip()

        item = {
            "date_posted": date_posted,
            "platform": "News",
            "profile": profile_value or "N/A",
            "profile_link": profile_value or "N/A",
            "followers": "N/A",
            "post_link": url,
            "topic": topic,
            "title": title,
            "summary": post_summary,
            "tone": "N/A",
            "category": "",
            "views": "N/A",
            "likes": "N/A",
            "comments": "N/A",
            "shares": "N/A",
            "eng_total": "N/A",
            "sentiment_score": "N/A",
            "verified": "N/A",
            "notes": "",
            # Preserve original text fields for topic filtering
            "selftext": "",
            "description": description,
            "source_name": source_name or "",
            "author": author,
        }

        # Apply platform defaults and validate
        item = apply_platform_defaults(item)
        is_valid, error_msg = validate_platform_item(item)
        if not is_valid:
            logger.warning(f"News item failed platform validation: {error_msg}")
            return None

        return item

    except Exception as e:
        logger.error(f"Error normalizing news article: {e}", exc_info=True)
        return None


def classify_topic(item: Dict[str, Any]) -> str:
    """
    Classify an item's topicality: "on_topic", "borderline", or "off_topic".

    Rules:
    1. on_topic: Strong keyword presence (SHRM anchors OR Johnny C. Taylor + case context)
    2. borderline: Weak or single mention (Johnny C. Taylor without case context, generic HR mentions)
    3. off_topic: No relevant keywords

    Args:
        item: Normalized item dictionary with title, selftext (Reddit), or description (News)

    Returns:
        Classification string: "on_topic", "borderline", or "off_topic"
    """
    text_parts = [
        item.get("title") or "",
        item.get("selftext") or "",
        item.get("description") or "",
    ]
    blob = " ".join(text_parts).lower()

    # Core SHRM anchors (strong signals)
    shrm_anchors = [
        "shrm",
        "society for human resource management",
        "shrm verdict",
        "shrm trial",
        "shrm lawsuit",
        "shrm scandal",
        "shrm controversy",
        "shrm harassment",
        "shrm sexual harassment",
        "shrm discrimination",
    ]

    # Johnny C. Taylor name variants
    johnny_anchors = [
        "johnny c. taylor",
        "johnny c taylor",
        "johnny taylor",
        "shrm ceo johnny",
    ]

    # Verdict / case context (strong signals)
    case_anchors = [
        "verdict",
        "trial",
        "lawsuit",
        "suit",
        "case",
        "allegation",
        "allegations",
        "harassment",
        "sexual harassment",
        "scandal",
        "controversy",
        "misconduct",
        "discrimination",
    ]

    # Noise/negative list (generic HR content where SHRM is only peripheral)
    noise_terms = [
        "hr best practices",
        "hr conference",
        "hr certification",
        "hr training",
    ]

    # Count matches
    shrm_matches = sum(1 for anchor in shrm_anchors if anchor in blob)
    johnny_matches = sum(1 for anchor in johnny_anchors if anchor in blob)
    case_matches = sum(1 for anchor in case_anchors if anchor in blob)
    noise_matches = sum(1 for term in noise_terms if term in blob)

    # Rule 1: Strong SHRM presence = on_topic
    if shrm_matches > 0:
        # If noise terms dominate, might be borderline, but for now keep as on_topic
        return "on_topic"

    # Rule 2: Johnny C. Taylor + case context = on_topic
    if johnny_matches > 0 and case_matches > 0:
        return "on_topic"

    # Rule 3: Johnny C. Taylor without case context = borderline
    if johnny_matches > 0:
        return "borderline"

    # Rule 4: Case context alone (without SHRM/Johnny) = borderline
    if case_matches > 0:
        return "borderline"

    # Rule 5: Everything else = off_topic
    return "off_topic"


def _process_item_with_dedupe(
    item: Dict[str, Any],
    platform: str,
    stats: Dict[str, int],
    all_items: List[Dict[str, Any]],
    new_urls: List[str],
    new_canonical_items: List[tuple],
) -> bool:
    """
    Process an item with canonical URL deduplication and repost detection.

    Args:
        item: Normalized item dictionary
        platform: Platform name ("News", "X", "Reddit")
        stats: Statistics dictionary to update
        all_items: List to append items to
        new_urls: List to append URLs to (for legacy dedupe)
        new_canonical_items: List to append (canonical_url, platform, profile, post_url) tuples

    Returns:
        True if item was added, False if it was filtered out
    """
    post_url = item.get("post_link", "")
    if not post_url:
        return False

    # Get canonical URL
    canonical = canonical_url(post_url)
    if not canonical:
        logger.warning(f"Item has invalid URL, skipping: {post_url}")
        stats["filtered_dedupe"] = stats.get("filtered_dedupe", 0) + 1
        return False

    profile = item.get("profile", "")

    # For News: skip if canonical URL already seen (any profile)
    if platform == "News":
        if has_seen_canonical_by_platform(canonical, platform):
            stats["filtered_dedupe"] = stats.get("filtered_dedupe", 0) + 1
            return False
    else:
        # For social platforms: check for exact duplicate (same canonical + same profile)
        has_seen_flag, existing_url = has_seen_canonical(canonical, platform, profile)
        if has_seen_flag:
            stats["filtered_dedupe"] = stats.get("filtered_dedupe", 0) + 1
            return False

        # Check for repost (same canonical, different profile)
        if has_seen_canonical_by_platform(canonical, platform):
            # This is a repost - tag it
            item["category"] = "Repost"
            item["notes"] = f"Repost of canonical URL: {canonical}"
            logger.info(
                f"Detected repost: {post_url} (canonical: {canonical}, profile: {profile})"
            )

    # Item passed dedupe - add it
    all_items.append(item)
    new_urls.append(post_url)  # Legacy dedupe
    new_canonical_items.append((canonical, platform, profile, post_url))
    return True


def is_on_topic(item: Dict[str, Any]) -> bool:
    """
    Check if an item is clearly related to SHRM/JCT using anchor-based filtering.

    This is a convenience wrapper around classify_topic that returns True only for "on_topic".

    Args:
        item: Normalized item dictionary with title, selftext (Reddit), or description (News)

    Returns:
        True if item is on_topic, False otherwise
    """
    return classify_topic(item) == "on_topic"


def _item_to_row(item: Dict[str, Any]) -> List[Any]:
    """
    Convert a normalized item dictionary to a row using the schema builder.

    This function uses build_row from utils.schema to ensure consistent
    17-column format matching the canonical schema.
    """
    return build_row(item)


def main_collect(
    search_terms: List[str],
    topic: str,
    dry_run: bool = False,
    max_results: Optional[int] = None,
    verdict_date_override: Optional[str] = None,
) -> int:
    """
    Orchestrate collection + normalization + dedupe + Google Sheets appending.

    Args:
        search_terms: List of search keywords to pass to both collectors.
        topic: Topic label to use in the 'Topic' column in the sheet.
        dry_run: If True, skip writing to Google Sheets.
        max_results: Optional limit on total items to process.
        verdict_date_override: Optional verdict date override (YYYY-MM-DD format).

    Returns:
        The number of rows successfully appended to Google Sheets (or would be appended in dry-run).
    """
    verdict_date = verdict_date_override if verdict_date_override else VERDICT_DATE
    logger.info("=" * 60)
    logger.info("Starting SHRM content collection pipeline")
    logger.info(f"Search terms: {search_terms}")
    logger.info(f"Topic: {topic}")
    logger.info(f"Verdict date filter: {verdict_date}")
    if dry_run:
        logger.info("DRY RUN MODE: Will not write to Google Sheets")
    if max_results:
        logger.info(f"Max results limit: {max_results}")
    logger.info("=" * 60)

    all_items = []
    new_urls = []  # Legacy dedupe URLs
    new_canonical_items = (
        []
    )  # List of (canonical_url, platform, profile, post_url) tuples

    # Statistics tracking
    reddit_stats = {
        "raw_collected": 0,
        "filtered_date": 0,
        "filtered_dedupe": 0,
        "normalized": 0,
        "errors": 0,
    }
    news_stats = {
        "raw_collected": 0,
        "filtered_date": 0,
        "filtered_dedupe": 0,
        "normalized": 0,
        "errors": 0,
    }
    twitter_stats = {
        "raw_collected": 0,
        "filtered_date": 0,
        "filtered_dedupe": 0,
        "normalized": 0,
        "errors": 0,
    }

    # Collect Reddit posts
    reddit_success = False
    try:
        logger.info("--- Reddit Collector: Starting ---")
        reddit_posts = collect_reddit_posts()
        reddit_stats["raw_collected"] = len(reddit_posts)
        logger.info(f"Reddit: Collected {reddit_stats['raw_collected']} raw posts")

        for post in reddit_posts:
            url = post.get("url")
            if not url:
                continue

            # Normalize (includes date filtering and URL validation)
            normalized = _normalize_reddit_item(post, topic, verdict_date_override)
            if not normalized:
                reddit_stats["filtered_date"] += 1
                continue

            # Process with canonical URL dedupe
            if _process_item_with_dedupe(
                normalized,
                "Reddit",
                reddit_stats,
                all_items,
                new_urls,
                new_canonical_items,
            ):
                reddit_stats["normalized"] += 1

        logger.info(
            f"Reddit: {reddit_stats['normalized']} normalized, "
            f"{reddit_stats['filtered_date']} filtered by date, "
            f"{reddit_stats['filtered_dedupe']} filtered by dedupe"
        )
        logger.info("--- Reddit Collector: Completed ---")
        reddit_success = True

    except Exception as e:
        reddit_stats["errors"] = 1
        logger.error(f"Reddit Collector: Failed with error: {e}", exc_info=True)
        logger.warning("Continuing with news collection despite Reddit failure")

    # Collect news articles
    news_success = False
    try:
        logger.info("--- News Collector: Starting ---")
        news_articles = collect_news_articles()
        news_stats["raw_collected"] = len(news_articles)
        logger.info(f"News: Collected {news_stats['raw_collected']} raw articles")

        for article in news_articles:
            url = article.get("url")
            if not url:
                continue

            # Normalize (includes date filtering and URL validation)
            normalized = _normalize_news_item(article, topic, verdict_date_override)
            if not normalized:
                news_stats["filtered_date"] += 1
                continue

            # Process with canonical URL dedupe (News: skip duplicates entirely)
            if _process_item_with_dedupe(
                normalized, "News", news_stats, all_items, new_urls, new_canonical_items
            ):
                news_stats["normalized"] += 1

        logger.info(
            f"News: {news_stats['normalized']} normalized, "
            f"{news_stats['filtered_date']} filtered by date, "
            f"{news_stats['filtered_dedupe']} filtered by dedupe"
        )
        logger.info("--- News Collector: Completed ---")
        news_success = True

    except Exception as e:
        news_stats["errors"] = 1
        logger.error(f"News Collector: Failed with error: {e}", exc_info=True)
        logger.warning("Continuing despite news collection failure")

    # Collect Twitter posts
    twitter_success = False
    try:
        logger.info("--- Twitter Collector: Starting ---")
        twitter_posts = collect_twitter_posts(
            search_terms=search_terms,
            topic=topic,
            verdict_date_override=verdict_date_override,
        )
        twitter_stats["raw_collected"] = len(twitter_posts)
        logger.info(f"Twitter: Collected {twitter_stats['raw_collected']} raw posts")

        for tweet in twitter_posts:
            url = tweet.get("post_link")
            if not url:
                continue

            # Process with canonical URL dedupe (X: detect reposts)
            if _process_item_with_dedupe(
                tweet, "X", twitter_stats, all_items, new_urls, new_canonical_items
            ):
                twitter_stats["normalized"] += 1

        logger.info(
            f"Twitter: {twitter_stats['normalized']} normalized, "
            f"{twitter_stats['filtered_date']} filtered by date, "
            f"{twitter_stats['filtered_dedupe']} filtered by dedupe"
        )
        logger.info("--- Twitter Collector: Completed ---")
        twitter_success = True

    except Exception as e:
        twitter_stats["errors"] = 1
        logger.error(f"Twitter Collector: Failed with error: {e}", exc_info=True)
        logger.warning("Continuing despite Twitter collection failure")

    # Apply on-topic anchor filtering (final safety layer)
    items_before_topic_filter = len(all_items)
    topic_classifications = {"on_topic": 0, "borderline": 0, "off_topic": 0}
    filtered_items = []

    for item in all_items:
        classification = classify_topic(item)
        topic_classifications[classification] = (
            topic_classifications.get(classification, 0) + 1
        )
        if classification == "on_topic":
            filtered_items.append(item)

    all_items = filtered_items
    items_filtered_topic = items_before_topic_filter - len(all_items)

    logger.info(
        f"Topic filtering: {topic_classifications['on_topic']} on-topic, "
        f"{topic_classifications['borderline']} borderline, "
        f"{topic_classifications['off_topic']} off-topic. "
        f"Keeping {len(all_items)} on-topic items."
    )

    # Apply max_results limit if specified
    original_count = len(all_items)
    if max_results and len(all_items) > max_results:
        logger.info(f"Limiting results from {len(all_items)} to {max_results}")
        all_items = all_items[:max_results]
        new_urls = new_urls[:max_results]

    # Convert items to rows with validation
    rows = []
    validation_failures = 0
    for item in all_items:
        row = _item_to_row(item)
        if validate_row(row):
            rows.append(row)
        else:
            validation_failures += 1
            logger.error(
                f"Row validation failed for item: {item.get('post_link', 'unknown')}. Skipping."
            )

    if validation_failures > 0:
        logger.warning(
            f"Row validation: {validation_failures} rows failed validation and were skipped"
        )

    # Aggregate counts for notification/summary
    dedupe_filtered_total = (
        reddit_stats["filtered_dedupe"]
        + news_stats["filtered_dedupe"]
        + twitter_stats["filtered_dedupe"]
    )
    offtopic_filtered = (
        topic_classifications["borderline"] + topic_classifications["off_topic"]
    )
    news_appended_count = sum(1 for r in rows if r[1] == "News")
    twitter_appended_count = sum(1 for r in rows if r[1] in ("X", "Twitter"))
    repost_appended_count = sum(1 for r in rows if str(r[8]).lower() == "repost")

    global LAST_RUN_SUMMARY
    LAST_RUN_SUMMARY = {
        "total_new": len(rows),
        "news_appended": news_appended_count,
        "twitter_appended": twitter_appended_count,
        "repost_appended": repost_appended_count,
        "dedupe_filtered": dedupe_filtered_total,
        "offtopic_filtered": offtopic_filtered,
        "topic_classifications": topic_classifications,
        "validation_failures": validation_failures,
    }

    # Calculate summary statistics
    total_raw = (
        reddit_stats["raw_collected"]
        + news_stats["raw_collected"]
        + twitter_stats["raw_collected"]
    )
    total_filtered = (
        reddit_stats["filtered_date"]
        + reddit_stats["filtered_dedupe"]
        + news_stats["filtered_date"]
        + news_stats["filtered_dedupe"]
        + twitter_stats["filtered_date"]
        + twitter_stats["filtered_dedupe"]
    )
    total_normalized = len(rows)

    logger.info("=" * 60)
    logger.info("Collection Summary:")
    logger.info(f"  Total raw items collected: {total_raw}")
    logger.info(f"    - Reddit: {reddit_stats['raw_collected']} raw")
    logger.info(f"    - News: {news_stats['raw_collected']} raw")
    logger.info(f"    - Twitter: {twitter_stats['raw_collected']} raw")
    logger.info(f"  Total items filtered out: {total_filtered}")
    logger.info(
        f"    - Filtered by date: "
        f"{reddit_stats['filtered_date'] + news_stats['filtered_date'] + twitter_stats['filtered_date']}"
    )
    logger.info(
        f"    - Filtered by dedupe: "
        f"{reddit_stats['filtered_dedupe'] + news_stats['filtered_dedupe'] + twitter_stats['filtered_dedupe']}"
    )
    logger.info(f"  Total items normalized: {total_normalized}")
    if max_results and original_count > max_results:
        logger.info(f"  Limited from {original_count} to {max_results} by max-results")
    logger.info("=" * 60)

    # Append rows to Google Sheet
    if rows:
        if dry_run:
            logger.info(f"DRY RUN: Would append {len(rows)} rows to Google Sheet")
            logger.info(f"DRY RUN: Would mark {len(new_urls)} URLs as seen")
            logger.info("=" * 60)
            if reddit_success and news_success:
                logger.info("Final Status: SUCCESS (dry-run)")
            elif reddit_success or news_success:
                logger.warning(
                    "Final Status: PARTIAL SUCCESS (dry-run, one collector failed)"
                )
            else:
                logger.error("Final Status: FAILURE (dry-run, both collectors failed)")
            logger.info("=" * 60)
            return len(rows)
        else:
            try:
                logger.info(f"Appending {len(rows)} rows to Google Sheet...")
                append_rows(rows)

                # Mark URLs as seen (legacy dedupe)
                mark_seen(new_urls)

                # Mark canonical URLs as seen (enhanced dedupe)
                from datetime import datetime

                current_date = datetime.now().strftime("%Y-%m-%d")
                for canonical, platform, profile, post_url in new_canonical_items:
                    mark_seen_canonical(
                        canonical, platform, post_url, profile, current_date
                    )

                logger.info(f"Successfully appended {len(rows)} rows")
                logger.info("=" * 60)
                if reddit_success and news_success:
                    logger.info("Final Status: SUCCESS")
                elif reddit_success or news_success:
                    logger.warning(
                        "Final Status: PARTIAL SUCCESS (one collector failed)"
                    )
                else:
                    logger.error("Final Status: FAILURE (both collectors failed)")
                logger.info("=" * 60)
                return len(rows)

            except Exception as e:
                logger.error(
                    f"Error appending rows to Google Sheet: {e}", exc_info=True
                )
                logger.error("Final Status: FAILURE")
                logger.info("=" * 60)
                raise
    else:
        logger.info("No new content to append")
        logger.info("=" * 60)
        if reddit_success and news_success:
            logger.info("Final Status: SUCCESS (no new content found)")
        elif reddit_success or news_success:
            logger.warning(
                "Final Status: PARTIAL SUCCESS (one collector failed, no new content)"
            )
        else:
            logger.error("Final Status: FAILURE (both collectors failed)")
        logger.info("=" * 60)
        return 0


def main():
    """Main execution function (CLI entrypoint)."""
    import argparse
    from collectors.reddit_collector import REDDIT_SEARCH_TERMS

    parser = argparse.ArgumentParser(
        description="SHRM Content Collection Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--terms",
        type=str,
        help="Comma-separated list of search terms. Recommended: 'SHRM verdict,SHRM trial,SHRM lawsuit,SHRM scandal,SHRM controversy,SHRM harassment allegations,SHRM sexual harassment case,Johnny C. Taylor SHRM,SHRM CEO Johnny Taylor,Society for Human Resource Management trial,Society for Human Resource Management verdict'",
    )
    parser.add_argument(
        "--topic",
        type=str,
        default="SHRM Trial Verdict – Public & HR Community Reaction",
        help="Topic label for the 'Topic' column in the sheet. Default: 'SHRM Trial Verdict – Public & HR Community Reaction'",
    )
    parser.add_argument(
        "--since",
        type=str,
        help="Override verdict date filter (YYYY-MM-DD format)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run without writing to Google Sheets",
    )
    parser.add_argument(
        "--max-results",
        type=int,
        help="Maximum number of items to process",
    )

    args = parser.parse_args()

    # Validate topic
    if not args.topic or not args.topic.strip():
        logger.error("Topic cannot be empty")
        sys.exit(1)

    # Parse search terms
    if args.terms:
        search_terms = [term.strip() for term in args.terms.split(",") if term.strip()]
        if not search_terms:
            logger.error("No valid search terms provided")
            sys.exit(1)
    else:
        search_terms = REDDIT_SEARCH_TERMS

    # Validate --since format if provided
    if args.since:
        try:
            from datetime import datetime

            datetime.strptime(args.since, "%Y-%m-%d")
        except ValueError:
            logger.error(
                f"Invalid date format for --since: {args.since}. Expected YYYY-MM-DD"
            )
            sys.exit(1)

    try:
        count = main_collect(
            search_terms=search_terms,
            topic=args.topic.strip(),
            dry_run=args.dry_run,
            max_results=args.max_results,
            verdict_date_override=args.since,
        )
        # Summary log
        if args.dry_run:
            logger.info("DRY RUN: Would have appended %s rows to Google Sheet", count)
        else:
            logger.info("Appended %s rows to Google Sheet", count)

        # 🔔 Telegram notification (only on real runs, only if something new)
        if not args.dry_run and count > 0:
            try:
                summary = LAST_RUN_SUMMARY or {}
                msg = build_telegram_summary(
                    topic=args.topic.strip(),
                    search_terms=search_terms,
                    total_new=count,
                    news_count=summary.get("news_appended", 0),
                    twitter_count=summary.get("twitter_appended", 0),
                    repost_count=summary.get("repost_appended", 0),
                    dedupe_count=summary.get("dedupe_filtered", 0),
                    offtopic_count=summary.get("offtopic_filtered", 0),
                )
                send_telegram_message(msg)
            except Exception as e:
                logger.error("Failed to send Telegram notification: %s", e)

        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
