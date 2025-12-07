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

# Import integrations
from integrations.google_sheets import append_rows
from integrations.dedupe_store import has_seen, mark_seen

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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def _normalize_reddit_item(
    post: Dict[str, Any], topic: str, verdict_date_override: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    """
    Normalize a Reddit post into the unified schema.

    Args:
        post: Raw Reddit post dictionary from collector
        topic: Topic label for the sheet

    Returns:
        Normalized item dictionary or None if date parsing fails
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
        title = post.get("title", "") or "N/A"
        selftext = post.get("selftext", "") or ""
        score = post.get("score", 0) or 0
        num_comments = post.get("numComments", 0) or 0

        # Build profile
        if username:
            profile = f"u/{username}"
            profile_link = f"https://www.reddit.com/user/{username}"
        else:
            profile = "N/A"
            profile_link = "N/A"

        # Calculate engagement total
        likes_val = int(score) if score is not None else 0
        comments_val = int(num_comments) if num_comments is not None else 0
        shares_val = 0  # Reddit doesn't have separate shares

        if likes_val is not None and comments_val is not None:
            eng_total = str(likes_val + comments_val + shares_val)
        else:
            eng_total = "N/A"

        # Build summary
        summary = selftext if selftext else title
        if len(summary) > 400:
            summary = summary[:400].rsplit(" ", 1)[0]  # Truncate at word boundary

        return {
            "date_posted": date_posted,
            "platform": "Reddit",
            "profile": profile,
            "profile_link": profile_link,
            "followers": "N/A",
            "post_link": url,
            "topic": topic,
            "title": title,
            "tone": "N/A",  # No sentiment analysis yet per requirements
            "views": "N/A",
            "likes": str(likes_val),
            "comments": str(comments_val),
            "shares": str(shares_val),
            "eng_total": eng_total,
            "summary": summary,
            "shrm_like": "",
            "shrm_comment": "",
        }

    except Exception as e:
        logger.error(f"Error normalizing Reddit post: {e}")
        return None


def _normalize_news_item(
    article: Dict[str, Any], topic: str, verdict_date_override: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    """
    Normalize a news article into the unified schema.

    Args:
        article: Raw news article dictionary from collector
        topic: Topic label for the sheet

    Returns:
        Normalized item dictionary or None if date parsing fails
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
        source_name = article.get("source_name", "") or "N/A"
        url = article.get("url", "")
        title = article.get("title", "") or "N/A"
        description = article.get("description", "") or "N/A"

        return {
            "date_posted": date_posted,
            "platform": "News",
            "profile": source_name,
            "profile_link": "N/A",
            "followers": "N/A",
            "post_link": url,
            "topic": topic,
            "title": title,
            "tone": "N/A",
            "views": "N/A",
            "likes": "N/A",
            "comments": "N/A",
            "shares": "N/A",
            "eng_total": "N/A",
            "summary": description,
            "shrm_like": "",
            "shrm_comment": "",
        }

    except Exception as e:
        logger.error(f"Error normalizing news article: {e}")
        return None


def _item_to_row(item: Dict[str, Any]) -> List[Any]:
    """
    Convert a normalized item dictionary to a row matching the sheet column order.

    Column order:
    1. Date Posted
    2. Platform
    3. Profile
    4. Link (profile link)
    5. Nº Of Followers
    6. Post Link
    7. Topic
    8. title
    9. Tone
    10. Views
    11. Likes
    12. Comments
    13. Shares
    14. Eng. Total
    15. Post Summary
    16. SHRM Like
    17. SHRM Comment
    """
    return [
        item["date_posted"],  # 1 Date Posted
        item["platform"],  # 2 Platform
        item["profile"],  # 3 Profile
        item["profile_link"],  # 4 Link (profile)
        item["followers"],  # 5 Nº Of Followers
        item["post_link"],  # 6 Post Link
        item["topic"],  # 7 Topic
        item["title"],  # 8 title
        item["tone"],  # 9 Tone
        item["views"],  # 10 Views
        item["likes"],  # 11 Likes
        item["comments"],  # 12 Comments
        item["shares"],  # 13 Shares
        item["eng_total"],  # 14 Eng. Total
        item["summary"],  # 15 Post Summary
        item["shrm_like"],  # 16 SHRM Like
        item["shrm_comment"],  # 17 SHRM Comment
    ]


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
    new_urls = []

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

            # Check if already seen
            if has_seen(url):
                reddit_stats["filtered_dedupe"] += 1
                continue

            # Normalize (includes date filtering)
            normalized = _normalize_reddit_item(post, topic, verdict_date_override)
            if normalized:
                all_items.append(normalized)
                new_urls.append(url)
                reddit_stats["normalized"] += 1
            else:
                reddit_stats["filtered_date"] += 1

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

            # Check if already seen
            if has_seen(url):
                news_stats["filtered_dedupe"] += 1
                continue

            # Normalize (includes date filtering)
            normalized = _normalize_news_item(article, topic, verdict_date_override)
            if normalized:
                all_items.append(normalized)
                new_urls.append(url)
                news_stats["normalized"] += 1
            else:
                news_stats["filtered_date"] += 1

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

    # Apply max_results limit if specified
    original_count = len(all_items)
    if max_results and len(all_items) > max_results:
        logger.info(f"Limiting results from {len(all_items)} to {max_results}")
        all_items = all_items[:max_results]
        new_urls = new_urls[:max_results]

    # Convert items to rows
    rows = [_item_to_row(item) for item in all_items]

    # Calculate summary statistics
    total_raw = reddit_stats["raw_collected"] + news_stats["raw_collected"]
    total_filtered = (
        reddit_stats["filtered_date"]
        + reddit_stats["filtered_dedupe"]
        + news_stats["filtered_date"]
        + news_stats["filtered_dedupe"]
    )
    total_normalized = len(rows)

    logger.info("=" * 60)
    logger.info("Collection Summary:")
    logger.info(f"  Total raw items collected: {total_raw}")
    logger.info(f"    - Reddit: {reddit_stats['raw_collected']} raw")
    logger.info(f"    - News: {news_stats['raw_collected']} raw")
    logger.info(f"  Total items filtered out: {total_filtered}")
    logger.info(
        f"    - Filtered by date: {reddit_stats['filtered_date'] + news_stats['filtered_date']}"
    )
    logger.info(
        f"    - Filtered by dedupe: {reddit_stats['filtered_dedupe'] + news_stats['filtered_dedupe']}"
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

                # Mark URLs as seen
                mark_seen(new_urls)

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
        help="Comma-separated list of search terms (default: uses collector defaults)",
    )
    parser.add_argument(
        "--topic",
        type=str,
        default="SHRM Trial Verdict",
        help="Topic label for the 'Topic' column in the sheet",
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
        if args.dry_run:
            logger.info(f"DRY RUN: Would have appended {count} rows to Google Sheet")
        else:
            logger.info(f"Successfully appended {count} rows to Google Sheet")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
