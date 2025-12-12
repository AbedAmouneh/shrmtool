"""
Reddit content collector using RSS feeds.

This module collects SHRM-related posts from Reddit by querying Reddit's public RSS feeds.
No authentication or API keys required.
"""

import logging
import re
from datetime import datetime
from html import unescape
from typing import Any, Dict, List, Optional
from urllib.parse import quote, urlparse

import feedparser
import pytz
import requests

from utils.schema import build_row, validate_row
from utils.time_utils import format_date_mmddyyyy, is_after_verdict_date, UTC, EASTERN
from utils.url_utils import is_valid_url
from utils.platform_rules import apply_platform_defaults, validate_platform_item

logger = logging.getLogger(__name__)

# Base URL for Reddit RSS search
REDDIT_RSS_BASE_URL = "https://www.reddit.com/search.rss"

# Custom User-Agent to avoid Reddit blocking
REDDIT_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# Default search terms
DEFAULT_SEARCH_TERMS = [
    "SHRM verdict",
    "Johnny C. Taylor",
    "SHRM lawsuit",
    "SHRM discrimination",
]

# Backward compatibility alias
REDDIT_SEARCH_TERMS = DEFAULT_SEARCH_TERMS


def _strip_html(text: str) -> str:
    """
    Strip HTML tags from text and unescape HTML entities.
    
    Args:
        text: Text that may contain HTML
        
    Returns:
        Plain text with HTML removed
    """
    if not text:
        return ""
    
    # Remove HTML tags using regex (simple approach)
    text = re.sub(r"<[^>]+>", "", text)
    # Unescape HTML entities (&amp; -> &, etc.)
    text = unescape(text)
    # Normalize whitespace
    text = re.sub(r"\s+", " ", text).strip()
    
    return text


def _clean_reddit_summary(entry: Dict[str, Any], title: str) -> str:
    """
    Extract and clean Reddit post summary, removing boilerplate text.
    
    Args:
        entry: Raw RSS entry from feedparser
        title: The post title (used as fallback)
        
    Returns:
        Cleaned summary text
    """
    raw_text = ""
    
    # 1. Prefer entry.content[0].value if available (often has the real text)
    content_list = entry.get("content", [])
    if content_list and isinstance(content_list, list) and len(content_list) > 0:
        raw_text = content_list[0].get("value", "") if isinstance(content_list[0], dict) else ""
    
    # 2. Fall back to summary or description if content was empty
    if not raw_text:
        raw_text = entry.get("summary", "") or entry.get("description", "") or ""
    
    # 2. Strip HTML tags
    text = _strip_html(raw_text)
    
    # 3. Remove common Reddit RSS boilerplate patterns
    # Pattern: "submitted by /u/username to r/subreddit"
    text = re.sub(r"submitted\s+by\s+/?u/\w+\s+to\s+r/\w+", "", text, flags=re.IGNORECASE)
    
    # Pattern: "[link]" and "[comments]" markers
    text = re.sub(r"\[link\]", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\[comments\]", "", text, flags=re.IGNORECASE)
    
    # Pattern: "submitted by /u/username"
    text = re.sub(r"submitted\s+by\s+/?u/\w+", "", text, flags=re.IGNORECASE)
    
    # Pattern: "to r/subreddit"
    text = re.sub(r"\s+to\s+r/\w+", "", text, flags=re.IGNORECASE)
    
    # Pattern: standalone "/u/username" or "u/username"
    text = re.sub(r"/?u/\w+", "", text)
    
    # Pattern: standalone "r/subreddit"
    text = re.sub(r"\br/\w+\b", "", text)
    
    # Normalize whitespace after removals
    text = re.sub(r"\s+", " ", text).strip()
    
    # 4. If the cleaned summary is too short (< 20 chars), use the title
    if len(text) < 20:
        # Use title as summary, but limit to 300 chars
        return title[:300] if title else "No Content"
    
    return text


def _parse_rss_date(date_str: str) -> Optional[datetime]:
    """
    Parse an RFC 3339 date string from RSS feed.
    
    Args:
        date_str: Date string in RFC 3339 format (e.g., "2025-12-12T10:30:00+00:00")
        
    Returns:
        Datetime object in UTC, or None if parsing fails
    """
    if not date_str:
        return None
    
    try:
        # feedparser provides parsed time tuple via _parse_date
        # This handles various RSS date formats
        try:
            parsed = feedparser._parse_date(date_str)
            if parsed:
                # Convert to datetime (parsed is a 9-tuple: (year, month, day, hour, minute, second, weekday, yearday, tz)
                dt = datetime(*parsed[:6], tzinfo=UTC)
                return dt
        except (AttributeError, TypeError):
            # _parse_date might not be available or might fail
            pass
    except Exception:
        pass
    
    # Fallback: try ISO format parsing
    try:
        # Handle common formats
        date_clean = date_str.strip()
        if date_clean.endswith("Z"):
            date_clean = date_clean.replace("Z", "+00:00")
        
        dt = datetime.fromisoformat(date_clean)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        else:
            dt = dt.astimezone(UTC)
        return dt
    except Exception as e:
        logger.warning(f"Failed to parse RSS date '{date_str}': {e}")
        return None


def _extract_profile_link(author: str) -> str:
    """
    Extract Reddit profile link from author field.
    
    Args:
        author: Author string (e.g., "/u/username" or "username")
        
    Returns:
        Full profile URL or "N/A"
    """
    if not author:
        return "N/A"
    
    # Remove common prefixes
    author = author.strip()
    if author.startswith("/u/"):
        author = author[3:]
    elif author.startswith("u/"):
        author = author[2:]
    
    if not author:
        return "N/A"
    
    return f"https://www.reddit.com/user/{author}"


class RedditCollector:
    """Collector for Reddit posts via RSS feeds."""
    
    def __init__(self):
        """Initialize collector."""
        pass
    
    def collect(
        self,
        keywords: Optional[List[str]] = None,
        topic: str = "SHRM Trial Verdict – Public & HR Community Reaction",
    ) -> List[Dict[str, Any]]:
        """
        Collect Reddit posts for given keywords using RSS feeds.
        
        Args:
            keywords: List of search keywords. Defaults to DEFAULT_SEARCH_TERMS
            topic: Topic label for the sheet
            
        Returns:
            List of normalized item dictionaries that pass validation
        """
        if keywords is None:
            keywords = DEFAULT_SEARCH_TERMS
        
        logger.info("Reddit RSS Collector: Starting collection")
        logger.info(f"Reddit RSS Collector: Using {len(keywords)} search keywords")
        
        all_items = []
        seen_urls = set()  # Per-run deduplication
        total_found = 0
        total_validated = 0
        relevance_filtered = 0
        
        for idx, keyword in enumerate(keywords, start=1):
            logger.info(
                f"Reddit RSS Collector: Processing query {idx}/{len(keywords)}: '{keyword}'"
            )
            
            try:
                # Construct RSS URL with query parameters
                query_params = {
                    "q": keyword,
                    "sort": "new",
                    "t": "week",  # Past week
                }
                
                # Build URL
                query_string = "&".join([f"{k}={quote(str(v))}" for k, v in query_params.items()])
                rss_url = f"{REDDIT_RSS_BASE_URL}?{query_string}"
                
                # Fetch RSS feed with custom User-Agent
                headers = {"User-Agent": REDDIT_USER_AGENT}
                response = requests.get(rss_url, headers=headers, timeout=30)
                
                if response.status_code != 200:
                    logger.warning(
                        f"Reddit RSS Collector: Query '{keyword}' failed with "
                        f"{response.status_code}: {response.text[:200]}"
                    )
                    continue
                
                # Parse RSS feed
                # feedparser can parse from URL or content
                # We'll parse from the response content to ensure User-Agent is used
                feed = feedparser.parse(response.content)
                
                entries = feed.get("entries", [])
                raw_count = len(entries)
                total_found += raw_count
                
                logger.info(
                    f"Reddit RSS Collector: Query '{keyword}' returned {raw_count} entries"
                )
                
                for entry in entries:
                    try:
                        normalized = self._normalize_entry(
                            entry, topic, seen_urls
                        )
                        
                        if normalized:
                            # Validate by building and checking row
                            row = build_row(normalized)
                            if validate_row(row):
                                all_items.append(normalized)
                                total_validated += 1
                                seen_urls.add(normalized.get("post_link", ""))
                            else:
                                logger.warning(
                                    f"Reddit RSS Collector: Item failed validation: "
                                    f"{normalized.get('post_link', 'unknown')}"
                                )
                        else:
                            relevance_filtered += 1
                    
                    except Exception as e:
                        logger.warning(
                            f"Reddit RSS Collector: Error normalizing entry: {e}"
                        )
                        continue
            
            except Exception as e:
                logger.error(
                    f"Reddit RSS Collector: Error collecting for keyword '{keyword}': {e}",
                    exc_info=True,
                )
                continue
        
        logger.info(
            f"Reddit RSS Collector: Completed - {total_found} entries found, "
            f"{relevance_filtered} filtered (empty/missing), "
            f"{total_validated} passed validation, {len(all_items)} unique items collected"
        )
        
        return all_items
    
    def _normalize_entry(
        self,
        entry: Dict[str, Any],
        topic: str,
        seen_urls: set,
    ) -> Optional[Dict[str, Any]]:
        """
        Normalize an RSS entry to our schema.
        
        Args:
            entry: Raw RSS entry from feedparser
            topic: Topic label
            seen_urls: Set of URLs already seen (for per-run dedupe)
            
        Returns:
            Normalized item dictionary or None if invalid
        """
        try:
            # Extract basic fields
            link = entry.get("link", "")
            if not link:
                return None
            
            # Skip if already seen in this run
            if link in seen_urls:
                return None
            
            # Validate URL
            if not is_valid_url(link):
                logger.warning(f"Reddit RSS Collector: Invalid URL: {link}")
                return None
            
            # Extract title
            title = entry.get("title", "").strip()
            
            # Extract and clean summary (removes boilerplate, prefers content over summary)
            summary = _clean_reddit_summary(entry, title)
            
            # Skip if title is empty
            if not title:
                logger.debug("Reddit RSS Collector: Skipping entry with empty title")
                return None
            
            # Parse date
            date_str = entry.get("updated") or entry.get("published") or entry.get("date")
            if not date_str:
                logger.warning(f"Reddit RSS Collector: Entry missing date: {link}")
                return None
            
            parsed_date = _parse_rss_date(date_str)
            if not parsed_date:
                logger.warning(f"Reddit RSS Collector: Failed to parse date: {date_str}")
                return None
            
            # Format date as MM/DD/YYYY
            date_posted = format_date_mmddyyyy(parsed_date)
            
            # Extract author/profile
            author_raw = entry.get("author", "") or entry.get("author_detail", {}).get("name", "")
            profile_link = _extract_profile_link(author_raw)
            profile_name = profile_link if profile_link != "N/A" else "N/A"
            
            # Build normalized item
            item = {
                "date_posted": date_posted,
                "platform": "Reddit-RSS",
                "profile": profile_name,
                "profile_link": profile_link,
                "followers": "N/A",
                "post_link": link,
                "topic": topic,
                "title": title,
                "summary": summary,  # Already cleaned and falls back to title if needed
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
                # Preserve fields for topic filtering
                "description": summary,
                "selftext": summary,
            }
            
            # Apply platform defaults and validate
            item = apply_platform_defaults(item)
            is_valid, error_msg = validate_platform_item(item)
            if not is_valid:
                logger.warning(
                    f"Reddit RSS Collector: Item failed platform validation: {error_msg}"
                )
                return None
            
            return item
        
        except Exception as e:
            logger.error(f"Reddit RSS Collector: Error normalizing entry: {e}", exc_info=True)
            return None


# Backward compatibility: export a function that matches the old interface
def collect_reddit_posts() -> List[Dict[str, Any]]:
    """
    Collect Reddit posts using RSS feeds.
    
    This function maintains backward compatibility with the old snscrape-based interface.
    
    Returns:
        List of normalized Reddit post dictionaries
    """
    collector = RedditCollector()
    return collector.collect()
