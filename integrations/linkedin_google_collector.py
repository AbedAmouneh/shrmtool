"""
LinkedIn Google Custom Search collector.

Uses Google Custom Search API to find LinkedIn posts about SHRM verdict.
Normalizes results to the 17-column schema and validates rows.
"""

from __future__ import annotations

import logging
import os
import re
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests
from urllib.parse import urlparse

from utils.schema import build_row, validate_row
from utils.time_utils import format_date_mmddyyyy, EASTERN
from utils.url_utils import is_valid_url
from utils.platform_rules import apply_platform_defaults, validate_platform_item

logger = logging.getLogger(__name__)

GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
GOOGLE_CSE_ID = os.getenv("GOOGLE_CSE_ID")
SEARCH_URL = "https://www.googleapis.com/customsearch/v1"

# Cutoff date: only accept posts from December 2025 onwards (SHRM verdict was Dec 5, 2025)
CUTOFF_DATE = datetime(2025, 12, 1, tzinfo=EASTERN)

# Month name mappings for date parsing
MONTH_NAMES = {
    "jan": 1, "january": 1,
    "feb": 2, "february": 2,
    "mar": 3, "march": 3,
    "apr": 4, "april": 4,
    "may": 5,
    "jun": 6, "june": 6,
    "jul": 7, "july": 7,
    "aug": 8, "august": 8,
    "sep": 9, "sept": 9, "september": 9,
    "oct": 10, "october": 10,
    "nov": 11, "november": 11,
    "dec": 12, "december": 12,
}


def _extract_date_from_text(text: str) -> Optional[datetime]:
    """
    Extract a date from text using common date patterns.
    
    Args:
        text: Text that may contain a date (e.g., snippet or title)
        
    Returns:
        Datetime object or None if no date found
    """
    if not text:
        return None
    
    text_lower = text.lower()
    
    # Pattern 1: "Dec 5, 2025" or "December 5, 2025" or "Dec 5 2025"
    pattern1 = r"\b(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\s+(\d{1,2})(?:,?\s+|\s+)(\d{4})\b"
    match = re.search(pattern1, text_lower)
    if match:
        month_str, day_str, year_str = match.groups()
        month = MONTH_NAMES.get(month_str)
        if month:
            try:
                return datetime(int(year_str), month, int(day_str), tzinfo=EASTERN)
            except ValueError:
                pass
    
    # Pattern 2: "5 Dec 2025" or "5 December 2025"
    pattern2 = r"\b(\d{1,2})\s+(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\s+(\d{4})\b"
    match = re.search(pattern2, text_lower)
    if match:
        day_str, month_str, year_str = match.groups()
        month = MONTH_NAMES.get(month_str)
        if month:
            try:
                return datetime(int(year_str), month, int(day_str), tzinfo=EASTERN)
            except ValueError:
                pass
    
    # Pattern 3: "12/05/2025" or "2025-12-05"
    pattern3 = r"\b(\d{1,2})/(\d{1,2})/(\d{4})\b"
    match = re.search(pattern3, text)
    if match:
        try:
            m, d, y = int(match.group(1)), int(match.group(2)), int(match.group(3))
            return datetime(y, m, d, tzinfo=EASTERN)
        except ValueError:
            pass
    
    pattern4 = r"\b(\d{4})-(\d{1,2})-(\d{1,2})\b"
    match = re.search(pattern4, text)
    if match:
        try:
            y, m, d = int(match.group(1)), int(match.group(2)), int(match.group(3))
            return datetime(y, m, d, tzinfo=EASTERN)
        except ValueError:
            pass
    
    return None


def _contains_old_date_marker(text: str) -> Optional[str]:
    """
    Check if text contains markers of old dates (pre-December 2025).
    
    Args:
        text: Text to check
        
    Returns:
        The old date marker found, or None if text is acceptable
    """
    if not text:
        return None
    
    text_lower = text.lower()
    
    # Check for old years (standalone years like "in 2020")
    old_years = ["2020", "2021", "2022", "2023", "2024"]
    for year in old_years:
        # Only match if it's a standalone year, not part of a larger number
        if re.search(rf"\b{year}\b", text_lower):
            return year
    
    # Check for months in 2025 before December
    early_2025_months = [
        "jan 2025", "january 2025",
        "feb 2025", "february 2025",
        "mar 2025", "march 2025",
        "apr 2025", "april 2025",
        "may 2025",
        "jun 2025", "june 2025",
        "jul 2025", "july 2025",
        "aug 2025", "august 2025",
        "sep 2025", "sept 2025", "september 2025",
        "oct 2025", "october 2025",
        "nov 2025", "november 2025",
    ]
    for marker in early_2025_months:
        if marker in text_lower:
            return marker
    
    return None


def _validate_post_date(item_data: Dict[str, Any]) -> tuple[bool, Optional[str], Optional[datetime]]:
    """
    Validate that a post is from after the CUTOFF_DATE.
    
    Args:
        item_data: Raw item from Google Custom Search API
        
    Returns:
        Tuple of (is_valid, reason_if_invalid, extracted_date)
    """
    title = item_data.get("title", "") or ""
    snippet = item_data.get("snippet", "") or ""
    combined_text = f"{title} {snippet}"
    
    # First, check for explicit old date markers
    old_marker = _contains_old_date_marker(combined_text)
    if old_marker:
        return False, f"contains old date marker '{old_marker}'", None
    
    # Try to extract an actual date
    extracted_date = _extract_date_from_text(combined_text)
    
    if extracted_date:
        if extracted_date < CUTOFF_DATE:
            return False, f"extracted date {extracted_date.strftime('%Y-%m-%d')} is before cutoff", extracted_date
        return True, None, extracted_date
    
    # If no date found, allow the post (we'll use today's date)
    return True, None, None


def _extract_linkedin_profile(link: str) -> str:
    """
    Extract LinkedIn profile URL from post URL if possible.
    
    Examples:
    - https://www.linkedin.com/posts/username-activity-123 → https://www.linkedin.com/in/username/
    - https://www.linkedin.com/feed/update/... → "N/A"
    
    Args:
        link: LinkedIn post URL
        
    Returns:
        Profile URL or "N/A"
    """
    if not link or "linkedin.com" not in link.lower():
        return "N/A"
    
    try:
        # Try to extract username from posts URL pattern
        # Pattern: linkedin.com/posts/username-activity-...
        match = re.search(r"linkedin\.com/posts/([^-]+)-", link, re.IGNORECASE)
        if match:
            username = match.group(1)
            return f"https://www.linkedin.com/in/{username}/"
        
        # Pattern: linkedin.com/pulse/...
        # Pattern: linkedin.com/feed/update/...
        # These don't have clear profile extraction, return N/A
        return "N/A"
    except Exception:
        return "N/A"


def _clean_title(title: str) -> str:
    """
    Clean LinkedIn title by removing " | LinkedIn" suffix.
    
    Args:
        title: Raw title from Google search
        
    Returns:
        Cleaned title
    """
    if not title:
        return "N/A"
    
    # Remove " | LinkedIn" suffix (case-insensitive)
    cleaned = re.sub(r"\s*\|\s*LinkedIn\s*$", "", title, flags=re.IGNORECASE)
    return cleaned.strip() or "N/A"


def _is_verdict_relevant(item_data: Dict[str, Any]) -> bool:
    """
    Check if a LinkedIn post is relevant to the SHRM verdict.
    
    Filters out false positives by requiring verdict-specific keywords
    and excluding known irrelevant content.
    
    Args:
        item_data: Raw item from Google Custom Search API
        
    Returns:
        True if relevant to verdict, False otherwise
    """
    # Required keywords: at least ONE must be present
    required_keywords = [
        "verdict",
        "jury",
        "11.5",
        "liable",
        "guilty",
        "trial",
        "damages",
        "11 million",
        "appeal",
    ]
    
    # Excluded keywords: immediate disqualification
    excluded_keywords = [
        "Robby Starbuck",
        "Starbuck",
        "Sep 2025",
        "Oct 2025",
        "Aug 2025",
        "inclusion conference",
    ]
    
    # Combine title and snippet for checking
    title = item_data.get("title", "").lower()
    snippet = item_data.get("snippet", "").lower()
    combined_text = f"{title} {snippet}"
    
    # Check excluded keywords first (immediate disqualification)
    for excluded in excluded_keywords:
        if excluded.lower() in combined_text:
            logger.debug(
                f"LinkedIn Google Collector: Excluded item due to keyword '{excluded}': "
                f"{item_data.get('title', 'N/A')[:100]}"
            )
            return False
    
    # Check required keywords (at least one must be present)
    has_required = any(keyword.lower() in combined_text for keyword in required_keywords)
    
    if not has_required:
        logger.debug(
            f"LinkedIn Google Collector: Excluded item missing verdict keywords: "
            f"{item_data.get('title', 'N/A')[:100]}"
        )
        return False
    
    return True


class LinkedInGoogleCollector:
    """Collector for LinkedIn posts via Google Custom Search API."""
    
    def __init__(self):
        """Initialize collector."""
        pass
    
    def collect(
        self,
        keywords: Optional[List[str]] = None,
        topic: str = "SHRM Trial Verdict – Public & HR Community Reaction",
    ) -> List[Dict[str, Any]]:
        """
        Collect LinkedIn posts for given keywords using Google Custom Search.
        
        Args:
            keywords: List of search keywords. Defaults to ["SHRM verdict", "Johnny C. Taylor", "SHRM discrimination"]
            topic: Topic label for the sheet
            
        Returns:
            List of normalized item dictionaries that pass validation
        """
        # Check API keys dynamically (allows tests to override env vars)
        api_key = os.getenv("GOOGLE_API_KEY")
        cse_id = os.getenv("GOOGLE_CSE_ID")
        
        if not api_key or not cse_id:
            logger.warning(
                "LinkedIn Google Collector: Skipped because GOOGLE_API_KEY or GOOGLE_CSE_ID is not set"
            )
            return []
        
        if keywords is None:
            # Refined search terms focused on verdict-specific content
            keywords = [
                "SHRM verdict",
                "Johnny C. Taylor verdict",
                "SHRM CEO verdict",
                "SHRM discrimination",
            ]
        
        logger.info("LinkedIn Google Collector: Starting collection")
        logger.info(f"LinkedIn Google Collector: Using {len(keywords)} search keywords")
        
        all_items = []
        seen_urls = set()  # Per-run deduplication
        total_found = 0
        total_validated = 0
        relevance_filtered = 0  # Track items filtered by relevance
        date_filtered = 0  # Track items filtered by old date
        
        # Get today's date in Eastern timezone as fallback
        today_dt = datetime.now(EASTERN)
        
        for idx, keyword in enumerate(keywords, start=1):
            logger.info(
                f"LinkedIn Google Collector: Processing query {idx}/{len(keywords)}: '{keyword}'"
            )
            
            try:
                params = {
                    "key": api_key,
                    "cx": cse_id,
                    "q": keyword,
                    "dateRestrict": "w[1]",  # Past week
                    "num": 10,  # Max results per page
                }
                
                response = requests.get(SEARCH_URL, params=params, timeout=30)
                
                if response.status_code != 200:
                    error_text = getattr(response, "text", "")[:200] or str(response.status_code)
                    logger.warning(
                        f"LinkedIn Google Collector: Query '{keyword}' failed with "
                        f"{response.status_code}: {error_text}"
                    )
                    continue
                
                try:
                    data = response.json()
                except Exception:
                    data = {}
                items = data.get("items", []) or []
                
                logger.info(
                    f"LinkedIn Google Collector: Query '{keyword}' returned {len(items)} results"
                )
                
                for item_data in items:
                    total_found += 1
                    
                    # Apply strict relevance filtering before normalization
                    if not _is_verdict_relevant(item_data):
                        relevance_filtered += 1
                        continue
                    
                    # Validate post date (filter out old "zombie" posts)
                    is_date_valid, date_reason, extracted_date = _validate_post_date(item_data)
                    if not is_date_valid:
                        date_filtered += 1
                        logger.info(
                            f"LinkedIn Google Collector: Skipping old post ({date_reason}): "
                            f"{item_data.get('link', 'unknown')[:100]}"
                        )
                        continue
                    
                    # Use extracted date if available, otherwise use today
                    if extracted_date:
                        date_posted = format_date_mmddyyyy(extracted_date)
                    else:
                        date_posted = format_date_mmddyyyy(today_dt)
                    
                    try:
                        normalized = self._normalize_item(
                            item_data, topic, date_posted, seen_urls
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
                                    f"LinkedIn Google Collector: Item failed validation: "
                                    f"{normalized.get('post_link', 'unknown')}"
                                )
                    
                    except Exception as e:
                        logger.warning(
                            f"LinkedIn Google Collector: Error normalizing item: {e}"
                        )
                        continue
            
            except Exception as e:
                logger.error(
                    f"LinkedIn Google Collector: Error collecting for keyword '{keyword}': {e}",
                    exc_info=True,
                )
                continue
        
        logger.info(
            f"LinkedIn Google Collector: Completed - {total_found} items found, "
            f"{relevance_filtered} filtered by relevance, {date_filtered} filtered by old date, "
            f"{total_validated} passed validation, {len(all_items)} unique items collected"
        )
        
        if relevance_filtered > 0:
            logger.info(
                f"LinkedIn Google Collector: Skipped {relevance_filtered} items for missing "
                f"verdict keywords or excluded content"
            )
        
        if date_filtered > 0:
            logger.info(
                f"LinkedIn Google Collector: Skipped {date_filtered} items for being "
                f"old posts (before {CUTOFF_DATE.strftime('%Y-%m-%d')})"
            )
        
        return all_items
    
    def _normalize_item(
        self,
        item_data: Dict[str, Any],
        topic: str,
        date_posted: str,
        seen_urls: set,
    ) -> Optional[Dict[str, Any]]:
        """
        Normalize a Google Custom Search result item to our schema.
        
        Args:
            item_data: Raw item from Google Custom Search API
            topic: Topic label
            date_posted: Date string in MM/DD/YYYY format
            seen_urls: Set of URLs already seen (for per-run dedupe)
            
        Returns:
            Normalized item dictionary or None if invalid
        """
        try:
            link = item_data.get("link", "")
            if not link:
                return None
            
            # Skip if already seen in this run
            if link in seen_urls:
                return None
            
            # Validate URL
            if not is_valid_url(link):
                logger.warning(f"LinkedIn Google Collector: Invalid URL: {link}")
                return None
            
            title = _clean_title(item_data.get("title", ""))
            snippet = item_data.get("snippet", "") or ""
            profile_link = _extract_linkedin_profile(link)
            
            item = {
                "date_posted": date_posted,
                "platform": "LinkedIn-Google",
                "profile": profile_link,  # Use profile_link as profile name
                "profile_link": profile_link,
                "followers": "N/A",
                "post_link": link,
                "topic": topic,
                "title": title,
                "summary": snippet,
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
                "description": snippet,
                "selftext": "",
            }
            
            # Apply platform defaults and validate
            item = apply_platform_defaults(item)
            is_valid, error_msg = validate_platform_item(item)
            if not is_valid:
                logger.warning(
                    f"LinkedIn Google Collector: Item failed platform validation: {error_msg}"
                )
                return None
            
            return item
        
        except Exception as e:
            logger.error(f"LinkedIn Google Collector: Error normalizing item: {e}", exc_info=True)
            return None

