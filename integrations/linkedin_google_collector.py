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
            keywords = ["SHRM verdict", "Johnny C. Taylor", "SHRM discrimination"]
        
        logger.info("LinkedIn Google Collector: Starting collection")
        logger.info(f"LinkedIn Google Collector: Using {len(keywords)} search keywords")
        
        all_items = []
        seen_urls = set()  # Per-run deduplication
        total_found = 0
        total_validated = 0
        
        # Get today's date in Eastern timezone, formatted as MM/DD/YYYY
        today_dt = datetime.now(EASTERN)
        date_posted = format_date_mmddyyyy(today_dt)
        
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
            f"{total_validated} passed validation, {len(all_items)} unique items collected"
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

