"""
News article collector using NewsAPI.org.

This module fetches SHRM-related news articles from NewsAPI's /everything endpoint.
"""

import requests
from typing import List, Dict, Any, Optional
import logging
from utils.config import NEWS_API_KEY, VERDICT_DATE

logger = logging.getLogger(__name__)

# NewsAPI base URL
NEWSAPI_BASE_URL = "https://newsapi.org/v2"

# Search terms for news
NEWS_SEARCH_TERMS = [
    "SHRM discrimination",
    "SHRM trial",
    "SHRM verdict",
    "Society for Human Resource Management verdict",
]


def fetch_newsapi_page(
    query: str, page: int = 1, page_size: int = 100
) -> Dict[str, Any]:
    """
    Fetch a single page of results from NewsAPI /everything endpoint.

    Args:
        query: Search query string
        page: Page number (default: 1)
        page_size: Results per page (max 100, default: 100)

    Returns:
        API response dictionary with 'articles' and 'totalResults' keys

    Raises:
        requests.RequestException: If API request fails
        ValueError: If API returns an error
    """
    url = f"{NEWSAPI_BASE_URL}/everything"

    params = {
        "q": query,
        "from": VERDICT_DATE,  # ISO date string
        "language": "en",
        "sortBy": "publishedAt",
        "page": page,
        "pageSize": min(page_size, 100),  # NewsAPI max is 100
        "apiKey": NEWS_API_KEY,
    }

    try:
        logger.info(f"Fetching NewsAPI page {page} for query: {query}")
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()

        data = response.json()

        # Check for API errors
        if data.get("status") == "error":
            error_msg = data.get("message", "Unknown error")
            raise ValueError(f"NewsAPI error: {error_msg}")

        return data

    except requests.exceptions.RequestException as e:
        logger.error(f"NewsAPI request failed for query '{query}': {e}")
        raise
    except ValueError as e:
        raise
    except Exception as e:
        logger.error(f"Unexpected error fetching NewsAPI for query '{query}': {e}")
        raise


def fetch_all_newsapi_results(
    query: str, max_results: int = 100
) -> List[Dict[str, Any]]:
    """
    Fetch all results from NewsAPI, paginating as needed.

    Args:
        query: Search query string
        max_results: Maximum number of results to fetch (default: 100)

    Returns:
        List of article dictionaries from NewsAPI
    """
    all_articles = []
    page = 1
    page_size = 100

    try:
        # Fetch first page
        data = fetch_newsapi_page(query, page=page, page_size=page_size)
        articles = data.get("articles", [])
        total_results = data.get("totalResults", 0)

        logger.info(f"Found {total_results} total results for query: {query}")

        all_articles.extend(articles)

        # Paginate if needed
        while (
            len(all_articles) < min(max_results, total_results)
            and len(articles) == page_size
        ):
            page += 1
            try:
                data = fetch_newsapi_page(query, page=page, page_size=page_size)
                articles = data.get("articles", [])
                all_articles.extend(articles)

                if not articles:  # No more results
                    break

            except Exception as e:
                logger.warning(f"Error fetching page {page} for query '{query}': {e}")
                break

        # Limit to max_results
        return all_articles[:max_results]

    except Exception as e:
        logger.error(f"Error fetching NewsAPI results for query '{query}': {e}")
        return []


def normalize_news_article(article_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize a news article from NewsAPI to a standard format.

    Args:
        article_data: Raw article data from NewsAPI

    Returns:
        Normalized dictionary with fields: source_name, title, description,
        url, publishedAt
    """
    # Extract source name
    source = article_data.get("source", {})
    if isinstance(source, dict):
        source_name = source.get("name", "")
    else:
        source_name = str(source) if source else ""

    title = article_data.get("title", "")
    description = (
        article_data.get("description", "") or article_data.get("content", "") or ""
    )
    url = article_data.get("url", "")
    published_at = article_data.get("publishedAt", "")

    return {
        "source_name": source_name,
        "title": title,
        "description": description,
        "url": url,
        "publishedAt": published_at,
    }


def collect_news_articles() -> List[Dict[str, Any]]:
    """
    Collect news articles for all search terms.

    Returns:
        List of normalized news article dictionaries
    """
    logger.info("News Collector: Starting collection")
    logger.info(f"News Collector: Using verdict date filter: {VERDICT_DATE}")
    all_articles = []
    seen_urls = set()  # Deduplicate within this collection run
    query_count = 0
    error_count = 0
    total_raw = 0
    skipped_malformed = 0

    for query in NEWS_SEARCH_TERMS:
        query_count += 1
        logger.info(
            f"News Collector: Processing query {query_count}/{len(NEWS_SEARCH_TERMS)}: '{query}'"
        )
        try:
            articles = fetch_all_newsapi_results(query, max_results=100)
            raw_count = len(articles)
            total_raw += raw_count
            logger.info(
                f"News Collector: Query '{query}' returned {raw_count} raw articles"
            )

            normalized_count = 0
            skipped_count = 0

            for article_data in articles:
                try:
                    normalized = normalize_news_article(article_data)

                    # Skip if URL is missing or already seen
                    if not normalized.get("url"):
                        skipped_count += 1
                        skipped_malformed += 1
                        logger.warning(f"News Collector: Article missing URL, skipping")
                        continue

                    if normalized["url"] in seen_urls:
                        skipped_count += 1
                        continue

                    seen_urls.add(normalized["url"])
                    all_articles.append(normalized)
                    normalized_count += 1
                except Exception as e:
                    skipped_malformed += 1
                    logger.warning(f"News Collector: Error normalizing article: {e}")
                    continue

            if skipped_count > 0:
                logger.info(
                    f"News Collector: Query '{query}': {normalized_count} normalized, {skipped_count} skipped (duplicates/malformed)"
                )
            else:
                logger.info(
                    f"News Collector: Query '{query}': {normalized_count} normalized"
                )

        except Exception as e:
            error_count += 1
            logger.error(
                f"News Collector: Error collecting articles for query '{query}': {e}",
                exc_info=True,
            )
            continue

    logger.info(
        f"News Collector: Completed - {len(all_articles)} unique articles collected from {query_count} queries, {total_raw} total raw articles, {skipped_malformed} malformed/skipped, {error_count} errors"
    )
    return all_articles
