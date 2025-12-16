"""
Spam domain purge script for Google Sheets cleanup.

Removes rows containing spam/blocked domains (biztoc.com, Google redirects, etc.)
from the "SM Listening (Dec 2025)" worksheet.
"""

import argparse
import logging
import sys
from urllib.parse import urlparse
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from integrations.google_sheets import get_worksheet_by_name, delete_rows, get_all_rows

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Spam domains to block
SPAM_DOMAINS = {
    "biztoc.com",
    "msn.com",
    "yahoo.com",
    "aol.com",
}

# Google redirect patterns
GOOGLE_REDIRECT_PATTERNS = [
    "google.com/url",
    "google.com/search",
]


def is_spam_url(url: str) -> bool:
    """
    Check if a URL is from a spam domain or Google redirect.

    Args:
        url: URL string to check

    Returns:
        True if URL is spam, False otherwise
    """
    if not url:
        return False

    try:
        parsed = urlparse(url)
        domain = parsed.netloc.lower().replace("www.", "")

        # Check against spam domains
        if domain in SPAM_DOMAINS:
            return True

        # Check for Google redirects
        path = parsed.path.lower()
        for pattern in GOOGLE_REDIRECT_PATTERNS:
            if pattern in path:
                return True

        return False
    except Exception:
        return False


def find_spam_rows(worksheet, dry_run: bool = False) -> list[int]:
    """
    Find all rows containing spam domains.

    Args:
        worksheet: The gspread Worksheet object
        dry_run: If True, only identify rows without deleting

    Returns:
        List of row numbers (1-indexed) containing spam
    """
    logger.info("Reading all rows from worksheet...")
    all_rows = get_all_rows(worksheet, include_header=False)

    # Column indices (0-indexed): Post Link is column 5 (index 4)
    POST_LINK_COLUMN = 4

    spam_rows = []
    for idx, row in enumerate(all_rows):
        # Row number in sheet is idx + 2 (idx is 0-indexed, +1 for header, +1 for 1-indexing)
        row_number = idx + 2

        if len(row) <= POST_LINK_COLUMN:
            continue

        post_link = row[POST_LINK_COLUMN]
        if is_spam_url(post_link):
            spam_rows.append(row_number)
            logger.info(f"Row {row_number}: Spam URL detected - {post_link}")

    logger.info(f"Found {len(spam_rows)} spam rows")
    return spam_rows


def main():
    parser = argparse.ArgumentParser(
        description="Purge spam domains from Google Sheet"
    )
    parser.add_argument(
        "--worksheet",
        type=str,
        default="SM Listening (Dec 2025)",
        help="Name of the worksheet to clean (default: 'SM Listening (Dec 2025)')",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Identify spam rows without deleting them",
    )

    args = parser.parse_args()

    try:
        logger.info(f"Opening worksheet: {args.worksheet}")
        worksheet = get_worksheet_by_name(args.worksheet)

        logger.info("Scanning for spam domains...")
        spam_rows = find_spam_rows(worksheet, dry_run=args.dry_run)

        if not spam_rows:
            logger.info("No spam rows found. Sheet is clean!")
            return 0

        if args.dry_run:
            logger.info(f"DRY RUN: Would delete {len(spam_rows)} rows: {spam_rows}")
            return 0

        logger.info(f"Deleting {len(spam_rows)} spam rows...")
        delete_rows(worksheet, spam_rows)
        logger.info(f"Successfully deleted {len(spam_rows)} spam rows")

        return 0

    except Exception as e:
        logger.error(f"Error purging spam domains: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())

