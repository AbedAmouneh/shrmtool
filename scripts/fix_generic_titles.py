"""
Fix generic titles in Google Sheets.

Replaces generic topic titles (e.g., "SHRM Trial Verdict – Public & HR Community Reaction")
with actual article/post headlines extracted from Summary or Post Link.
"""

import argparse
import logging
import re
import sys
from pathlib import Path
from urllib.parse import urlparse

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from integrations.google_sheets import get_worksheet_by_name, get_all_rows, update_row

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Column indices (0-indexed)
PLATFORM_COLUMN = 1
POST_LINK_COLUMN = 4
TOPIC_TITLE_COLUMN = 5
SUMMARY_COLUMN = 6

GENERIC_TITLE = "SHRM Trial Verdict – Public & HR Community Reaction"


def extract_title_from_summary(summary: str) -> str:
    """
    Try to extract a title from the summary text.

    Args:
        summary: Summary text

    Returns:
        Extracted title or empty string
    """
    if not summary:
        return ""

    # Look for patterns like "Title: ..." or first sentence
    summary = summary.strip()

    # If summary starts with a quote or is very short, use it as title
    if len(summary) < 100 and not summary.count(".") > 2:
        return summary

    # Try to find the first sentence
    sentences = re.split(r"[.!?]\s+", summary)
    if sentences:
        first_sentence = sentences[0].strip()
        if 20 <= len(first_sentence) <= 150:
            return first_sentence

    return ""


def extract_title_from_url(url: str) -> str:
    """
    Try to extract a title from the URL path.

    Args:
        url: Post/article URL

    Returns:
        Extracted title or empty string
    """
    if not url:
        return ""

    try:
        parsed = urlparse(url)
        path = parsed.path.strip("/")

        # For LinkedIn: activity URLs don't have titles in path
        if "linkedin.com" in parsed.netloc:
            return ""

        # For news sites: sometimes title is in the path
        # e.g., /article-title-here-2025-12
        if path:
            # Take the last segment and clean it up
            segments = path.split("/")
            if segments:
                last_segment = segments[-1]
                # Remove date suffixes and clean up
                title = re.sub(r"-\d{4}-\d{2}$", "", last_segment)
                title = title.replace("-", " ").title()
                if 10 <= len(title) <= 100:
                    return title

    except Exception:
        pass

    return ""


def fix_title_in_row(row: list, row_number: int) -> tuple[list, bool]:
    """
    Fix generic title in a single row.

    Args:
        row: List of row values
        row_number: Row number (for logging)

    Returns:
        Tuple of (fixed_row, was_changed)
    """
    if len(row) < 17:
        row = row + [""] * (17 - len(row))

    fixed = row.copy()
    changed = False

    current_title = str(fixed[TOPIC_TITLE_COLUMN]).strip() if len(fixed) > TOPIC_TITLE_COLUMN else ""

    # Only fix if it's the generic title
    if current_title != GENERIC_TITLE:
        return fixed, False

    # Try to extract title from summary first
    summary = str(fixed[SUMMARY_COLUMN]).strip() if len(fixed) > SUMMARY_COLUMN else ""
    new_title = extract_title_from_summary(summary)

    # If that didn't work, try URL
    if not new_title:
        post_link = str(fixed[POST_LINK_COLUMN]).strip() if len(fixed) > POST_LINK_COLUMN else ""
        new_title = extract_title_from_url(post_link)

    # If we found a new title, use it
    if new_title and new_title != GENERIC_TITLE:
        fixed[TOPIC_TITLE_COLUMN] = new_title
        changed = True
        logger.info(f"Row {row_number}: Fixed title from '{GENERIC_TITLE[:30]}...' to '{new_title[:50]}...'")
    else:
        logger.warning(f"Row {row_number}: Could not extract title from summary or URL")

    return fixed, changed


def fix_all_titles(worksheet, dry_run: bool = False) -> int:
    """
    Fix all generic titles in the worksheet.

    Args:
        worksheet: The gspread Worksheet object
        dry_run: If True, only identify fixes without applying them

    Returns:
        Number of rows fixed
    """
    logger.info("Reading all rows from worksheet...")
    all_rows = get_all_rows(worksheet, include_header=False)

    fixed_count = 0

    for idx, row in enumerate(all_rows):
        row_number = idx + 2  # +1 for header, +1 for 1-indexing

        fixed_row, changed = fix_title_in_row(row, row_number)

        if changed:
            if dry_run:
                logger.info(f"DRY RUN: Would fix row {row_number}")
            else:
                update_row(worksheet, row_number, fixed_row)
            fixed_count += 1

    return fixed_count


def main():
    parser = argparse.ArgumentParser(
        description="Fix generic titles in Google Sheet"
    )
    parser.add_argument(
        "--worksheet",
        type=str,
        default="SM Listening (Dec 2025)",
        help="Name of the worksheet to fix (default: 'SM Listening (Dec 2025)')",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Identify fixes without applying them",
    )

    args = parser.parse_args()

    try:
        logger.info(f"Opening worksheet: {args.worksheet}")
        worksheet = get_worksheet_by_name(args.worksheet)

        count = fix_all_titles(worksheet, dry_run=args.dry_run)

        if args.dry_run:
            logger.info(f"DRY RUN: Would fix {count} rows")
        else:
            logger.info(f"Successfully fixed {count} rows")

        return 0

    except Exception as e:
        logger.error(f"Error fixing titles: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())

