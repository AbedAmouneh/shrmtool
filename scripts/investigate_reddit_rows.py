"""
Investigate Reddit rows in Google Sheets.

Scans for reddit.com URLs that might be mislabeled as "News" or other platforms.
"""

import argparse
import csv
import logging
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


def is_reddit_url(url: str) -> bool:
    """
    Check if a URL is a Reddit URL.

    Args:
        url: URL string to check

    Returns:
        True if URL is from Reddit
    """
    if not url:
        return False

    try:
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        return "reddit.com" in domain
    except Exception:
        return False


def investigate_reddit_rows(worksheet, output_csv: Path, fix: bool = False) -> int:
    """
    Investigate Reddit rows and generate a report.

    Args:
        worksheet: The gspread Worksheet object
        output_csv: Path to output CSV report
        fix: If True, fix mislabeled Reddit rows

    Returns:
        Number of Reddit rows found
    """
    logger.info("Reading all rows from worksheet...")
    all_rows = get_all_rows(worksheet, include_header=False)

    reddit_rows = []

    for idx, row in enumerate(all_rows):
        row_number = idx + 2  # +1 for header, +1 for 1-indexing

        if len(row) <= POST_LINK_COLUMN:
            continue

        post_link = row[POST_LINK_COLUMN]
        platform = row[PLATFORM_COLUMN] if len(row) > PLATFORM_COLUMN else ""

        if is_reddit_url(post_link):
            is_mislabeled = platform != "Reddit"
            reddit_rows.append(
                {
                    "row_number": row_number,
                    "post_link": post_link,
                    "current_platform": platform,
                    "is_mislabeled": is_mislabeled,
                }
            )

            if is_mislabeled:
                logger.info(f"Row {row_number}: Reddit URL mislabeled as '{platform}'")

            if fix and is_mislabeled:
                # Fix the platform label
                fixed_row = row.copy()
                if len(fixed_row) < 17:
                    fixed_row = fixed_row + [""] * (17 - len(fixed_row))
                fixed_row[PLATFORM_COLUMN] = "Reddit"
                update_row(worksheet, row_number, fixed_row)
                logger.info(f"Row {row_number}: Fixed platform to 'Reddit'")

    # Write report
    if reddit_rows:
        logger.info(f"Writing report to {output_csv}")
        with open(output_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f, fieldnames=["row_number", "post_link", "current_platform", "is_mislabeled"]
            )
            writer.writeheader()
            writer.writerows(reddit_rows)
        logger.info(f"Found {len(reddit_rows)} Reddit rows ({sum(1 for r in reddit_rows if r['is_mislabeled'])} mislabeled)")
    else:
        logger.info("No Reddit rows found in sheet")

    return len(reddit_rows)


def main():
    parser = argparse.ArgumentParser(
        description="Investigate Reddit rows in Google Sheet"
    )
    parser.add_argument(
        "--worksheet",
        type=str,
        default="SM Listening (Dec 2025)",
        help="Name of the worksheet to investigate (default: 'SM Listening (Dec 2025)')",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(__file__).parent.parent / "audit-report" / "reddit_rows_report.csv",
        help="Path to output CSV report",
    )
    parser.add_argument(
        "--fix",
        action="store_true",
        help="Fix mislabeled Reddit rows (change platform to 'Reddit')",
    )

    args = parser.parse_args()

    try:
        logger.info(f"Opening worksheet: {args.worksheet}")
        worksheet = get_worksheet_by_name(args.worksheet)

        count = investigate_reddit_rows(worksheet, args.output, fix=args.fix)

        logger.info(f"Investigation complete. Found {count} Reddit rows.")

        return 0

    except Exception as e:
        logger.error(f"Error investigating Reddit rows: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())

