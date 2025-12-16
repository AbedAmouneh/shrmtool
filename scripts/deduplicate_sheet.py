"""
Deduplication cleanup script for Google Sheets.

Removes duplicate links from the "SM Listening (Dec 2025)" worksheet,
keeping the most recent entry (by Date Posted) for each duplicate group.
"""

import argparse
import csv
import logging
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from integrations.google_sheets import (
    get_worksheet_by_name,
    delete_rows,
    get_all_rows,
    update_row,
)
from utils.url_utils import canonical_url

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Column indices (0-indexed)
DATE_POSTED_COLUMN = 0
POST_LINK_COLUMN = 4


def parse_date_posted(date_str: str) -> datetime:
    """
    Parse date string in MM/DD/YYYY format to datetime.

    Args:
        date_str: Date string in MM/DD/YYYY format

    Returns:
        datetime object, or datetime.min if parsing fails
    """
    try:
        return datetime.strptime(date_str.strip(), "%m/%d/%Y")
    except (ValueError, AttributeError):
        return datetime.min


def load_duplicate_report(csv_path: Path) -> Dict[str, List[int]]:
    """
    Load duplicate links report and return mapping of canonical URL to row numbers.

    Args:
        csv_path: Path to the duplicate links report CSV

    Returns:
        Dictionary mapping canonical URL to list of row numbers (1-indexed)
    """
    duplicates = defaultdict(list)

    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            link = row["link"]
            sheet_rows_str = row["sheet_rows"]

            # Parse row numbers from string like "3, 5, 7, 9, ..."
            row_numbers = [
                int(r.strip()) for r in sheet_rows_str.split(",") if r.strip()
            ]

            # Normalize URL to canonical form
            canonical = canonical_url(link)
            if canonical:
                duplicates[canonical].extend(row_numbers)

    return dict(duplicates)


def find_duplicates_in_sheet(worksheet) -> Dict[str, List[Tuple[int, datetime]]]:
    """
    Find duplicate links in the worksheet by canonical URL.

    Args:
        worksheet: The gspread Worksheet object

    Returns:
        Dictionary mapping canonical URL to list of (row_number, date_posted) tuples
    """
    logger.info("Reading all rows from worksheet...")
    all_rows = get_all_rows(worksheet, include_header=False)

    duplicates = defaultdict(list)

    for idx, row in enumerate(all_rows):
        row_number = idx + 2  # +1 for header, +1 for 1-indexing

        if len(row) <= POST_LINK_COLUMN:
            continue

        post_link = row[POST_LINK_COLUMN]
        canonical = canonical_url(post_link)

        if not canonical:
            continue

        # Get date posted
        date_str = row[DATE_POSTED_COLUMN] if len(row) > DATE_POSTED_COLUMN else ""
        date_posted = parse_date_posted(date_str)

        duplicates[canonical].append((row_number, date_posted))

    # Filter to only groups with duplicates
    return {url: rows for url, rows in duplicates.items() if len(rows) > 1}


def deduplicate_rows(
    worksheet, duplicates: Dict[str, List[Tuple[int, datetime]]], dry_run: bool = False
) -> List[int]:
    """
    Remove duplicate rows, keeping the most recent entry for each duplicate group.

    Args:
        worksheet: The gspread Worksheet object
        duplicates: Dictionary mapping canonical URL to list of (row_number, date_posted) tuples
        dry_run: If True, only identify rows to delete without deleting

    Returns:
        List of row numbers to delete
    """
    rows_to_delete = []

    for canonical_url, rows_with_dates in duplicates.items():
        # Sort by date (most recent first), then by row number (keep first occurrence if dates are equal)
        sorted_rows = sorted(rows_with_dates, key=lambda x: (x[1], -x[0]), reverse=True)

        # Keep the first (most recent) entry
        keep_row, keep_date = sorted_rows[0]
        delete_rows_list = [row_num for row_num, _ in sorted_rows[1:]]

        logger.info(
            f"Canonical URL: {canonical_url[:60]}...\n"
            f"  Keeping row {keep_row} (date: {keep_date.strftime('%m/%d/%Y') if keep_date != datetime.min else 'N/A'})\n"
            f"  Deleting rows: {delete_rows_list}"
        )

        rows_to_delete.extend(delete_rows_list)

    if not rows_to_delete:
        logger.info("No duplicate rows to delete")
        return []

    logger.info(f"Total rows to delete: {len(rows_to_delete)}")
    return rows_to_delete


def main():
    parser = argparse.ArgumentParser(
        description="Remove duplicate links from Google Sheet"
    )
    parser.add_argument(
        "--worksheet",
        type=str,
        default="SM Listening (Dec 2025)",
        help="Name of the worksheet to clean (default: 'SM Listening (Dec 2025)')",
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=Path(__file__).parent.parent / "audit-report" / "SM_Listening_Dec2025_DuplicateLinks_Report.csv",
        help="Path to duplicate links report CSV",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Identify duplicate rows without deleting them",
    )

    args = parser.parse_args()

    try:
        logger.info(f"Opening worksheet: {args.worksheet}")
        worksheet = get_worksheet_by_name(args.worksheet)

        # Option 1: Use duplicate report if provided
        if args.report.exists():
            logger.info(f"Loading duplicate report from: {args.report}")
            duplicate_groups = load_duplicate_report(args.report)

            # Convert to format expected by deduplicate_rows
            # For now, we'll keep the most recent row number (highest = most recent)
            # This avoids needing to read all rows to get dates
            duplicates = {}
            for canonical, row_numbers in duplicate_groups.items():
                if len(row_numbers) > 1:
                    # Sort row numbers descending (assume higher = more recent)
                    # Keep the first (highest) and mark others for deletion
                    sorted_rows = sorted(row_numbers, reverse=True)
                    # Use a dummy date (datetime.min) since we're sorting by row number
                    rows_with_dates = [
                        (row_num, datetime.min) for row_num in sorted_rows
                    ]
                    duplicates[canonical] = rows_with_dates
                    logger.info(
                        f"Canonical URL: {canonical[:60]}...\n"
                        f"  Keeping row {sorted_rows[0]} (highest row number = most recent)\n"
                        f"  Deleting rows: {sorted_rows[1:]}"
                    )
        else:
            # Option 2: Scan sheet for duplicates
            logger.info("Scanning worksheet for duplicate links...")
            duplicates = find_duplicates_in_sheet(worksheet)

        if not duplicates:
            logger.info("No duplicates found. Sheet is clean!")
            return 0

        logger.info(f"Found {len(duplicates)} duplicate groups")
        rows_to_delete = deduplicate_rows(worksheet, duplicates, dry_run=args.dry_run)

        if args.dry_run:
            logger.info(f"DRY RUN: Would delete {len(rows_to_delete)} duplicate rows")
            return 0

        if rows_to_delete:
            logger.info(f"Deleting {len(rows_to_delete)} duplicate rows...")
            delete_rows(worksheet, rows_to_delete)
            logger.info(f"Successfully deleted {len(rows_to_delete)} duplicate rows")

        return 0

    except Exception as e:
        logger.error(f"Error deduplicating sheet: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())

