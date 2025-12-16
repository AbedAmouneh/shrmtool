"""
Fix existing metric columns by replacing "N/A" with "0".

Bulk find/replace operation for metric columns (Views, Likes, Comments, Shares, Eng. Total).
"""

import argparse
import logging
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from integrations.google_sheets import get_worksheet_by_name, get_all_rows, batch_update_rows

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Column indices (0-indexed) - metric columns
METRIC_COLUMNS = {
    9: "Views",
    10: "Likes",
    11: "Comments",
    12: "Shares",
    13: "Eng. Total",
}


def fix_metrics_in_row(row: list, row_number: int) -> tuple[list, bool]:
    """
    Fix metric columns in a single row by replacing "N/A" with "0".

    Args:
        row: List of row values
        row_number: Row number (for logging)

    Returns:
        Tuple of (fixed_row, was_changed)
    """
    if len(row) < 17:
        # Pad to 17 columns if needed
        row = row + [""] * (17 - len(row))

    fixed = row.copy()
    changed = False

    for col_idx, col_name in METRIC_COLUMNS.items():
        if len(fixed) > col_idx:
            value = str(fixed[col_idx]).strip().upper()
            if value in ("N/A", "NONE", "NULL", ""):
                fixed[col_idx] = "0"
                changed = True
                logger.debug(f"Row {row_number}: Fixed {col_name} (was {value})")

    return fixed, changed


def fix_all_metrics(worksheet, dry_run: bool = False) -> int:
    """
    Fix all metric columns in the worksheet using batch updates.

    Args:
        worksheet: The gspread Worksheet object
        dry_run: If True, only identify fixes without applying them

    Returns:
        Number of rows fixed
    """
    logger.info("Reading all rows from worksheet...")
    all_rows = get_all_rows(worksheet, include_header=False)

    updates = []  # List of (row_number, fixed_row) tuples

    for idx, row in enumerate(all_rows):
        row_number = idx + 2  # +1 for header, +1 for 1-indexing

        fixed_row, changed = fix_metrics_in_row(row, row_number)

        if changed:
            if dry_run:
                logger.info(f"DRY RUN: Would fix row {row_number}")
            else:
                updates.append((row_number, fixed_row))
            # Count for logging
            if not dry_run:
                logger.debug(f"Prepared fix for row {row_number}")

    if not dry_run and updates:
        logger.info(f"Batch updating {len(updates)} rows...")
        batch_update_rows(worksheet, updates)
        logger.info(f"Successfully batch updated {len(updates)} rows")

    return len(updates)


def main():
    parser = argparse.ArgumentParser(
        description="Fix metric columns by replacing N/A with 0"
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

        count = fix_all_metrics(worksheet, dry_run=args.dry_run)

        if args.dry_run:
            logger.info(f"DRY RUN: Would fix {count} rows")
        else:
            logger.info(f"Successfully fixed {count} rows")

        return 0

    except Exception as e:
        logger.error(f"Error fixing metrics: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())

