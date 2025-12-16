"""
Column alignment repair script for Google Sheets.

Fixes misaligned columns by moving narrative text from metric columns
to the correct Summary column.
"""

import argparse
import csv
import logging
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from integrations.google_sheets import get_worksheet_by_name, get_all_rows, update_row

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Column indices (0-indexed) - based on 17-column schema
SUMMARY_COLUMN = 6
VIEWS_COLUMN = 9
LIKES_COLUMN = 10
COMMENTS_COLUMN = 11
SHARES_COLUMN = 12
ENG_TOTAL_COLUMN = 13


def repair_row(row: list, row_number: int) -> tuple[list, bool]:
    """
    Repair a single row by fixing column alignment.

    Args:
        row: List of row values
        row_number: Row number (for logging)

    Returns:
        Tuple of (repaired_row, was_changed)
    """
    if len(row) < 17:
        # Pad to 17 columns if needed
        row = row + [""] * (17 - len(row))

    repaired = row.copy()
    changed = False

    # If Views contains narrative text, move it to Summary
    if len(repaired) > VIEWS_COLUMN:
        views_value = str(repaired[VIEWS_COLUMN]).strip()
        if views_value and len(views_value) > 20 and views_value.count(" ") > 3:
            # This looks like narrative text
            current_summary = str(repaired[SUMMARY_COLUMN]).strip() if len(repaired) > SUMMARY_COLUMN else ""

            # Move Views text to Summary (append if Summary already has content)
            if current_summary:
                repaired[SUMMARY_COLUMN] = f"{current_summary} {views_value}"
            else:
                repaired[SUMMARY_COLUMN] = views_value

            # Clear Views and shift subsequent columns
            repaired[VIEWS_COLUMN] = "0"
            changed = True
            logger.info(f"Row {row_number}: Moved narrative text from Views to Summary")

    # Ensure metric columns are "0" or numeric, not narrative text
    for col_idx, col_name in [
        (LIKES_COLUMN, "Likes"),
        (COMMENTS_COLUMN, "Comments"),
        (SHARES_COLUMN, "Shares"),
        (ENG_TOTAL_COLUMN, "Eng. Total"),
    ]:
        if len(repaired) > col_idx:
            value = str(repaired[col_idx]).strip()
            if value and len(value) > 20:  # Likely narrative text
                repaired[col_idx] = "0"
                changed = True
                logger.info(f"Row {row_number}: Fixed {col_name} column (was narrative text)")

    return repaired, changed


def repair_alignment(worksheet, report_csv: Path, dry_run: bool = False) -> int:
    """
    Repair column alignment based on analysis report.

    Args:
        worksheet: The gspread Worksheet object
        report_csv: Path to alignment analysis CSV report
        dry_run: If True, only identify fixes without applying them

    Returns:
        Number of rows repaired
    """
    if not report_csv.exists():
        logger.error(f"Report file not found: {report_csv}")
        return 0

    # Load misaligned rows from report
    misaligned_row_numbers = set()
    with open(report_csv, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            misaligned_row_numbers.add(int(row["row_number"]))

    logger.info(f"Found {len(misaligned_row_numbers)} rows to repair from report")

    # Read all rows
    all_rows = get_all_rows(worksheet, include_header=False)

    repaired_count = 0

    for idx, row in enumerate(all_rows):
        row_number = idx + 2  # +1 for header, +1 for 1-indexing

        if row_number not in misaligned_row_numbers:
            continue

        repaired_row, changed = repair_row(row, row_number)

        if changed:
            if dry_run:
                logger.info(f"DRY RUN: Would repair row {row_number}")
            else:
                update_row(worksheet, row_number, repaired_row)
                logger.info(f"Repaired row {row_number}")
            repaired_count += 1

    return repaired_count


def main():
    parser = argparse.ArgumentParser(
        description="Repair column alignment in Google Sheet"
    )
    parser.add_argument(
        "--worksheet",
        type=str,
        default="SM Listening (Dec 2025)",
        help="Name of the worksheet to repair (default: 'SM Listening (Dec 2025)')",
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=Path(__file__).parent.parent / "audit-report" / "column_alignment_issues.csv",
        help="Path to alignment analysis CSV report",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Identify repairs without applying them",
    )

    args = parser.parse_args()

    try:
        logger.info(f"Opening worksheet: {args.worksheet}")
        worksheet = get_worksheet_by_name(args.worksheet)

        count = repair_alignment(worksheet, args.report, dry_run=args.dry_run)

        if args.dry_run:
            logger.info(f"DRY RUN: Would repair {count} rows")
        else:
            logger.info(f"Successfully repaired {count} rows")

        return 0

    except Exception as e:
        logger.error(f"Error repairing alignment: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())

