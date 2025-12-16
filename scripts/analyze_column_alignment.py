"""
Column alignment analyzer for Google Sheets.

Detects rows where columns are misaligned (e.g., narrative text in Views column).
"""

import argparse
import csv
import logging
import re
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from integrations.google_sheets import get_worksheet_by_name, get_all_rows

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Column indices (0-indexed) - based on 17-column schema
VIEWS_COLUMN = 9
LIKES_COLUMN = 10
COMMENTS_COLUMN = 11
SHARES_COLUMN = 12
ENG_TOTAL_COLUMN = 13
SUMMARY_COLUMN = 6


def is_numeric_or_na(value: str) -> bool:
    """
    Check if a value is numeric, "0", "N/A", or empty.

    Args:
        value: String value to check

    Returns:
        True if value is numeric/NA/empty, False if it contains narrative text
    """
    if not value:
        return True

    value = str(value).strip().upper()

    # Allow N/A, empty, or numeric values
    if value in ("N/A", "", "0", "NONE", "NULL"):
        return True

    # Check if it's a number
    try:
        float(value.replace(",", ""))
        return True
    except ValueError:
        pass

    # If it contains multiple words or is longer than 20 chars, likely narrative text
    if len(value) > 20 or len(value.split()) > 3:
        return False

    return False


def is_narrative_text(value: str) -> bool:
    """
    Check if a value looks like narrative text (not a metric).

    Args:
        value: String value to check

    Returns:
        True if value appears to be narrative text
    """
    if not value:
        return False

    value = str(value).strip()

    # If it's very short or matches known patterns, it's probably not narrative
    if len(value) <= 10:
        return False

    # If it contains multiple sentences or is very long, it's narrative
    if len(value) > 50 or value.count(".") > 1:
        return True

    # If it contains common narrative words
    narrative_indicators = [
        "the ",
        "and ",
        "for ",
        "with ",
        "that ",
        "this ",
        "from ",
        "source:",
        "by ",
    ]
    value_lower = value.lower()
    if any(indicator in value_lower for indicator in narrative_indicators):
        return True

    return False


def analyze_alignment(worksheet, output_csv: Path) -> int:
    """
    Analyze column alignment and generate a report.

    Args:
        worksheet: The gspread Worksheet object
        output_csv: Path to output CSV report

    Returns:
        Number of misaligned rows found
    """
    logger.info("Reading all rows from worksheet...")
    all_rows = get_all_rows(worksheet, include_header=False)

    misaligned_rows = []

    for idx, row in enumerate(all_rows):
        row_number = idx + 2  # +1 for header, +1 for 1-indexing

        if len(row) < 17:
            logger.warning(f"Row {row_number}: Has only {len(row)} columns (expected 17)")
            continue

        issues = []

        # Check Views column for narrative text
        views_value = row[VIEWS_COLUMN] if len(row) > VIEWS_COLUMN else ""
        if is_narrative_text(views_value):
            issues.append(f"Views contains narrative text: {views_value[:50]}...")

        # Check Likes column
        likes_value = row[LIKES_COLUMN] if len(row) > LIKES_COLUMN else ""
        if is_narrative_text(likes_value):
            issues.append(f"Likes contains narrative text: {likes_value[:50]}...")

        # Check Comments column
        comments_value = row[COMMENTS_COLUMN] if len(row) > COMMENTS_COLUMN else ""
        if is_narrative_text(comments_value):
            issues.append(f"Comments contains narrative text: {comments_value[:50]}...")

        # Check if metric columns contain non-numeric values (except N/A)
        for col_idx, col_name in [
            (VIEWS_COLUMN, "Views"),
            (LIKES_COLUMN, "Likes"),
            (COMMENTS_COLUMN, "Comments"),
            (SHARES_COLUMN, "Shares"),
            (ENG_TOTAL_COLUMN, "Eng. Total"),
        ]:
            if len(row) > col_idx:
                value = row[col_idx]
                if not is_numeric_or_na(value) and value:
                    issues.append(f"{col_name} contains invalid value: {value[:50]}")

        if issues:
            misaligned_rows.append(
                {
                    "row_number": row_number,
                    "post_link": row[4] if len(row) > 4 else "",
                    "issues": "; ".join(issues),
                    "views_value": views_value[:100] if views_value else "",
                    "summary_value": row[SUMMARY_COLUMN][:100] if len(row) > SUMMARY_COLUMN else "",
                }
            )
            logger.info(f"Row {row_number}: {len(issues)} alignment issue(s)")

    # Write report
    if misaligned_rows:
        logger.info(f"Writing report to {output_csv}")
        with open(output_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f, fieldnames=["row_number", "post_link", "issues", "views_value", "summary_value"]
            )
            writer.writeheader()
            writer.writerows(misaligned_rows)
        logger.info(f"Found {len(misaligned_rows)} misaligned rows")
    else:
        logger.info("No misaligned rows found. Sheet alignment is correct!")

    return len(misaligned_rows)


def main():
    parser = argparse.ArgumentParser(
        description="Analyze column alignment in Google Sheet"
    )
    parser.add_argument(
        "--worksheet",
        type=str,
        default="SM Listening (Dec 2025)",
        help="Name of the worksheet to analyze (default: 'SM Listening (Dec 2025)')",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(__file__).parent.parent / "audit-report" / "column_alignment_issues.csv",
        help="Path to output CSV report",
    )

    args = parser.parse_args()

    try:
        logger.info(f"Opening worksheet: {args.worksheet}")
        worksheet = get_worksheet_by_name(args.worksheet)

        count = analyze_alignment(worksheet, args.output)
        logger.info(f"Analysis complete. Found {count} misaligned rows.")

        return 0

    except Exception as e:
        logger.error(f"Error analyzing alignment: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())

