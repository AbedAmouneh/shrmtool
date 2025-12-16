"""
Google Sheets integration using gspread.

This module handles appending rows to a Google Sheet using service account authentication.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, List

import gspread
from google.oauth2.service_account import Credentials

from utils.config import SHEET_ID, SERVICE_ACCOUNT_PATH


# Google Sheets API scope
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


class _SimpleResponse:
    """
    Minimal Response-like object for gspread.exceptions.APIError.

    - .text: message string
    - .json(): raises so APIError falls back to using .text
    """

    def __init__(self, text: str):
        self.text = text

    def json(self):
        raise ValueError("no json")


def get_sheets_client() -> gspread.Client:
    """
    Create and return an authorized gspread client.

    Checks for SERVICE_ACCOUNT_JSON environment variable first,
    then falls back to service_account.json file.

    Raises:
        FileNotFoundError: if neither env var nor file exists
        ValueError: if SERVICE_ACCOUNT_JSON env var contains invalid JSON
    """
    # Check for environment variable first (for GitHub Actions, etc.)
    service_account_json = os.getenv("SERVICE_ACCOUNT_JSON")
    if service_account_json:
        try:
            service_account_info = json.loads(service_account_json)
            creds = Credentials.from_service_account_info(
                service_account_info, scopes=SCOPES
            )
            client = gspread.authorize(creds)
            return client
        except json.JSONDecodeError as e:
            raise ValueError(
                f"Invalid JSON in SERVICE_ACCOUNT_JSON environment variable: {e}"
            )

    # Fall back to file-based approach
    if not SERVICE_ACCOUNT_PATH.exists():
        raise FileNotFoundError(
            f"Service account file not found at: {SERVICE_ACCOUNT_PATH} "
            f"and SERVICE_ACCOUNT_JSON environment variable not set"
        )

    creds = Credentials.from_service_account_file(
        str(SERVICE_ACCOUNT_PATH), scopes=SCOPES
    )
    client = gspread.authorize(creds)
    return client


def append_rows(rows: List[List[Any]]) -> None:
    """
    Append a batch of rows to the Google Sheet.

    Appends to the first worksheet in the sheet specified by SHEET_ID.
    Assumes the first row contains headers.

    Args:
        rows: List of row lists, where each row is a list of values

    Raises:
        gspread.exceptions.APIError: If sheet access fails or sheet not found
        RuntimeError: For non-API errors
    """
    if not rows:
        return

    try:
        client = get_sheets_client()
        sheet = client.open_by_key(SHEET_ID)
        worksheet = sheet.get_worksheet(0)  # First worksheet

        # Append all rows in batch
        worksheet.append_rows(rows)

    except gspread.exceptions.APIError as e:
        msg = str(e)

        # Permission / auth problems (including 403)
        if "PERMISSION_DENIED" in msg or "403" in msg:
            raise gspread.exceptions.APIError(
                _SimpleResponse(
                    f"Permission denied. Ensure the service account email has "
                    f"Editor access to the sheet. Error: {e}"
                )
            )

        # Sheet not found / wrong SHEET_ID (including 404)
        if "NOT_FOUND" in msg or "404" in msg:
            raise gspread.exceptions.APIError(
                _SimpleResponse(
                    f"Sheet not found. Check that SHEET_ID is correct. Error: {e}"
                )
            )

        # Anything else: re-raise the original APIError
        raise

    except Exception as e:
        # Non-API errors: wrap in RuntimeError so callers get a clear, consistent error
        raise RuntimeError(f"Failed to append rows to Google Sheet: {e}") from e


def get_sheet() -> gspread.Spreadsheet:
    """
    Get the Google Sheet by ID.

    Returns:
        The gspread Spreadsheet object

    Raises:
        gspread.exceptions.APIError: If sheet access fails or sheet not found
        RuntimeError: For non-API errors
    """
    try:
        client = get_sheets_client()
        return client.open_by_key(SHEET_ID)
    except gspread.exceptions.APIError as e:
        msg = str(e)
        if "PERMISSION_DENIED" in msg or "403" in msg:
            raise gspread.exceptions.APIError(
                _SimpleResponse(
                    f"Permission denied. Ensure the service account email has "
                    f"Editor access to the sheet. Error: {e}"
                )
            )
        if "NOT_FOUND" in msg or "404" in msg:
            raise gspread.exceptions.APIError(
                _SimpleResponse(
                    f"Sheet not found. Check that SHEET_ID is correct. Error: {e}"
                )
            )
        raise
    except Exception as e:
        raise RuntimeError(f"Failed to get Google Sheet: {e}") from e


def get_worksheet_by_name(sheet_name: str) -> gspread.Worksheet:
    """
    Get a worksheet by name from the Google Sheet.

    Args:
        sheet_name: Name of the worksheet to retrieve

    Returns:
        The gspread Worksheet object

    Raises:
        gspread.exceptions.WorksheetNotFound: If worksheet doesn't exist
        RuntimeError: For other errors
    """
    try:
        sheet = get_sheet()
        return sheet.worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        raise
    except Exception as e:
        raise RuntimeError(f"Failed to get worksheet '{sheet_name}': {e}") from e


def get_all_rows(worksheet: gspread.Worksheet, include_header: bool = False) -> List[List[Any]]:
    """
    Get all rows from a worksheet.

    Args:
        worksheet: The gspread Worksheet object
        include_header: If True, includes the first row (header). Default: False

    Returns:
        List of all rows (each row is a list of values)

    Raises:
        RuntimeError: For errors reading the worksheet
    """
    try:
        all_values = worksheet.get_all_values()
        if not include_header and all_values:
            return all_values[1:]  # Skip header row
        return all_values
    except Exception as e:
        raise RuntimeError(f"Failed to get rows from worksheet: {e}") from e


def delete_rows(worksheet: gspread.Worksheet, row_numbers: List[int]) -> None:
    """
    Delete specific rows from a worksheet by row number.

    Note: Row numbers are 1-indexed (first data row is 2, since row 1 is header).
    This function deletes rows in reverse order to avoid index shifting issues.
    Uses batch deletion with rate limiting to avoid API quota issues.
    Filters out invalid row numbers that don't exist in the sheet.

    Args:
        worksheet: The gspread Worksheet object
        row_numbers: List of row numbers to delete (1-indexed, where 1 is header)

    Raises:
        RuntimeError: For errors deleting rows
    """
    if not row_numbers:
        return

    try:
        import time
        
        # Get current row count to filter out invalid row numbers
        row_count = worksheet.row_count
        
        # Filter out rows that don't exist (row numbers > row_count)
        valid_rows = [r for r in row_numbers if 1 <= r <= row_count]
        
        if not valid_rows:
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"All {len(row_numbers)} row numbers are invalid (sheet has {row_count} rows)")
            return
        
        if len(valid_rows) < len(row_numbers):
            import logging
            logger = logging.getLogger(__name__)
            logger.info(f"Filtered out {len(row_numbers) - len(valid_rows)} invalid row numbers (sheet has {row_count} rows)")
        
        # Sort in descending order to delete from bottom to top (avoids index shifting)
        sorted_rows = sorted(set(valid_rows), reverse=True)

        # Batch deletions to avoid rate limits (delete 20 at a time with 3 second delay)
        batch_size = 20
        for i in range(0, len(sorted_rows), batch_size):
            batch = sorted_rows[i : i + batch_size]
            
            # Delete batch (gspread's delete_rows can handle multiple rows if they're consecutive)
            # For non-consecutive rows, we delete one at a time but in batches
            for row_num in batch:
                worksheet.delete_rows(row_num)
            
            # Add delay between batches to avoid rate limits (except for last batch)
            if i + batch_size < len(sorted_rows):
                time.sleep(3)  # 3 second delay between batches

    except Exception as e:
        raise RuntimeError(f"Failed to delete rows {row_numbers}: {e}") from e


def update_row(worksheet: gspread.Worksheet, row_number: int, values: List[Any]) -> None:
    """
    Update a single row in the worksheet.

    Args:
        worksheet: The gspread Worksheet object
        row_number: Row number to update (1-indexed, where 1 is header)
        values: List of values to write to the row

    Raises:
        RuntimeError: For errors updating the row
    """
    try:
        worksheet.update(f"A{row_number}", [values])
    except Exception as e:
        raise RuntimeError(f"Failed to update row {row_number}: {e}") from e


def batch_update_rows(
    worksheet: gspread.Worksheet, updates: List[tuple[int, List[Any]]]
) -> None:
    """
    Batch update multiple rows in the worksheet.

    Args:
        worksheet: The gspread Worksheet object
        updates: List of (row_number, values) tuples to update

    Raises:
        RuntimeError: For errors updating rows
    """
    if not updates:
        return

    try:
        # Prepare batch update data
        data = []
        for row_number, values in updates:
            # Convert row number to A1 notation range
            range_name = f"A{row_number}:Q{row_number}"  # Q is column 17
            data.append({"range": range_name, "values": [values]})

        # Perform batch update (max 100 updates per batch)
        batch_size = 100
        for i in range(0, len(data), batch_size):
            batch = data[i : i + batch_size]
            worksheet.batch_update(batch)

    except Exception as e:
        raise RuntimeError(f"Failed to batch update rows: {e}") from e
