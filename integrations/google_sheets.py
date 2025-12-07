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
