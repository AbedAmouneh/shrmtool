"""
Configuration loader for environment variables.

Loads .env file and exposes configuration values for the application.
"""

from pathlib import Path
from dotenv import load_dotenv
import os

load_dotenv()

NEWS_API_KEY = os.getenv("NEWS_API_KEY")
SHEET_ID = os.getenv("SHEET_ID")
VERDICT_DATE = os.getenv("VERDICT_DATE")  # "2025-12-05" etc.

# Path to your service account file
SERVICE_ACCOUNT_PATH = Path("service_account.json")

if not NEWS_API_KEY:
    raise ValueError("NEWS_API_KEY is not set in .env")
if not SHEET_ID:
    raise ValueError("SHEET_ID is not set in .env")
if not VERDICT_DATE:
    raise ValueError("VERDICT_DATE is not set in .env")
