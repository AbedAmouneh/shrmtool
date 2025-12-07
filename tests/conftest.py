"""
Shared pytest fixtures and configuration for tests.
"""

import pytest
import os
from pathlib import Path
from unittest.mock import patch, MagicMock


class DummyResponse:
    """
    Minimal fake Response object for gspread.exceptions.APIError.

    Used in tests to simulate gspread API error responses.
    - .text: message string
    - .json(): raises so APIError falls back to using .text
    """
    def __init__(self, text: str):
        self.text = text

    def json(self):
        # Force APIError to go into the except path
        raise ValueError("no json")


@pytest.fixture
def mock_env_vars(monkeypatch):
    """Fixture to set mock environment variables for testing."""
    env_vars = {
        "NEWS_API_KEY": "test_news_api_key",
        "SHEET_ID": "test_sheet_id_12345",
        "VERDICT_DATE": "2025-12-05",
    }
    
    for key, value in env_vars.items():
        monkeypatch.setenv(key, value)
    
    return env_vars


@pytest.fixture
def mock_config(mock_env_vars, monkeypatch):
    """Fixture to mock config module values."""
    # Reload config module to pick up new env vars
    import importlib
    import utils.config
    
    # Force reload to get new env vars
    importlib.reload(utils.config)
    
    # Also reload time_utils to pick up new VERDICT_DATE from config
    # This is needed because time_utils imports VERDICT_DATE at module level
    try:
        import utils.time_utils
        importlib.reload(utils.time_utils)
    except ImportError:
        pass  # time_utils might not be imported yet
    
    # Mock service account path to avoid file existence checks
    mock_path = MagicMock(spec=Path)
    mock_path.exists.return_value = True
    monkeypatch.setattr(utils.config, "SERVICE_ACCOUNT_PATH", mock_path)
    
    return {
        "NEWS_API_KEY": mock_env_vars["NEWS_API_KEY"],
        "SHEET_ID": mock_env_vars["SHEET_ID"],
        "VERDICT_DATE": mock_env_vars["VERDICT_DATE"],
        "SERVICE_ACCOUNT_PATH": mock_path,
    }


@pytest.fixture
def tmp_db_path(tmp_path):
    """Fixture providing a temporary database path for dedupe_store tests."""
    return tmp_path / "test_seen_urls.db"


@pytest.fixture
def mock_service_account_path(tmp_path):
    """Fixture providing a mock service account file path."""
    service_account_file = tmp_path / "service_account.json"
    service_account_file.write_text('{"type": "service_account"}')
    return service_account_file

