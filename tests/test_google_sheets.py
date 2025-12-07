"""
Tests for integrations.google_sheets module.
"""

import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock
import gspread
from integrations import google_sheets
from tests.conftest import DummyResponse


class TestGetSheetsClient:
    """Tests for get_sheets_client function."""
    
    def test_get_sheets_client_raises_if_service_account_missing(self, mock_config, monkeypatch):
        """Test that FileNotFoundError is raised if service account file doesn't exist."""
        # Mock SERVICE_ACCOUNT_PATH to not exist
        mock_path = MagicMock(spec=Path)
        mock_path.exists.return_value = False
        monkeypatch.setattr(google_sheets, "SERVICE_ACCOUNT_PATH", mock_path)
        
        with pytest.raises(FileNotFoundError, match="Service account file not found"):
            google_sheets.get_sheets_client()
    
    @patch('integrations.google_sheets.gspread.authorize')
    @patch('integrations.google_sheets.Credentials.from_service_account_file')
    def test_get_sheets_client_success(self, mock_creds, mock_authorize, mock_config, monkeypatch):
        """Test successful client creation."""
        # Mock service account path exists
        mock_path = MagicMock(spec=Path)
        mock_path.exists.return_value = True
        monkeypatch.setattr(google_sheets, "SERVICE_ACCOUNT_PATH", mock_path)
        
        # Mock credentials and client
        mock_cred_obj = MagicMock()
        mock_creds.return_value = mock_cred_obj
        
        mock_client = MagicMock()
        mock_authorize.return_value = mock_client
        
        result = google_sheets.get_sheets_client()
        
        assert result == mock_client
        mock_creds.assert_called_once()
        mock_authorize.assert_called_once_with(mock_cred_obj)


class TestAppendRows:
    """Tests for append_rows function."""
    
    def test_append_rows_empty_list_returns_early(self, mock_config):
        """Test that append_rows with empty list returns early without API calls."""
        with patch('integrations.google_sheets.get_sheets_client') as mock_get_client:
            google_sheets.append_rows([])
            
            # Should not call get_sheets_client
            mock_get_client.assert_not_called()
    
    @patch('integrations.google_sheets.get_sheets_client')
    def test_append_rows_calls_worksheet_append_rows(self, mock_get_client, mock_config):
        """Test that append_rows calls worksheet.append_rows with correct data."""
        # Create mock objects
        mock_client = MagicMock()
        mock_sheet = MagicMock()
        mock_worksheet = MagicMock()
        
        mock_get_client.return_value = mock_client
        mock_client.open_by_key.return_value = mock_sheet
        mock_sheet.get_worksheet.return_value = mock_worksheet
        
        # Test data
        rows = [
            ["12/06/2025", "Reddit", "u/testuser", "N/A", "https://reddit.com/post1"],
            ["12/06/2025", "Media", "Bloomberg", "N/A", "https://bloomberg.com/article1"],
        ]
        
        google_sheets.append_rows(rows)
        
        # Verify calls
        mock_get_client.assert_called_once()
        mock_client.open_by_key.assert_called_once_with(google_sheets.SHEET_ID)
        mock_sheet.get_worksheet.assert_called_once_with(0)
        mock_worksheet.append_rows.assert_called_once_with(rows)
    
    @patch('integrations.google_sheets.get_sheets_client')
    def test_append_rows_handles_permission_denied(self, mock_get_client, mock_config):
        """Test that PERMISSION_DENIED error is handled with helpful message."""
        # Create mock objects
        mock_client = MagicMock()
        mock_sheet = MagicMock()
        mock_worksheet = MagicMock()
        
        mock_get_client.return_value = mock_client
        mock_client.open_by_key.return_value = mock_sheet
        mock_sheet.get_worksheet.return_value = mock_worksheet
        
        # Mock APIError with PERMISSION_DENIED
        original_error = gspread.exceptions.APIError(
            DummyResponse("PERMISSION_DENIED: Access denied")
        )
        mock_worksheet.append_rows.side_effect = original_error
        
        rows = [["12/06/2025", "Reddit", "u/testuser", "N/A", "https://reddit.com/post1"]]
        
        with pytest.raises(gspread.exceptions.APIError) as exc_info:
            google_sheets.append_rows(rows)
        
        # Check error message contains helpful info
        assert "Permission denied" in str(exc_info.value)
        assert "Editor access" in str(exc_info.value)
    
    @patch('integrations.google_sheets.get_sheets_client')
    def test_append_rows_handles_permission_denied_403(self, mock_get_client, mock_config):
        """Test that 403 error is handled as permission denied."""
        mock_client = MagicMock()
        mock_sheet = MagicMock()
        mock_worksheet = MagicMock()
        
        mock_get_client.return_value = mock_client
        mock_client.open_by_key.return_value = mock_sheet
        mock_sheet.get_worksheet.return_value = mock_worksheet
        
        # Mock APIError with 403
        original_error = gspread.exceptions.APIError(
            DummyResponse("403 Forbidden")
        )
        mock_worksheet.append_rows.side_effect = original_error
        
        rows = [["12/06/2025", "Reddit", "u/testuser", "N/A", "https://reddit.com/post1"]]
        
        with pytest.raises(gspread.exceptions.APIError) as exc_info:
            google_sheets.append_rows(rows)
        
        assert "Permission denied" in str(exc_info.value)
    
    @patch('integrations.google_sheets.get_sheets_client')
    def test_append_rows_handles_not_found(self, mock_get_client, mock_config):
        """Test that NOT_FOUND error is handled with helpful message."""
        mock_client = MagicMock()
        mock_sheet = MagicMock()
        mock_worksheet = MagicMock()
        
        mock_get_client.return_value = mock_client
        mock_client.open_by_key.return_value = mock_sheet
        mock_sheet.get_worksheet.return_value = mock_worksheet
        
        # Mock APIError with NOT_FOUND
        original_error = gspread.exceptions.APIError(
            DummyResponse("NOT_FOUND: Sheet not found")
        )
        mock_worksheet.append_rows.side_effect = original_error
        
        rows = [["12/06/2025", "Reddit", "u/testuser", "N/A", "https://reddit.com/post1"]]
        
        with pytest.raises(gspread.exceptions.APIError) as exc_info:
            google_sheets.append_rows(rows)
        
        # Check error message contains helpful info
        assert "Sheet not found" in str(exc_info.value)
        assert "SHEET_ID" in str(exc_info.value)
    
    @patch('integrations.google_sheets.get_sheets_client')
    def test_append_rows_handles_not_found_404(self, mock_get_client, mock_config):
        """Test that 404 error is handled as not found."""
        mock_client = MagicMock()
        mock_sheet = MagicMock()
        mock_worksheet = MagicMock()
        
        mock_get_client.return_value = mock_client
        mock_client.open_by_key.return_value = mock_sheet
        mock_sheet.get_worksheet.return_value = mock_worksheet
        
        # Mock APIError with 404
        original_error = gspread.exceptions.APIError(
            DummyResponse("404 Not Found")
        )
        mock_worksheet.append_rows.side_effect = original_error
        
        rows = [["12/06/2025", "Reddit", "u/testuser", "N/A", "https://reddit.com/post1"]]
        
        with pytest.raises(gspread.exceptions.APIError) as exc_info:
            google_sheets.append_rows(rows)
        
        assert "Sheet not found" in str(exc_info.value)
    
    @patch('integrations.google_sheets.get_sheets_client')
    def test_append_rows_handles_other_api_errors(self, mock_get_client, mock_config):
        """Test that other API errors are re-raised."""
        mock_client = MagicMock()
        mock_sheet = MagicMock()
        mock_worksheet = MagicMock()
        
        mock_get_client.return_value = mock_client
        mock_client.open_by_key.return_value = mock_sheet
        mock_sheet.get_worksheet.return_value = mock_worksheet
        
        # Mock APIError with different error
        original_error = gspread.exceptions.APIError(
            DummyResponse("RATE_LIMIT_EXCEEDED")
        )
        mock_worksheet.append_rows.side_effect = original_error
        
        rows = [["12/06/2025", "Reddit", "u/testuser", "N/A", "https://reddit.com/post1"]]
        
        with pytest.raises(gspread.exceptions.APIError) as exc_info:
            google_sheets.append_rows(rows)
        
        # Should re-raise the original error
        assert exc_info.value == original_error
    
    @patch('integrations.google_sheets.get_sheets_client')
    def test_append_rows_handles_non_api_errors(self, mock_get_client, mock_config):
        """Test that non-API errors are wrapped in RuntimeError."""
        mock_client = MagicMock()
        mock_sheet = MagicMock()
        mock_worksheet = MagicMock()
        
        mock_get_client.return_value = mock_client
        mock_client.open_by_key.return_value = mock_sheet
        mock_sheet.get_worksheet.return_value = mock_worksheet
        
        # Mock non-API error
        original_error = ValueError("Unexpected error")
        mock_worksheet.append_rows.side_effect = original_error
        
        rows = [["12/06/2025", "Reddit", "u/testuser", "N/A", "https://reddit.com/post1"]]
        
        with pytest.raises(RuntimeError) as exc_info:
            google_sheets.append_rows(rows)
        
        # Should wrap in RuntimeError
        assert "Failed to append rows to Google Sheet" in str(exc_info.value)
        assert isinstance(exc_info.value.__cause__, ValueError)
    
    @patch('integrations.google_sheets.get_sheets_client')
    def test_append_rows_with_large_batch(self, mock_get_client, mock_config):
        """Test appending a large batch of rows."""
        mock_client = MagicMock()
        mock_sheet = MagicMock()
        mock_worksheet = MagicMock()
        
        mock_get_client.return_value = mock_client
        mock_client.open_by_key.return_value = mock_sheet
        mock_sheet.get_worksheet.return_value = mock_worksheet
        
        # Create 100 rows
        rows = [
            ["12/06/2025", "Reddit", f"u/user{i}", "N/A", f"https://reddit.com/post{i}"]
            for i in range(100)
        ]
        
        google_sheets.append_rows(rows)
        
        # Should call append_rows once with all rows
        mock_worksheet.append_rows.assert_called_once_with(rows)
