"""
Tests for integrations.dedupe_store module.
"""

import pytest
from unittest.mock import patch, MagicMock
from pathlib import Path
import integrations.dedupe_store as dedupe_module


class TestDedupeStore:
    """Tests for dedupe_store functions."""
    
    def test_has_seen_returns_false_for_new_url(self, tmp_path, monkeypatch):
        """Test that has_seen returns False for URLs not yet seen."""
        db_path = tmp_path / "test_seen_urls.db"
        
        # Mock DB_PATH to use temporary database
        monkeypatch.setattr(dedupe_module, "DB_PATH", db_path)
        
        # Reload module to pick up new DB_PATH
        import importlib
        importlib.reload(dedupe_module)
        
        assert dedupe_module.has_seen("https://example.com/post1") is False
    
    def test_has_seen_returns_true_after_mark_seen(self, tmp_path, monkeypatch):
        """Test that has_seen returns True after marking URL as seen."""
        db_path = tmp_path / "test_seen_urls.db"
        
        monkeypatch.setattr(dedupe_module, "DB_PATH", db_path)
        import importlib
        importlib.reload(dedupe_module)
        
        url = "https://example.com/post1"
        
        # Initially not seen
        assert dedupe_module.has_seen(url) is False
        
        # Mark as seen
        dedupe_module.mark_seen([url])
        
        # Now should be seen
        assert dedupe_module.has_seen(url) is True
    
    def test_mark_seen_single_url(self, tmp_path, monkeypatch):
        """Test marking a single URL as seen."""
        db_path = tmp_path / "test_seen_urls.db"
        
        monkeypatch.setattr(dedupe_module, "DB_PATH", db_path)
        import importlib
        importlib.reload(dedupe_module)
        
        url = "https://example.com/post1"
        dedupe_module.mark_seen([url])
        
        assert dedupe_module.has_seen(url) is True
        assert dedupe_module.get_seen_count() == 1
    
    def test_mark_seen_multiple_urls(self, tmp_path, monkeypatch):
        """Test marking multiple URLs as seen."""
        db_path = tmp_path / "test_seen_urls.db"
        
        monkeypatch.setattr(dedupe_module, "DB_PATH", db_path)
        import importlib
        importlib.reload(dedupe_module)
        
        urls = [
            "https://example.com/post1",
            "https://example.com/post2",
            "https://example.com/post3",
        ]
        
        dedupe_module.mark_seen(urls)
        
        # All should be seen
        for url in urls:
            assert dedupe_module.has_seen(url) is True
        
        assert dedupe_module.get_seen_count() == 3
    
    def test_mark_seen_duplicate_urls(self, tmp_path, monkeypatch):
        """Test that marking the same URL twice doesn't crash."""
        db_path = tmp_path / "test_seen_urls.db"
        
        monkeypatch.setattr(dedupe_module, "DB_PATH", db_path)
        import importlib
        importlib.reload(dedupe_module)
        
        url = "https://example.com/post1"
        
        # Mark first time
        dedupe_module.mark_seen([url])
        assert dedupe_module.get_seen_count() == 1
        
        # Mark again (should not crash, uses INSERT OR IGNORE)
        dedupe_module.mark_seen([url])
        
        # Should still be only one entry
        assert dedupe_module.get_seen_count() == 1
        assert dedupe_module.has_seen(url) is True
    
    def test_mark_seen_empty_list(self, tmp_path, monkeypatch):
        """Test that marking empty list doesn't crash."""
        db_path = tmp_path / "test_seen_urls.db"
        
        monkeypatch.setattr(dedupe_module, "DB_PATH", db_path)
        import importlib
        importlib.reload(dedupe_module)
        
        # Should not raise an error
        dedupe_module.mark_seen([])
        
        assert dedupe_module.get_seen_count() == 0
    
    def test_get_seen_count(self, tmp_path, monkeypatch):
        """Test that get_seen_count returns correct count."""
        db_path = tmp_path / "test_seen_urls.db"
        
        monkeypatch.setattr(dedupe_module, "DB_PATH", db_path)
        import importlib
        importlib.reload(dedupe_module)
        
        # Initially zero
        assert dedupe_module.get_seen_count() == 0
        
        # Add one
        dedupe_module.mark_seen(["https://example.com/post1"])
        assert dedupe_module.get_seen_count() == 1
        
        # Add more
        dedupe_module.mark_seen([
            "https://example.com/post2",
            "https://example.com/post3",
        ])
        assert dedupe_module.get_seen_count() == 3
    
    def test_persistence_across_calls(self, tmp_path, monkeypatch):
        """Test that URLs persist across multiple function calls."""
        db_path = tmp_path / "test_seen_urls.db"
        
        monkeypatch.setattr(dedupe_module, "DB_PATH", db_path)
        import importlib
        importlib.reload(dedupe_module)
        
        url = "https://example.com/post1"
        
        # Mark as seen
        dedupe_module.mark_seen([url])
        
        # Check in separate call
        assert dedupe_module.has_seen(url) is True
        
        # Check count in separate call
        assert dedupe_module.get_seen_count() == 1
        
        # Add another URL
        dedupe_module.mark_seen(["https://example.com/post2"])
        
        # Both should be seen
        assert dedupe_module.has_seen(url) is True
        assert dedupe_module.has_seen("https://example.com/post2") is True
        assert dedupe_module.get_seen_count() == 2
    
    def test_different_urls_not_seen(self, tmp_path, monkeypatch):
        """Test that different URLs are tracked separately."""
        db_path = tmp_path / "test_seen_urls.db"
        
        monkeypatch.setattr(dedupe_module, "DB_PATH", db_path)
        import importlib
        importlib.reload(dedupe_module)
        
        url1 = "https://example.com/post1"
        url2 = "https://example.com/post2"
        
        # Mark only first URL
        dedupe_module.mark_seen([url1])
        
        # First should be seen, second should not
        assert dedupe_module.has_seen(url1) is True
        assert dedupe_module.has_seen(url2) is False
