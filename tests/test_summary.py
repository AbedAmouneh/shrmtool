"""
Tests for utils.summary module.
"""

import pytest
from utils import summary


class TestTruncateText:
    """Tests for truncate_text function."""
    
    def test_truncate_text_shorter_than_max(self):
        """Test that text shorter than max_length is returned unchanged."""
        text = "This is a short text"
        result = summary.truncate_text(text, max_length=100)
        
        assert result == text
        assert len(result) == len(text)
    
    def test_truncate_text_exactly_max_length(self):
        """Test that text exactly at max_length is returned unchanged."""
        text = "a" * 300
        result = summary.truncate_text(text, max_length=300)
        
        assert result == text
        assert len(result) == 300
    
    def test_truncate_text_longer_than_max(self):
        """Test that text longer than max_length is truncated."""
        text = "This is a very long text that exceeds the maximum length limit"
        result = summary.truncate_text(text, max_length=20)
        
        # Should truncate at word boundary, no ellipsis in new implementation
        assert len(result) <= 20
        assert not result.endswith("...")
    
    def test_truncate_text_truncates_at_word_boundary(self):
        """Test that truncation happens at word boundary when possible."""
        text = "This is a sentence with multiple words that should be truncated properly"
        result = summary.truncate_text(text, max_length=30)
        
        # Should truncate at a space, not in the middle of a word
        assert " " in result
        # Should be close to max_length but not exceed it
        assert len(result) <= 30
    
    def test_truncate_text_no_word_boundary_found(self):
        """Test truncation when no word boundary is found near max_length."""
        # Text with no spaces near the limit
        text = "a" * 100
        result = summary.truncate_text(text, max_length=50)
        
        # Should truncate at max_length, no ellipsis
        assert len(result) == 50
    
    def test_truncate_text_default_max_length(self):
        """Test that default max_length is 300."""
        text = "a" * 400
        result = summary.truncate_text(text)
        
        assert len(result) <= 300
    
    def test_truncate_text_custom_max_length(self):
        """Test truncation with custom max_length."""
        text = "This is a test sentence"
        result = summary.truncate_text(text, max_length=10)
        
        assert len(result) <= 10


class TestBuildSummary:
    """Tests for build_summary function."""
    
    def test_build_summary_title_only(self):
        """Test building summary from title only."""
        result = summary.build_summary("SHRM Trial Verdict", None)
        
        assert result == "SHRM Trial Verdict"
        assert len(result) <= 300
    
    def test_build_summary_title_and_body(self):
        """Test building summary from title and body."""
        title = "SHRM Case"
        body = "This is a detailed description of the SHRM discrimination case."
        result = summary.build_summary(title, body)
        
        assert "SHRM Case" in result
        assert "detailed description" in result
        assert len(result) <= 300
    
    def test_build_summary_body_starts_with_title(self):
        """Test that body starting with title doesn't duplicate title."""
        title = "SHRM Trial"
        body = "SHRM Trial verdict was announced today"
        result = summary.build_summary(title, body)
        
        # Should not have "SHRM Trial" twice
        assert result.count("SHRM Trial") == 1
        assert "verdict was announced" in result
    
    def test_build_summary_body_starts_with_title_case_insensitive(self):
        """Test title deduplication is case-insensitive."""
        title = "SHRM Trial"
        body = "shrm trial verdict was announced"
        result = summary.build_summary(title, body)
        
        # Should deduplicate even with different case
        assert result.count("SHRM Trial") <= 1
        assert "verdict was announced" in result
    
    def test_build_summary_long_text_truncates(self):
        """Test that long text is truncated to max length."""
        title = "SHRM Case"
        body = "This is a very long description. " * 50  # Very long text
        result = summary.build_summary(title, body)
        
        assert len(result) <= 300
    
    def test_build_summary_none_title(self):
        """Test building summary with None title."""
        body = "This is the body text"
        result = summary.build_summary(None, body)
        
        assert result == body
        assert len(result) <= 300
    
    def test_build_summary_none_body(self):
        """Test building summary with None body."""
        title = "SHRM Trial Verdict"
        result = summary.build_summary(title, None)
        
        assert result == title
        assert len(result) <= 300
    
    def test_build_summary_both_none(self):
        """Test building summary with both None."""
        result = summary.build_summary(None, None)
        
        assert result == ""
    
    def test_build_summary_empty_strings(self):
        """Test building summary with empty strings."""
        result = summary.build_summary("", "")
        
        assert result == ""
    
    def test_build_summary_whitespace_handling(self):
        """Test that whitespace is properly handled."""
        title = "  SHRM Case  "
        body = "  This is the body  "
        result = summary.build_summary(title, body)
        
        # Should strip whitespace
        assert not result.startswith(" ")
        assert not result.endswith(" ")
        assert "SHRM Case" in result
        assert "This is the body" in result
    
    def test_build_summary_exact_max_length(self):
        """Test summary that exactly matches max length."""
        # Create text that's exactly 300 chars
        title = "SHRM Case"
        body = "a" * (300 - len(title) - 1)  # -1 for space
        result = summary.build_summary(title, body)
        
        assert len(result) == 300
    
    def test_build_summary_very_long_title(self):
        """Test summary with very long title."""
        title = "This is a very long title that might exceed the maximum length limit by itself"
        body = "Additional body text"
        result = summary.build_summary(title, body)
        
        assert len(result) <= 300
    
    def test_build_summary_multiple_spaces(self):
        """Test that multiple spaces are normalized to single spaces."""
        title = "SHRM  Case"
        body = "This  has  multiple  spaces"
        result = summary.build_summary(title, body)
        
        # Should normalize multiple spaces to single spaces
        assert "  " not in result
