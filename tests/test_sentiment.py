"""
Tests for utils.sentiment module.
"""

import pytest
from utils import sentiment


class TestClassifySentiment:
    """Tests for classify_sentiment function."""
    
    def test_classify_sentiment_negative_discrimination(self):
        """Test that 'discrimination' keyword returns Negative."""
        result = sentiment.classify_sentiment("This is about discrimination")
        assert result == "Negative"
    
    def test_classify_sentiment_negative_lawsuit(self):
        """Test that 'lawsuit' keyword returns Negative."""
        result = sentiment.classify_sentiment("There's a lawsuit against SHRM")
        assert result == "Negative"
    
    def test_classify_sentiment_negative_racist(self):
        """Test that 'racist' keyword returns Negative."""
        result = sentiment.classify_sentiment("Racist behavior was found")
        assert result == "Negative"
    
    def test_classify_sentiment_negative_verdict(self):
        """Test that 'verdict' keyword returns Negative."""
        result = sentiment.classify_sentiment("The verdict was announced")
        assert result == "Negative"
    
    def test_classify_sentiment_negative_guilty(self):
        """Test that 'guilty' keyword returns Negative."""
        result = sentiment.classify_sentiment("They were found guilty")
        assert result == "Negative"
    
    def test_classify_sentiment_negative_multiple_keywords(self):
        """Test that any negative keyword triggers Negative."""
        result = sentiment.classify_sentiment("The trial resulted in a verdict of guilty")
        assert result == "Negative"
    
    def test_classify_sentiment_neutral_text(self):
        """Test that neutral text returns Neutral."""
        result = sentiment.classify_sentiment("SHRM is a professional organization")
        assert result == "Neutral"
    
    def test_classify_sentiment_neutral_no_keywords(self):
        """Test that text without negative keywords returns Neutral."""
        result = sentiment.classify_sentiment("Human resources management is important")
        assert result == "Neutral"
    
    def test_classify_sentiment_case_insensitive(self):
        """Test that keyword matching is case-insensitive."""
        result1 = sentiment.classify_sentiment("DISCRIMINATION case")
        result2 = sentiment.classify_sentiment("Discrimination Case")
        result3 = sentiment.classify_sentiment("discrimination case")
        
        assert result1 == "Negative"
        assert result2 == "Negative"
        assert result3 == "Negative"
    
    def test_classify_sentiment_none_input(self):
        """Test that None input returns Neutral."""
        result = sentiment.classify_sentiment(None)
        assert result == "Neutral"
    
    def test_classify_sentiment_empty_string(self):
        """Test that empty string returns Neutral."""
        result = sentiment.classify_sentiment("")
        assert result == "Neutral"
    
    def test_classify_sentiment_keyword_in_word(self):
        """Test that keywords are matched as substrings."""
        # "discrimination" in "nondiscrimination" should match
        result = sentiment.classify_sentiment("nondiscrimination policy")
        assert result == "Negative"
    
    def test_classify_sentiment_all_negative_keywords(self):
        """Test that all negative keywords are detected."""
        keywords = [
            "discrimination", "lawsuit", "racist", "racism", "toxic",
            "verdict", "guilty", "hostile", "bias", "biased",
            "unfair", "unlawful", "illegal", "violation", "violated",
            "sued", "suing", "settlement", "damages", "plaintiff",
            "defendant", "court", "judge", "jury", "trial",
            "convicted", "condemned", "criticized", "criticism",
            "scandal", "controversy", "outrage", "protest", "boycott"
        ]
        
        for keyword in keywords:
            result = sentiment.classify_sentiment(f"This contains {keyword}")
            assert result == "Negative", f"Keyword '{keyword}' should return Negative"


class TestClassifySentimentCombined:
    """Tests for classify_sentiment_combined function."""
    
    def test_classify_sentiment_combined_title_only_negative(self):
        """Test combined sentiment with negative title only."""
        result = sentiment.classify_sentiment_combined("SHRM discrimination case", None)
        assert result == "Negative"
    
    def test_classify_sentiment_combined_title_only_neutral(self):
        """Test combined sentiment with neutral title only."""
        result = sentiment.classify_sentiment_combined("SHRM organization", None)
        assert result == "Neutral"
    
    def test_classify_sentiment_combined_title_and_body_negative(self):
        """Test combined sentiment with negative title and body."""
        result = sentiment.classify_sentiment_combined(
            "SHRM case",
            "The lawsuit resulted in a verdict"
        )
        assert result == "Negative"
    
    def test_classify_sentiment_combined_title_neutral_body_negative(self):
        """Test combined sentiment with neutral title but negative body."""
        result = sentiment.classify_sentiment_combined(
            "SHRM news",
            "The court found them guilty"
        )
        assert result == "Negative"
    
    def test_classify_sentiment_combined_title_negative_body_neutral(self):
        """Test combined sentiment with negative title but neutral body."""
        result = sentiment.classify_sentiment_combined(
            "SHRM discrimination",
            "This is about professional development"
        )
        assert result == "Negative"
    
    def test_classify_sentiment_combined_both_neutral(self):
        """Test combined sentiment with both title and body neutral."""
        result = sentiment.classify_sentiment_combined(
            "SHRM organization",
            "Professional human resources management"
        )
        assert result == "Neutral"
    
    def test_classify_sentiment_combined_none_title(self):
        """Test combined sentiment with None title."""
        result = sentiment.classify_sentiment_combined(None, "This is about discrimination")
        assert result == "Negative"
    
    def test_classify_sentiment_combined_none_body(self):
        """Test combined sentiment with None body."""
        result = sentiment.classify_sentiment_combined("SHRM discrimination", None)
        assert result == "Negative"
    
    def test_classify_sentiment_combined_both_none(self):
        """Test combined sentiment with both None."""
        result = sentiment.classify_sentiment_combined(None, None)
        assert result == "Neutral"
    
    def test_classify_sentiment_combined_empty_strings(self):
        """Test combined sentiment with empty strings."""
        result = sentiment.classify_sentiment_combined("", "")
        assert result == "Neutral"
