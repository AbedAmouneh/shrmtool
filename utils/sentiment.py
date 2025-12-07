"""
Simple keyword-based sentiment classification.

Classifies text as "Negative" or "Neutral" based on keyword matching.
"""

from typing import Optional

# Negative keywords that indicate negative sentiment about SHRM or the case
NEGATIVE_KEYWORDS = [
    "discrimination",
    "lawsuit",
    "racist",
    "racism",
    "toxic",
    "verdict",
    "guilty",
    "hostile",
    "bias",
    "biased",
    "unfair",
    "unlawful",
    "illegal",
    "violation",
    "violated",
    "sued",
    "suing",
    "settlement",
    "damages",
    "plaintiff",
    "defendant",
    "court",
    "judge",
    "jury",
    "trial",
    "convicted",
    "condemned",
    "criticized",
    "criticism",
    "scandal",
    "controversy",
    "outrage",
    "protest",
    "boycott",
]


def classify_sentiment(text: Optional[str]) -> str:
    """
    Classify sentiment as "Negative" or "Neutral" based on keyword matching.
    
    Args:
        text: Text to analyze (title, description, or post body)
        
    Returns:
        "Negative" if negative keywords found, "Neutral" otherwise
    """
    if not text:
        return "Neutral"
    
    text_lower = text.lower()
    
    # Check if any negative keywords appear in the text
    for keyword in NEGATIVE_KEYWORDS:
        if keyword in text_lower:
            return "Negative"
    
    return "Neutral"


def classify_sentiment_combined(title: Optional[str], body: Optional[str] = None) -> str:
    """
    Classify sentiment from combined title and body text.
    
    Args:
        title: Title text
        body: Optional body/description text
        
    Returns:
        "Negative" if negative keywords found, "Neutral" otherwise
    """
    combined = ""
    if title:
        combined += title + " "
    if body:
        combined += body
    
    return classify_sentiment(combined.strip() if combined else None)
