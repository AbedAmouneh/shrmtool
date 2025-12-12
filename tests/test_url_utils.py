from utils.url_utils import canonical_url, is_valid_url


def test_canonical_url_strips_tracking_and_fragments():
    raw = "https://example.com/article?utm_source=twitter&fbclid=abc#section"
    expected = "https://example.com/article"
    assert canonical_url(raw) == expected


def test_canonical_url_normalizes_trailing_slash():
    raw = "http://example.com/path/"
    expected = "https://example.com/path"
    assert canonical_url(raw) == expected


def test_canonical_url_equivalence_with_tracking():
    base = "https://example.com/post"
    a = f"{base}?utm_medium=social"
    b = f"{base}?fbclid=123"
    assert canonical_url(a) == canonical_url(b) == base


def test_is_valid_url_accepts_http_https():
    assert is_valid_url("https://example.com/path")
    assert is_valid_url("http://example.com/path")


def test_is_valid_url_rejects_invalid():
    assert not is_valid_url("example.com")
    assert not is_valid_url("ftp://example.com/file")
    assert not is_valid_url("")
    assert not is_valid_url(None)


def test_canonical_url_strips_all_query_params_for_news_domains():
    """Test that News domains strip ALL query parameters, not just tracking params."""
    # News domains should strip all query parameters
    url1 = "http://news.com/a?ref=1"
    url2 = "http://news.com/a?ref=2"
    url3 = "http://news.com/a?r=1234&virt=abc"
    
    canonical1 = canonical_url(url1)
    canonical2 = canonical_url(url2)
    canonical3 = canonical_url(url3)
    
    # All should normalize to the same string (no query params)
    expected = "https://news.com/a"
    assert canonical1 == expected
    assert canonical2 == expected
    assert canonical3 == expected
    assert canonical1 == canonical2 == canonical3


def test_canonical_url_preserves_query_params_for_social_media():
    """Test that social media domains preserve non-tracking query parameters."""
    # YouTube should preserve query parameters (except tracking ones)
    youtube_url = "https://youtube.com/watch?v=abc123&utm_source=twitter"
    canonical = canonical_url(youtube_url)
    # Should preserve v=abc123 but strip utm_source
    assert "v=abc123" in canonical
    assert "utm_source" not in canonical

