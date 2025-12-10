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

