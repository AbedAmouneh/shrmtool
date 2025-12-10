from main_collect import classify_topic


def test_classify_on_topic_shrm():
    item = {"title": "SHRM hit with verdict", "selftext": "", "description": ""}
    assert classify_topic(item) == "on_topic"


def test_classify_on_topic_johnny_with_case():
    item = {
        "title": "Johnny C. Taylor faces harassment lawsuit",
        "selftext": "",
        "description": "",
    }
    assert classify_topic(item) == "on_topic"


def test_classify_borderline_johnny_only():
    item = {"title": "Johnny C. Taylor speaks at conference", "selftext": "", "description": ""}
    assert classify_topic(item) == "borderline"


def test_classify_off_topic():
    item = {"title": "General HR best practices", "selftext": "", "description": ""}
    assert classify_topic(item) == "off_topic"

