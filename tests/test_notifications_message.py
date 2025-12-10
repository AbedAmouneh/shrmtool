from notifications.message_builder import build_telegram_summary


def test_build_telegram_summary_includes_all_counts():
    msg = build_telegram_summary(
        topic="SHRM Trial Verdict",
        search_terms=["SHRM verdict", "SHRM trial"],
        total_new=4,
        news_count=2,
        twitter_count=2,
        repost_count=1,
        dedupe_count=3,
        offtopic_count=5,
    )

    assert "<b>New items added to sheet:</b> 4 items" in msg
    assert "News: 2" in msg
    assert "X/Twitter: 2" in msg
    assert "Reposts detected: 1" in msg
    assert "Duplicates removed: 3" in msg
    assert "Off-topic discarded: 5" in msg
    assert "SHRM Trial Verdict" in msg
    assert "SHRM verdict, SHRM trial" in msg


def test_build_telegram_summary_escapes_html():
    msg = build_telegram_summary(
        topic="SHRM <Verdict>",
        search_terms=["A&B", "C<D"],
        total_new=1,
        news_count=1,
        twitter_count=0,
        repost_count=0,
        dedupe_count=0,
        offtopic_count=0,
    )

    assert "&lt;" in msg and "&gt;" in msg and "&amp;" in msg

