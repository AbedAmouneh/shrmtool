from notifications.message_builder import build_telegram_summary


def test_build_telegram_summary_includes_all_counts():
    msg = build_telegram_summary(
        topic="SHRM Trial Verdict",
        search_terms=["SHRM verdict", "SHRM trial"],
        total_new=7,
        news_count=2,
        twitter_count=2,
        linkedin_count=3,
        reddit_count=1,
        blocked_count=5,
        date_filtered_count=10,
        dedupe_count=3,
        offtopic_count=5,
    )

    assert "<b>New items added to sheet:</b> 7" in msg
    assert "📰 News: 2" in msg
    assert "🐦 X/Twitter: 2" in msg
    assert "👔 LinkedIn: 3" in msg
    assert "🔴 Reddit: 1" in msg
    assert "🛡️ Spam/Blocked: 5" in msg
    assert "📅 Date Filtered: 10" in msg
    assert "♻️ Duplicates Skipped: 3" in msg
    assert "🚫 Off-topic Discarded: 5" in msg
    assert "SHRM Trial Verdict" in msg
    assert "SHRM verdict, SHRM trial" in msg
    assert "URL Canonicalization (Aggressive)" in msg
    assert "Strict Date Guard" in msg
    assert "Spam Domain Blocking (Biztoc)" in msg
    assert "Title-Based Deduplication" in msg


def test_build_telegram_summary_escapes_html():
    msg = build_telegram_summary(
        topic="SHRM <Verdict>",
        search_terms=["A&B", "C<D"],
        total_new=1,
        news_count=1,
        twitter_count=0,
        linkedin_count=0,
        reddit_count=0,
        blocked_count=0,
        date_filtered_count=0,
        dedupe_count=0,
        offtopic_count=0,
    )

    assert "&lt;" in msg and "&gt;" in msg and "&amp;" in msg

