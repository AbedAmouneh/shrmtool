# SHRM Content Collector

Automated collection tool for monitoring SHRM discrimination trial-related content from Reddit and news sources. Collects post-verdict content, deduplicates by URL, and appends standardized rows to a Google Sheet.

## Architecture Overview

The pipeline follows a simple data flow architecture:

```
Reddit Collector ───┐
                     ├── Normalize → Filter → Dedupe → Sheets Append
News Collector ──────┘

```


1. **Collectors** fetch raw content from Reddit (via snscrape) and NewsAPI
2. **Normalization** converts items into a unified 17-column schema
3. **Filtering** removes items before the verdict date
4. **Deduplication** prevents duplicate URLs from being appended
5. **Google Sheets** receives the final standardized rows

## Setup

### 1. Create a Virtual Environment

```bash
python3 -m venv venv
source venv/bin/activate  # On Linux/Mac
# or
venv\Scripts\activate  # On Windows
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure Environment Variables

Create a `.env` file in the project root with the following variables:

```env
NEWS_API_KEY=your_newsapi_key_here
SHEET_ID=your_google_sheet_id_here
VERDICT_DATE=2025-12-05
```

- `NEWS_API_KEY`: Your NewsAPI.org API key
- `SHEET_ID`: The Google Sheet ID (found in the sheet's URL)
- `VERDICT_DATE`: ISO date string (YYYY-MM-DD) for the minimum date filter

### 4. Service Account Setup

Place your `service_account.json` file in the project root directory. This file should:

- Be created in Google Cloud Console
- Have the Google Sheets API enabled
- Be shared with the Google Sheet (share the service account email with Editor access)

## How Collectors Work

### Reddit Collector

The Reddit collector uses `snscrape` (a command-line tool) to search Reddit for posts matching specified search terms.

**Flow:**

1. For each search term, runs `snscrape reddit-search "query" --jsonl` as a subprocess
2. Parses JSONL output (one JSON object per line)
3. Normalizes each post to extract: URL, title, date, author, score, comments, selftext
4. Filters out posts missing URLs or dates
5. Deduplicates within the collection run (same URL seen multiple times)

**Normalization:**

- Extracts username and builds profile link (`https://www.reddit.com/user/{username}`)
- Parses date from various formats (ISO string, Unix timestamp)
- Maps Reddit-specific fields: `score` → likes, `numComments` → comments, `shares` = 0
- Calculates engagement total (likes + comments + shares)

### News Collector

The News collector uses NewsAPI.org's `/everything` endpoint to fetch articles.

**Flow:**

1. For each search term, makes HTTP GET requests to NewsAPI
2. Handles pagination (fetches multiple pages if needed, up to max_results)
3. Normalizes each article to extract: source name, title, description, URL, publishedAt
4. Filters out articles missing URLs or dates
5. Deduplicates within the collection run

**Normalization:**

- Extracts source name from nested `source` object (e.g., "Business Insider", "Reuters")
- Extracts author name if available from NewsAPI response
- Maps NewsAPI fields: `description` → summary, `publishedAt` → date
- Sets engagement fields to "N/A" (News articles don't have likes/comments/shares)
- Profile column shows the news source name (or falls back to URL domain if source is missing)
- Post Summary includes source attribution: `"Description text (Source: Business Insider – by Jack Newsham)"` or `"Description text (Source: Business Insider)"` if author is missing

### X Collector

The X (Twitter) collector uses the X API v2 Recent Search endpoint to fetch tweets.

**Env vars:**

- `X_BEARER_TOKEN`: X API bearer token

**Flow:**

1. For each search term, calls `https://api.twitter.com/2/tweets/search/recent`
2. Requests `tweet.fields=created_at,public_metrics,text` and `expansions=author_id` with `user.fields=username,public_metrics`
3. Normalizes each tweet to SHRM schema:
   - Platform: `X`
   - URL: `https://twitter.com/i/web/status/{tweet_id}`
   - Profile: `@username`, Profile link: `https://x.com/{username}`
   - Followers: `followers_count` (or `"N/A"`)
   - Likes: `like_count`
   - Comments: `reply_count`
   - Shares: `retweet_count + quote_count`
   - Eng. Total: likes + comments + shares
   - Views: `impression_count` if available, else `"N/A"`
   - Title: first ~160 chars of text
   - Post Summary: same as title
   - SHRM Like / SHRM Comment: `"N/A"`
4. Filters by verdict date using `is_after_verdict_date`
5. Deduplicates within the run (same tweet URL only once)
6. Errors are logged but non-fatal (continues to next query)

### Shared Processing

Both collectors' normalized items go through:

1. **Verdict Date Filtering**: Only items with dates >= VERDICT_DATE (in US/Eastern timezone) are kept
2. **Anchor-Based Topic Filtering**: A final safety layer that keeps only items clearly related to SHRM/JCT. Items must contain at least one of these anchor terms in their title, body, or description:

   - "shrm"
   - "society for human resource management"
   - "johnny c. taylor"
   - "johnny taylor"
   - "shrm ceo"

   This filter applies to both Reddit posts and News articles. Off-topic items are logged and removed before deduplication.

3. **Deduplication**: URLs are checked against a SQLite database (`seen_urls.db`) to prevent duplicates
4. **Schema Mapping**: Items are converted to the 17-column Google Sheet format
5. **Google Sheets Append**: New rows are appended in batch to the first worksheet

**Example of Topic Filtering:**

- ✅ **Kept**: "SHRM CEO Johnny Taylor faces backlash after verdict" (contains "shrm" and "johnny taylor")
- ✅ **Kept**: "Society for Human Resource Management trial update" (contains "society for human resource management")
- ❌ **Removed**: "HR best practices for 2025" (no anchor terms, off-topic)
- ❌ **Removed**: "General workplace discrimination discussion" (no SHRM-specific anchors)

## Running

### Basic Usage

Activate your virtual environment and run:

```bash
python -m main_collect \
  --terms "SHRM verdict,SHRM trial,SHRM lawsuit,SHRM scandal,SHRM controversy,SHRM harassment allegations,SHRM sexual harassment case,Johnny C. Taylor SHRM,SHRM CEO Johnny Taylor,Society for Human Resource Management trial,Society for Human Resource Management verdict" \
  --topic "SHRM Trial Verdict – Public & HR Community Reaction"
```

### Recommended Search Terms

For high-precision collection, use this comma-separated list:

```
SHRM verdict,SHRM trial,SHRM lawsuit,SHRM scandal,SHRM controversy,SHRM harassment allegations,SHRM sexual harassment case,Johnny C. Taylor SHRM,SHRM CEO Johnny Taylor,Society for Human Resource Management trial,Society for Human Resource Management verdict
```

### CLI Options

```bash
python -m main_collect \
  --terms "SHRM verdict,SHRM trial,SHRM lawsuit,SHRM scandal,SHRM controversy,SHRM harassment allegations,SHRM sexual harassment case,Johnny C. Taylor SHRM,SHRM CEO Johnny Taylor,Society for Human Resource Management trial,Society for Human Resource Management verdict" \
  --topic "SHRM Trial Verdict – Public & HR Community Reaction" \
  --since 2025-12-05 \
  --dry-run \
  --max-results 50
```

**Arguments:**

- `--terms` (optional): Comma-separated list of search terms. If not provided, uses collector defaults. Recommended terms listed above.
- `--topic` (optional): Topic label for the 'Topic' column in the sheet. Default: "SHRM Trial Verdict – Public & HR Community Reaction"
- `--since` (optional): Override verdict date filter (YYYY-MM-DD format)
- `--dry-run` (flag): Run without writing to Google Sheets or updating dedupe store
- `--max-results` (optional): Maximum number of items to process

**Example Output:**

```
[2025-01-15 08:00:00] INFO - Starting SHRM content collection pipeline
[2025-01-15 08:00:00] INFO - Search terms: ['SHRM', 'HR', 'verdict']
[2025-01-15 08:00:00] INFO - Topic: SHRM Trial Verdict
[2025-01-15 08:00:00] INFO - Verdict date filter: 2025-12-05
...
[2025-01-15 08:01:30] INFO - Successfully appended 5 rows
[2025-01-15 08:01:30] INFO - Final Status: SUCCESS
```

## 17-Column Schema Definition

The Google Sheet uses a standardized 17-column schema. Here's how each column is populated:

| Column              | Type                | Description                                  | Reddit Source                            | News Source            |
| ------------------- | ------------------- | -------------------------------------------- | ---------------------------------------- | ---------------------- |
| **Date Posted**     | String (MM/DD/YYYY) | Post/article publication date                | `date` field (parsed)                    | `publishedAt` (parsed) |
| **Platform**        | String              | Source platform name                         | `"Reddit"`                               | `"News"`               |
| **Profile**         | String              | Author/source identifier                     | `u/{username}`                           | `source_name` (or URL domain fallback) |
| **Link (profile)**  | String              | Profile URL                                  | `https://www.reddit.com/user/{username}` | `"N/A"`                |
| **Nº Of Followers** | String              | Follower count                               | `"N/A"`                                  | `"N/A"`                |
| **Post Link**       | String              | Direct URL to post/article                   | `url`                                    | `url`                  |
| **Topic**           | String              | Topic label                                  | CLI argument                             | CLI argument           |
| **title**           | String              | Post/article title                           | `title`                                  | `title`                |
| **Tone**            | String              | Sentiment classification                     | `"N/A"`                                  | `"N/A"`                |
| **Views**           | String              | View count                                   | `"N/A"`                                  | `"N/A"`                |
| **Likes**           | String              | Like/upvote count                            | `score`                                  | `"N/A"`                |
| **Comments**        | String              | Comment count                                | `numComments`                            | `"N/A"`                |
| **Shares**          | String              | Share/retweet count                          | `"0"`                                    | `"N/A"`                |
| **Eng. Total**      | String              | Total engagement (likes + comments + shares) | Calculated sum                           | `"N/A"`                |
| **Post Summary**    | String              | Summary text (truncated to ~300 chars)       | `selftext` or `title`                    | `description` + `(Source: {source} – by {author})` |
| **SHRM Like**       | String              | Manual input field                           | `""` (blank)                             | `""` (blank)           |
| **SHRM Comment**    | String              | Manual input field                           | `""` (blank)                             | `""` (blank)           |

**Note:** The schema has been updated to the new 17-column format. The column order is:

1. Date Posted, 2. Platform, 3. Profile Link, 4. N° of Followers, 5. Post Link, 6. Topic title, 7. Summary, 8. Tone, 9. Category, 10. Views, 11. Likes, 12. Comments, 13. Shares, 14. Eng. Total, 15. Sentiment Score, 16. Verified (Y/N), 17. Notes

## Data Quality & Schema Guarantees

The pipeline implements strict data quality controls to ensure all rows written to the Google Sheet are clean, correctly mapped, and properly deduplicated.

### Schema Validation

- **Strict 17-column enforcement**: All rows must have exactly 17 columns in the canonical order
- **Required field validation**: Date Posted, Platform, Post Link, and Topic title must be non-empty
- **Fail-fast behavior**: Invalid rows are logged and skipped (not appended to the sheet)
- **Automatic defaults**: Missing non-metric values are filled with safe defaults ("N/A" or empty string)

### Metric Normalization & Eng. Total

- **K/M number parsing**: Follower counts and metrics in "64.5K" or "1.2M" format are parsed to integers
- **Consistent Eng. Total calculation**: `Eng. Total = Likes + Comments + Shares` (computed when all three are numeric)
- **Platform-specific defaults**:
  - **News**: All metrics (Views, Likes, Comments, Shares, Eng. Total) are set to "N/A"
  - **X/Reddit**: Metrics are normalized to integers; Eng. Total is computed from the sum
- **No blank metrics**: Metrics are never blank; they are either numeric strings or "N/A"

### Topic & Date Filtering

- **Enhanced topic classification**: Items are classified as "on_topic", "borderline", or "off_topic"
  - **on_topic**: Strong keyword presence (SHRM anchors OR Johnny C. Taylor + case context)
  - **borderline**: Weak mentions (Johnny C. Taylor without case context, generic HR content)
  - **off_topic**: No relevant keywords
- **Current behavior**: Only "on_topic" items are appended to the sheet; borderline and off_topic are logged and filtered out
- **Verdict date filtering**: All items must have dates >= VERDICT_DATE (in US/Eastern timezone)
- **Logging**: Topic filter decisions are logged with counts for observability

### Deduplication & Repost Tagging

- **Canonical URL normalization**: URLs are normalized by:
  - Converting http → https
  - Stripping tracking parameters (utm\_\*, fbclid, gclid, etc.)
  - Removing fragments (#...)
  - Lowercasing hostname
- **News deduplication**: If a News article's canonical URL has already been seen (any source), it is skipped entirely
- **Social platform repost detection**:
  - **Strict duplicate**: Same canonical URL + same profile → skipped
  - **Repost**: Same canonical URL + different profile → kept and tagged
    - `Category` field set to "Repost"
    - `Notes` field contains: "Repost of canonical URL: {canonical_url}"
- **URL validation**: Malformed or invalid URLs are rejected before processing

### Per-Platform Mapping Rules

Each platform has specific rules enforced during normalization:

- **News**:
  - Metrics (Views, Likes, Comments, Shares, Eng. Total): Always "N/A"
  - Followers: Always "N/A"
  - Profile (col 3): News source name (e.g., "Business Insider", "Reuters") from `source.name`, or URL domain (e.g., "biztoc.com") if source is missing
  - Profile Link: "N/A" (no profile URLs for news sources)
  - Post Summary: Description text with source attribution suffix: `"(Source: {source} – by {author})"` when both are available, or `"(Source: {source})"` when only source is available
- **X (Twitter)**:
  - Metrics: Must be numeric (parsed from API responses)
  - Followers: Numeric if available, "N/A" if not
  - Profile Link: `https://x.com/{username}` format
- **Reddit**:
  - Metrics: Numeric (score → likes, numComments → comments)
  - Followers: "N/A" (Reddit doesn't expose follower counts)
  - Profile Link: `https://www.reddit.com/user/{username}` format

### Troubleshooting Data Issues

**Why are some items not appearing in the sheet?**

- **Off-topic filtering**: Items without strong SHRM/JCT keywords are filtered out (check logs for "Topic filtering" messages)
- **Date filtering**: Items before VERDICT_DATE are excluded
- **Deduplication**: News articles with duplicate canonical URLs are skipped
- **URL validation**: Items with malformed URLs are rejected
- **Row validation**: Items that fail schema validation are logged and skipped

**Why are some News articles missing?**

- NewsAPI may return the same article from multiple sources; only the first one (by canonical URL) is kept
- Articles with invalid or missing URLs are skipped

**How are News articles displayed in the sheet?**

- **Profile column**: Shows the news source name (e.g., "Business Insider", "Reuters") from NewsAPI's `source.name` field
- If `source.name` is missing, the Profile column falls back to the URL domain (e.g., "biztoc.com")
- **Post Summary**: Includes source attribution at the end:
  - `"Article description... (Source: Business Insider – by Jack Newsham)"` when both source and author are available
  - `"Article description... (Source: Business Insider)"` when only source is available
  - `"Article description..."` when neither is available

**Why are some social posts tagged as "Repost"?**

- Multiple profiles sharing the same article URL are detected as reposts
- The first occurrence is kept as-is; subsequent occurrences are tagged with `Category="Repost"` and a note

## Testing

This project uses `pytest` for the test suite. All tests are fully isolated and do not call external services.

### How to Run Tests

From the project root:

```bash
# Basic test run
pytest

# More verbose output
pytest -v

# Run a single test file
pytest tests/test_main_collect.py -v

# Run a specific test
pytest tests/test_main_collect.py::TestMainCollectHappyPath::test_happy_path_mixed_sources -v
```

### Test Structure

Tests are organized in the `tests/` directory:

- **`tests/test_time_utils.py`** - Time/date utilities (parsing, formatting, timezone conversion)
- **`tests/test_sentiment.py`** - Sentiment classification utilities
- **`tests/test_summary.py`** - Summary generation utilities
- **`tests/test_dedupe_store.py`** - SQLite deduplication store
- **`tests/test_google_sheets.py`** - Google Sheets integration
- **`tests/test_collector.py`** - Reddit and NewsAPI collectors (combined)
- **`tests/test_main_collect.py`** - Main orchestrator end-to-end tests
- **`tests/test_metrics.py`** - Metric parsing and engagement helpers
- **`tests/test_url_utils.py`** - URL canonicalization and validation
- **`tests/test_schema.py`** - Schema build/validation helpers
- **`tests/test_dedupe_behavior.py`** - Canonical dedupe and repost tagging
- **`tests/test_topic_classification.py`** - Topic classification (on/borderline/off-topic)
- **`tests/test_notifications_message.py`** - Telegram message builder and notification helpers

### Mocking Strategy

All external services are mocked to ensure tests are fast, deterministic, and don't require real credentials:

- **Config values**: Provided via environment variables using `monkeypatch.setenv()` in fixtures (`tests/conftest.py`)
- **Reddit collector**: `subprocess.run` is mocked to simulate snscrape output (no real snscrape calls)
  - Tests provide fake JSONL lines representing Reddit posts
  - Verifies parsing and normalization logic
- **NewsAPI**: `requests.get` is mocked to simulate HTTP responses (no real API calls)
  - Tests provide fake JSON responses with `articles` array
  - Includes pagination testing
- **Google Sheets**: The entire `gspread` client chain is mocked (no real API calls)
  - `gspread.authorize()` returns a mock client
  - Mock worksheet captures `append_rows()` calls
  - Tests verify error handling (permission denied, not found, etc.)
- **SQLite dedupe store**: Uses temporary databases created with `tmp_path` fixture (no writes to real DB)
  - Each test gets a fresh temporary database
  - Tests verify `has_seen()` and `mark_seen()` behavior
- **Service account**: `Path.exists()` is mocked to avoid file system checks
- **Canonical dedupe**: Tests mock `has_seen_canonical*`/`mark_seen_canonical` to avoid touching real SQLite
- **Telegram notifications**: `send_telegram_message` is mocked to avoid real API calls

### Optional: Coverage

If you want a coverage report, install `pytest-cov`:

```bash
pip install pytest-cov
```

Then run:

```bash
pytest --cov=. --cov-report=term-missing
```

This will show line coverage and highlight any missing lines.

## How to Deploy

### Local Development

Run the collector manually:

```bash
python -m main_collect --terms "SHRM,HR,verdict" --topic "SHRM Trial Verdict"
```

### Cron Setup (Linux Server)

To run every 30 minutes, add to crontab:

```bash
*/30 * * * * cd /path/to/shrmtool && /path/to/venv/bin/python -m main_collect --terms "SHRM,HR,verdict" --topic "SHRM Trial Verdict" >> /path/to/logs/collect.log 2>&1
```

To run daily at 8 AM ET:

```bash
0 13 * * * cd /path/to/shrmtool && /path/to/venv/bin/python -m main_collect --terms "SHRM,HR,verdict" --topic "SHRM Trial Verdict" >> /path/to/logs/collect.log 2>&1
```

### GitHub Actions Automation

The project includes a GitHub Actions workflow (`.github/workflows/collector.yml`) that runs daily at 8 AM ET.

**Setup Instructions:**

1. **Add Secrets to GitHub Repository:**

   - Go to your repository → Settings → Secrets and variables → Actions
   - Add the following secrets:
     - `NEWS_API_KEY`: Your NewsAPI.org API key
     - `SHEET_ID`: Your Google Sheet ID
     - `VERDICT_DATE`: Verdict date in YYYY-MM-DD format (e.g., `2025-12-05`)
     - `SERVICE_ACCOUNT_JSON`: The entire contents of your `service_account.json` file (as a single-line JSON string)

2. **Verify Workflow:**

   - Go to Actions tab in your repository
   - The workflow will run automatically at 8 AM ET daily
   - You can also trigger it manually via "Run workflow" button

3. **Monitor Runs:**
   - Check the Actions tab for run history
   - Logs are available in each run's output
   - Artifacts (logs and service account file) are uploaded for 7 days

**Required Environment Variables:**

For local runs or server deployment, ensure these are set in your `.env` file:

```env
NEWS_API_KEY=your_newsapi_key_here
SHEET_ID=your_google_sheet_id_here
VERDICT_DATE=2025-12-05
```

For GitHub Actions, these are provided via secrets (see above).

**Optional: Telegram Notifications**

The pipeline can send detailed daily intake summaries via Telegram when new items are appended:

- **Env vars** (optional):
  - `TELEGRAM_BOT_TOKEN`: Bot token from BotFather
  - `TELEGRAM_CHAT_ID`: Your chat ID (integer or string)

- **Notification behavior**:
  - Only sent when not in `--dry-run` mode
  - Only sent when at least one new row was appended
  - Includes detailed statistics: platform breakdown (News/X/Reddit), repost counts, dedupe counts, off-topic filtering stats
  - Formatted as HTML for rich display in Telegram

- **Message format**: The notification includes:
  - Total new items added
  - Breakdown by platform (News, X/Twitter, Reddit)
  - Reposts detected and filtered
  - Duplicates removed
  - Off-topic items discarded
  - Focus topics and search terms used
  - Automated checks passed (URL canonicalization, schema validation, metric normalization, etc.)

**Service Account Setup:**

- **Local/Server**: Place `service_account.json` in the project root
- **GitHub Actions**: The workflow writes `SERVICE_ACCOUNT_JSON` secret to `service_account.json` automatically
- The code supports both methods: checks `SERVICE_ACCOUNT_JSON` env var first, then falls back to file

## Project Structure

```
shrmtool/
├── collectors/          # Reddit and news collectors
│   ├── reddit_collector.py
│   └── news_collector.py
├── integrations/        # Google Sheets and deduplication
│   ├── google_sheets.py
│   └── dedupe_store.py
├── utils/              # Configuration, time, sentiment, summary utilities
│   ├── config.py
│   ├── time_utils.py
│   ├── sentiment.py
│   ├── summary.py
│   ├── metrics.py
│   ├── schema.py
│   ├── url_utils.py
│   └── platform_rules.py
├── notifications/      # Notification services
│   ├── telegram_notifier.py
│   └── message_builder.py
├── tests/              # Test suite
│   ├── conftest.py
│   ├── test_*.py
│   └── ...
├── .github/
│   └── workflows/
│       └── collector.yml  # GitHub Actions workflow
├── main_collect.py     # Main orchestrator
├── requirements.txt    # Python dependencies
├── .env               # Environment variables (not committed)
└── service_account.json # Google service account (not committed)
```
