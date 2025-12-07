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
- Extracts source name from nested `source` object
- Maps NewsAPI fields: `description` → summary, `publishedAt` → date
- Sets engagement fields to "N/A" (News articles don't have likes/comments/shares)

### Shared Processing

Both collectors' normalized items go through:

1. **Verdict Date Filtering**: Only items with dates >= VERDICT_DATE (in US/Eastern timezone) are kept
2. **Deduplication**: URLs are checked against a SQLite database (`seen_urls.db`) to prevent duplicates
3. **Schema Mapping**: Items are converted to the 17-column Google Sheet format
4. **Google Sheets Append**: New rows are appended in batch to the first worksheet

## Running

### Basic Usage

Activate your virtual environment and run:

```bash
python -m main_collect --terms "SHRM,HR,verdict" --topic "SHRM Trial Verdict"
```

### CLI Options

```bash
python -m main_collect \
  --terms "SHRM,HR,verdict" \
  --topic "SHRM Trial Verdict" \
  --since 2025-12-05 \
  --dry-run \
  --max-results 50
```

**Arguments:**
- `--terms` (required): Comma-separated list of search terms
- `--topic` (required): Topic label for the 'Topic' column in the sheet
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

| Column | Type | Description | Reddit Source | News Source |
|-------|------|-------------|---------------|-------------|
| **Date Posted** | String (MM/DD/YYYY) | Post/article publication date | `date` field (parsed) | `publishedAt` (parsed) |
| **Platform** | String | Source platform name | `"Reddit"` | `"News"` |
| **Profile** | String | Author/source identifier | `u/{username}` | `source_name` |
| **Link (profile)** | String | Profile URL | `https://www.reddit.com/user/{username}` | `"N/A"` |
| **Nº Of Followers** | String | Follower count | `"N/A"` | `"N/A"` |
| **Post Link** | String | Direct URL to post/article | `url` | `url` |
| **Topic** | String | Topic label | CLI argument | CLI argument |
| **title** | String | Post/article title | `title` | `title` |
| **Tone** | String | Sentiment classification | `"N/A"` | `"N/A"` |
| **Views** | String | View count | `"N/A"` | `"N/A"` |
| **Likes** | String | Like/upvote count | `score` | `"N/A"` |
| **Comments** | String | Comment count | `numComments` | `"N/A"` |
| **Shares** | String | Share/retweet count | `"0"` | `"N/A"` |
| **Eng. Total** | String | Total engagement (likes + comments + shares) | Calculated sum | `"N/A"` |
| **Post Summary** | String | Summary text (truncated to ~300 chars) | `selftext` or `title` | `description` |
| **SHRM Like** | String | Manual input field | `""` (blank) | `""` (blank) |
| **SHRM Comment** | String | Manual input field | `""` (blank) | `""` (blank) |

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
│   └── summary.py
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

