"""
hn_ingester.py — Production Grade
===================================
Pulls HackerNews stories mentioning target frameworks.
Uses Algolia HN Search API for efficient keyword search.

WHY ALGOLIA HN API NOT OFFICIAL HN API:
Official HN API = fetch top 500 stories one by one = 500 HTTP calls.
Algolia HN Search API = search by keyword = 1 HTTP call per framework.
Same data, 30x fewer API calls. Always pick the right tool.

PRODUCTION PATTERNS IMPLEMENTED:
  1. Incremental ingestion    — high-water mark from INGESTION_LOG
  2. Rate limit handler       — request counter with 1s polite delay
  3. Exponential backoff      — 3 retries: 2s, 4s, 8s
  4. Dead letter queue        — failed keywords → ERROR.DLQ_RECORDS
  5. Schema drift detection   — validates every response
  6. Run logging              — RUNNING → SUCCESS/FAILED
  7. Row count anomaly check  — 7-day rolling average
  8. Pagination handling      — fetches all pages per keyword
"""

import json
import time
import os
import uuid
import logging
from datetime import datetime, timezone, timedelta

import requests
import yaml
from dotenv import load_dotenv

from ingestion.utils.snowflake_utils import (
    get_snowflake_connection,
    execute_query,
    get_last_ingested_at,
    log_run_start,
    log_run_complete
)
from ingestion.utils.schema_validator import validate_and_filter
from ingestion.utils.anomaly_detector import check_row_count_anomaly
from ingestion.utils.slack_alerts import (
    send_dlq_alert,
    send_freshness_breach_alert
)

# ── Setup ──────────────────────────────────────────────────────
load_dotenv('config/.env')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s'
)
log = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────
# Algolia HN Search API — free, no auth, returns HN stories by keyword
ALGOLIA_BASE  = 'https://hn.algolia.com/api/v1'
SOURCE        = 'hackernews'

# Load framework keywords from config
with open('config/framework_keywords.yaml', 'r') as f:
    KEYWORDS = yaml.safe_load(f)['keywords']

log.info(f"Loaded {len(KEYWORDS)} framework keywords from config")


# ══════════════════════════════════════════════════════════════════
# PATTERN 2 — RATE LIMIT HANDLER
# ══════════════════════════════════════════════════════════════════
class RequestThrottle:
    """
    Simple token bucket throttle.
    HackerNews/Algolia has no strict rate limit but we add
    a polite delay to avoid being blocked.

    WHY A CLASS NOT A FUNCTION:
    We need to track request count across multiple calls.
    A class holds state between calls. A function cannot.
    In production this would be Redis-backed for distributed
    pipelines running on multiple workers.

    INTERVIEW Q: How do you implement rate limiting in a
                 distributed pipeline?
    A: In single-process: token bucket in memory.
       In distributed: Redis INCR with TTL as atomic counter.
       Every worker checks Redis before making API call.
       This is how Uber's API gateway implements rate limiting.
    """
    def __init__(self, max_per_minute: int = 60):
        self.max_per_minute = max_per_minute
        self.request_count  = 0
        self.window_start   = time.time()

    def wait_if_needed(self):
        """Sleep if we are approaching rate limit."""
        self.request_count += 1
        elapsed = time.time() - self.window_start

        if elapsed >= 60:
            # Reset window
            self.request_count = 1
            self.window_start  = time.time()
            return

        if self.request_count >= self.max_per_minute - 5:
            sleep_time = 60 - elapsed + 1
            log.warning(
                f"Rate limit approaching ({self.request_count} requests). "
                f"Sleeping {sleep_time:.0f}s..."
            )
            time.sleep(sleep_time)
            self.request_count = 0
            self.window_start  = time.time()

        # Polite delay between every request
        time.sleep(0.5)


throttle = RequestThrottle(max_per_minute=60)


# ══════════════════════════════════════════════════════════════════
# PATTERN 3 — EXPONENTIAL BACKOFF FETCH
# ══════════════════════════════════════════════════════════════════
def fetch_with_retry(url: str, params: dict, conn,
                     context_label: str) -> dict | None:
    """
    Fetches URL with exponential backoff on failure.
    Writes to DLQ after 3 failed attempts.
    """
    delays = [2, 4, 8]

    for attempt, delay in enumerate(delays, start=1):
        try:
            throttle.wait_if_needed()
            response = requests.get(url, params=params, timeout=15)

            if response.status_code == 200:
                return response.json()

            elif response.status_code == 429:
                # Explicit rate limit response
                log.warning(
                    f"429 Too Many Requests for {context_label}. "
                    f"Sleeping 30s..."
                )
                time.sleep(30)
                continue

            elif response.status_code in (500, 502, 503, 504):
                log.warning(
                    f"Server error {response.status_code} attempt "
                    f"{attempt}/3 for {context_label}. "
                    f"Retrying in {delay}s..."
                )
                time.sleep(delay)
                continue

            else:
                log.warning(
                    f"Status {response.status_code} for {context_label}"
                )
                return None

        except (requests.exceptions.Timeout,
                requests.exceptions.ConnectionError) as e:
            log.warning(
                f"Network error attempt {attempt}/3 "
                f"for {context_label}: {e}. Retrying in {delay}s..."
            )
            time.sleep(delay)

    # All retries exhausted
    log.error(f"All retries failed for {context_label}. Writing to DLQ.")
    write_to_dlq(conn, context_label, "All 3 retries exhausted", SOURCE)
    return None


# ══════════════════════════════════════════════════════════════════
# PATTERN 4 — DEAD LETTER QUEUE
# ══════════════════════════════════════════════════════════════════
def write_to_dlq(conn, raw_record: str,
                 error_message: str, source: str):
    """Writes failed record to ERROR.DLQ_RECORDS."""
    record_id = str(uuid.uuid4())
    execute_query(
        conn,
        """
        INSERT INTO DEV_ECOSYSTEM_DB.ERROR.DLQ_RECORDS
        (ID, RAW_RECORD, ERROR_MESSAGE, SOURCE, RETRY_COUNT, STATUS)
        SELECT %s, %s, %s, %s, 0, 'PENDING'
        """,
        (record_id, raw_record, error_message, source)
    )
    conn.commit()
    send_dlq_alert(source, record_id, error_message, 0)


# ══════════════════════════════════════════════════════════════════
# PATTERN 1 — INCREMENTAL INGESTION
# ══════════════════════════════════════════════════════════════════
def get_fetch_since_timestamp(conn) -> int:
    """
    Returns Unix timestamp to fetch stories from.

    First run  → last 7 days (get baseline data)
    Next runs  → since last successful ingestion

    WHY 7 DAYS FOR FIRST RUN:
    HN has years of data. Fetching everything = millions of records.
    7 days gives us enough baseline to start anomaly detection.
    After first run, incremental keeps data fresh daily.

    INTERVIEW Q: How do you handle first run vs incremental run?
    A: Check metadata table for last successful run timestamp.
       If none exists, define a sensible backfill window (7-30 days)
       rather than fetching everything. Document this decision —
       full historical backfill should be a separate one-time job,
       not mixed into the daily incremental pipeline.
    """
    last_run = get_last_ingested_at(conn, SOURCE)

    if last_run is None:
        # First run — fetch last 7 days
        since_dt = datetime.now(timezone.utc) - timedelta(days=30)
        log.info(f"First run — fetching last 7 days from {since_dt.date()}")
    else:
        since_dt = last_run
        log.info(f"Incremental run — fetching since {since_dt.isoformat()}")

    return int(since_dt.timestamp())


# ══════════════════════════════════════════════════════════════════
# CORE FETCH — ALGOLIA SEARCH WITH PAGINATION
# ══════════════════════════════════════════════════════════════════
def fetch_stories_for_keyword(keyword: str, since_ts: int,
                               conn) -> list[dict]:
    """
    Fetches all HN stories mentioning a keyword since timestamp.
    Handles pagination — fetches all pages not just first.

    WHY ALGOLIA NOT OFFICIAL HN FIREBASE API:
    Official API = you get top N story IDs, then fetch each one
    separately = N+1 HTTP calls. For 500 stories = 501 calls.
    Algolia = search endpoint returns full story objects with
    keyword filter and date range = 1-3 calls per keyword.
    Always use the search API when available.

    INTERVIEW Q: What is the N+1 query problem?
    A: When you fetch a list of IDs then make one DB/API call
       per ID. Classic example: fetch 100 user IDs, then loop
       and fetch each user profile = 101 queries.
       Fix: use a search/batch endpoint, or SQL IN clause,
       or DataLoader pattern (used by GraphQL).
    """
    url        = f"{ALGOLIA_BASE}/search"
    all_hits   = []
    page       = 0
    total_pages = 1  # will update after first response

    while page < total_pages:
        params = {
            'query':        keyword,
            'tags':         'story',        # only stories not comments
            'numericFilters': f'created_at_i>{since_ts}',
            'hitsPerPage':  100,
            'page':         page,
            'attributesToRetrieve': (
                'objectID,title,url,author,points,'
                'num_comments,created_at_i,story_text'
            )
        }

        data = fetch_with_retry(
            url, params, conn,
            f"hackernews/search/{keyword}/page{page}"
        )

        if data is None:
            break

        hits        = data.get('hits', [])
        total_pages = data.get('nbPages', 1)
        all_hits.extend(hits)

        log.info(
            f"  [{keyword}] Page {page+1}/{total_pages} "
            f"— {len(hits)} stories fetched"
        )

        page += 1

        # Safety limit — max 5 pages per keyword per run
        # Prevents runaway fetches on popular keywords like 'python'
        if page >= 5:
            log.info(f"  [{keyword}] Page limit reached. Stopping.")
            break

    return all_hits


# ══════════════════════════════════════════════════════════════════
# BRONZE WRITER
# ══════════════════════════════════════════════════════════════════
def write_posts_to_bronze(cursor, conn, keyword: str,
                           stories: list[dict], run_id: str,
                           week_start: str) -> int:
    """
    Writes HN stories to BRONZE.HACKERNEWS_POSTS_RAW.
    One row per story per keyword mention.

    Returns count of rows written.

    WHY ONE ROW PER STORY NOT AGGREGATED:
    Bronze = raw. Never aggregate at ingestion time.
    If you aggregate now and your formula is wrong,
    you cannot recover the original data.
    Keep raw, aggregate in Silver/Gold with dbt.
    This is the core Medallion Architecture principle.

    INTERVIEW Q: Why store raw data before transforming?
    A: Raw data is your source of truth. If a transformation
       has a bug you can reprocess from Bronze without
       re-calling the API. Re-fetching from API costs quota,
       time, and may be impossible for historical data.
       Bronze is your insurance policy.
    """
    written = 0

    for story in stories:
        try:
            # PATTERN 5 — Schema validation
            filtered = validate_and_filter(story, SOURCE, conn)

            post_id       = story.get('objectID', '')
            title         = story.get('title', '')[:500]  # truncate long titles
            score         = story.get('points', 0) or 0
            comment_count = story.get('num_comments', 0) or 0
            author        = story.get('author', '') or ''
            post_url      = story.get('url', '') or ''
            created_ts    = story.get('created_at_i', 0)

            # Convert Unix timestamp to datetime
            created_at = datetime.fromtimestamp(
                created_ts, tz=timezone.utc
            ).isoformat() if created_ts else None

            raw_json = json.dumps(filtered, ensure_ascii=False)

            cursor.execute(
                """
                INSERT INTO DEV_ECOSYSTEM_DB.BRONZE.HACKERNEWS_POSTS_RAW
                (POST_ID, FRAMEWORK_MENTIONED, TITLE, SCORE,
                 COMMENT_COUNT, AUTHOR, POST_URL, POST_TYPE,
                 CREATED_AT, WEEK_START, INGESTION_RUN_ID, RAW_DATA)
                SELECT %s, %s, %s, %s, %s, %s, %s, %s,
                       %s, %s, %s, PARSE_JSON(%s)
                """,
                (
                    int(post_id) if post_id else 0,
                    keyword,
                    title,
                    score,
                    comment_count,
                    author,
                    post_url,
                    'story',
                    created_at,
                    week_start,
                    run_id,
                    raw_json
                )
            )
            written += 1

        except Exception as e:
            log.warning(f"  Failed to write story {story.get('objectID')}: {e}")
            continue

    conn.commit()
    return written


# ══════════════════════════════════════════════════════════════════
# MAIN ORCHESTRATOR
# ══════════════════════════════════════════════════════════════════
def run_hn_ingestion():
    """
    Main entry point. Orchestrates all 8 production patterns.

    FLOW:
    1.  Generate unique run_id
    2.  Connect to Snowflake
    3.  Log RUNNING status
    4.  Get incremental timestamp
    5.  For each keyword: fetch stories → validate → write to Bronze
    6.  Handle failures with DLQ
    7.  Log final status with counts
    8.  Run anomaly detection
    """
    run_id     = str(uuid.uuid4())
    week_start = (
        datetime.now(timezone.utc) - timedelta(
            days=datetime.now(timezone.utc).weekday()
        )
    ).strftime('%Y-%m-%d')

    log.info("=" * 65)
    log.info(f"HackerNews Ingestion Started | run_id: {run_id}")
    log.info(f"Week start: {week_start}")
    log.info(f"Keywords to process: {len(KEYWORDS)}")
    log.info("=" * 65)

    conn   = get_snowflake_connection()
    cursor = conn.cursor()

    # PATTERN 6 — Log RUNNING at start
    log_run_start(conn, run_id, SOURCE)

    # PATTERN 1 — Get incremental timestamp
    since_ts = get_fetch_since_timestamp(conn)

    fetched_count = 0
    written_count = 0
    failed_count  = 0
    dlq_count     = 0

    for keyword in KEYWORDS:
        log.info(f"\nProcessing keyword: [{keyword}]")

        try:
            # Fetch all stories for this keyword
            stories = fetch_stories_for_keyword(keyword, since_ts, conn)
            fetched_count += len(stories)

            if not stories:
                log.info(f"  No stories found for [{keyword}]")
                continue

            log.info(f"  Total stories fetched: {len(stories)}")

            # Calculate quick stats for logging
            avg_score = (
                sum(s.get('points', 0) or 0 for s in stories)
                / len(stories)
            )
            avg_comments = (
                sum(s.get('num_comments', 0) or 0 for s in stories)
                / len(stories)
            )
            log.info(
                f"  Avg score: {avg_score:.1f} | "
                f"Avg comments: {avg_comments:.1f}"
            )

            # Write to Bronze
            written = write_posts_to_bronze(
                cursor, conn, keyword,
                stories, run_id, week_start
            )
            written_count += written
            log.info(f"  ✅ Written to Bronze: {written} rows")

        except Exception as e:
            log.error(f"  Failed processing keyword [{keyword}]: {e}")
            write_to_dlq(conn, keyword, str(e), SOURCE)
            failed_count += 1
            dlq_count    += 1

    # PATTERN 6 — Log final status
    final_status = 'SUCCESS' if failed_count == 0 else 'PARTIAL'
    log_run_complete(
        conn, run_id, SOURCE,
        fetched_count, written_count,
        failed_count, dlq_count,
        final_status
    )

    # PATTERN 7 — Anomaly check
    anomaly = check_row_count_anomaly(SOURCE, written_count, conn)
    if anomaly['status'] == 'ANOMALY_LOW':
        send_freshness_breach_alert(
            SOURCE, 0,
            f"Row count anomaly: {anomaly['message']}"
        )

    # ── Final Summary ─────────────────────────────────────────
    log.info("\n" + "=" * 65)
    log.info("HackerNews Ingestion Complete")
    log.info(f"  Run ID   : {run_id}")
    log.info(f"  Keywords : {len(KEYWORDS)}")
    log.info(f"  Fetched  : {fetched_count} stories")
    log.info(f"  Written  : {written_count} rows")
    log.info(f"  Failed   : {failed_count}")
    log.info(f"  DLQ      : {dlq_count}")
    log.info(f"  Status   : {final_status}")
    log.info(f"  Anomaly  : {anomaly['status']}")
    log.info("=" * 65)

    cursor.close()
    conn.close()


if __name__ == '__main__':
    run_hn_ingestion()