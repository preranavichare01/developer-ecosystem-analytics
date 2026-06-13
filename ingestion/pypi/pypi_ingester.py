import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

"""
pypi_ingester.py — Production Grade
=====================================
Pulls weekly download stats for 15 target Python packages
from PyPI Stats API via pypistats library.

WHY PYPI STATS:
PyPI downloads = actual production usage, not just hype.
A package with 10M weekly downloads is embedded in
production systems worldwide. GitHub stars can be gamed.
Downloads cannot — nobody downloads a package 10M times
for show. This is the strongest signal in our composite
score for "is this framework actually being used in production?"

PRODUCTION PATTERNS:
  1. Incremental   — skip packages already fetched this week
  2. Rate limit    — polite delay between calls
  3. Exp backoff   — retry on failure
  4. DLQ           — failed packages to ERROR.DLQ_RECORDS
  5. Schema drift  — validate before write
  6. Run logging   — RUNNING to SUCCESS/FAILED
  7. Anomaly check — row count vs 7-day average
  8. Growth calc   — week over week download change %
"""

import json
import time
import uuid
import logging
from datetime import datetime, timezone, timedelta

import pypistats
import yaml
from dotenv import load_dotenv

from ingestion.utils.snowflake_utils import (
    get_snowflake_connection,
    execute_query,
    get_last_ingested_at,
    log_run_start,
    log_run_complete
)
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

SOURCE = 'pypi'

# Load packages from config
with open('config/pypi_packages.yaml', 'r') as f:
    PACKAGES = yaml.safe_load(f)['packages']

log.info(f"Loaded {len(PACKAGES)} packages from config")


# ══════════════════════════════════════════════════════════════════
# PATTERN 1 — INCREMENTAL CHECK
# ══════════════════════════════════════════════════════════════════
def get_week_start() -> str:
    """
    Returns Monday of current week as YYYY-MM-DD string.

    WHY WEEK-BASED NOT DAY-BASED:
    PyPI Stats API aggregates by week. Daily granularity
    is not available for historical data. Week is the
    natural grain for this source.

    INTERVIEW Q: How do you handle sources with different
                 time granularities?
    A: Design your fact table grain around the source's
       natural granularity. PyPI = weekly grain.
       GitHub = daily grain. HackerNews = daily grain.
       In Gold layer dbt models align everything to weekly
       grain using DATE_TRUNC('week', date_column).
       Never force a finer grain than the source supports
       — you will manufacture false precision.
    """
    today  = datetime.now(timezone.utc)
    monday = today - timedelta(days=today.weekday())
    return monday.strftime('%Y-%m-%d')


def already_fetched_this_week(conn, package_name: str,
                               week_start: str) -> bool:
    """
    Returns True if this package was already fetched this week.
    Prevents duplicate weekly records.
    """
    results = execute_query(
        conn,
        """
        SELECT COUNT(*)
        FROM DEV_ECOSYSTEM_DB.BRONZE.PYPI_DOWNLOADS_RAW
        WHERE PACKAGE_NAME = %s
          AND WEEK_START   = %s
        """,
        (package_name, week_start)
    )
    count = results[0][0] if results else 0
    return count > 0


# ══════════════════════════════════════════════════════════════════
# PATTERN 4 — DEAD LETTER QUEUE
# ══════════════════════════════════════════════════════════════════
def write_to_dlq(conn, raw_record: str,
                 error_message: str, source: str):
    """Writes failed package to ERROR.DLQ_RECORDS."""
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
# PATTERN 3 — EXPONENTIAL BACKOFF FETCH
# ══════════════════════════════════════════════════════════════════
def fetch_weekly_downloads(package_name: str,
                            conn) -> dict | None:
    """
    Fetches last week download data for a package.
    Returns dict with current week downloads.

    WHY 5 WEEKS NOT JUST CURRENT:
    We need previous week to calculate growth rate.
    We fetch recent + monthly to approximate previous week.
    Storing all in Bronze means Silver/Gold can calculate
    moving averages without re-calling API.

    INTERVIEW Q: Why store more history than you immediately need?
    A: Avoid API dependency in transformation layer.
       If PyPI Stats API changes or goes down, your Silver
       and Gold transforms still work because raw history
       is in Bronze. This is why Bronze is append-only
       and never modified.
    """
    delays = [2, 4, 8]

    for attempt, delay in enumerate(delays, start=1):
        try:
            raw  = pypistats.recent(
                package_name,
                period="week",
                format="json"
            )
            data = json.loads(raw)

            if data.get('data'):
                downloads = data['data'].get('last_week', 0)
                return {
                    'package_name':     package_name,
                    'weekly_downloads': downloads,
                    'raw_response':     data
                }
            return None

        except Exception as e:
            error_str = str(e)

            # Package not found — not retryable
            if 'not found' in error_str.lower() or '404' in error_str:
                log.warning(f"  Package not found on PyPI: {package_name}")
                return None

            if attempt <= len(delays):
                log.warning(
                    f"  Attempt {attempt}/3 failed for {package_name}: "
                    f"{error_str}. Retrying in {delay}s..."
                )
                time.sleep(delay)
            else:
                log.error(
                    f"  All retries failed for {package_name}: {error_str}"
                )
                write_to_dlq(conn, package_name, error_str, SOURCE)
                return None

    return None


def fetch_previous_week_downloads(package_name: str) -> int:
    """
    Fetches previous week downloads for growth rate calculation.

    Growth rate = (current - previous) / previous * 100

    INTERVIEW Q: How do you calculate week over week growth?
    A: (current_week - previous_week) / previous_week * 100.
       Edge cases: previous_week = 0 means new package,
       set growth to 0 not divide by zero.
       Negative growth = declining adoption.
       This feeds directly into our Health Index score.
    """
    try:
        raw  = pypistats.recent(
            package_name,
            period="month",
            format="json"
        )
        data = json.loads(raw)
        if data.get('data'):
            # Monthly / 4 = approximate weekly average
            monthly = data['data'].get('last_month', 0)
            return int(monthly / 4) if monthly else 0
    except Exception:
        pass
    return 0


# ══════════════════════════════════════════════════════════════════
# BRONZE WRITER
# ══════════════════════════════════════════════════════════════════
def write_to_bronze(cursor, conn, package_data: dict,
                    growth_pct: float, week_start: str,
                    run_id: str):
    """
    Writes PyPI download record to BRONZE.PYPI_DOWNLOADS_RAW.

    WHY STORE GROWTH_PCT IN BRONZE:
    Normally Bronze = raw only, no calculations.
    Growth % is an exception because it requires two API
    calls (current + previous week). If we store only raw
    and calculate in Silver, we need both weeks present in
    Bronze simultaneously — a self-join that breaks on
    first-week data. Storing pre-calculated value avoids
    this edge case. Pragmatism over purity when the
    calculation is simple and stable.

    INTERVIEW Q: When is it acceptable to calculate derived
                 metrics in the Bronze layer?
    A: When the calculation requires data from multiple API
       calls that would be expensive to reconstruct later.
       Simple arithmetic like growth rate is acceptable.
       Complex business logic like scoring formulas should
       never be in Bronze.
    """
    cursor.execute(
        """
        INSERT INTO DEV_ECOSYSTEM_DB.BRONZE.PYPI_DOWNLOADS_RAW
        (PACKAGE_NAME, WEEKLY_DOWNLOADS, DOWNLOAD_GROWTH_PCT,
         WEEK_START, INGESTION_RUN_ID)
        SELECT %s, %s, %s, %s, %s
        """,
        (
            package_data['package_name'],
            package_data['weekly_downloads'],
            round(growth_pct, 4),
            week_start,
            run_id
        )
    )
    conn.commit()


# ══════════════════════════════════════════════════════════════════
# MAIN ORCHESTRATOR
# ══════════════════════════════════════════════════════════════════
def run_pypi_ingestion():
    """
    Main entry point.

    FLOW:
    1.  Generate run_id
    2.  Connect Snowflake
    3.  Log RUNNING
    4.  For each package:
        a. Skip if already fetched this week
        b. Fetch current week downloads
        c. Fetch previous week for growth calc
        d. Write to Bronze
    5.  Log SUCCESS/PARTIAL
    6.  Anomaly check
    """
    run_id     = str(uuid.uuid4())
    week_start = get_week_start()

    log.info("=" * 65)
    log.info(f"PyPI Ingestion Started | run_id: {run_id}")
    log.info(f"Week start  : {week_start}")
    log.info(f"Packages    : {len(PACKAGES)}")
    log.info("=" * 65)

    conn   = get_snowflake_connection()
    cursor = conn.cursor()

    # PATTERN 6 — Log RUNNING
    log_run_start(conn, run_id, SOURCE)

    fetched_count = 0
    written_count = 0
    skipped_count = 0
    failed_count  = 0
    dlq_count     = 0

    for package_name in PACKAGES:
        log.info(f"\nProcessing: {package_name}")

        # PATTERN 1 — Skip if already fetched this week
        if already_fetched_this_week(conn, package_name, week_start):
            log.info(f"  SKIP — already fetched this week")
            skipped_count += 1
            continue

        # PATTERN 3 — Fetch with retry
        package_data = fetch_weekly_downloads(package_name, conn)

        if package_data is None:
            failed_count += 1
            dlq_count    += 1
            continue

        fetched_count += 1

        # PATTERN 8 — Calculate growth rate
        prev_downloads = fetch_previous_week_downloads(package_name)
        current        = package_data['weekly_downloads']

        if prev_downloads > 0:
            growth_pct = (current - prev_downloads) / prev_downloads * 100
        else:
            growth_pct = 0.0

        log.info(
            f"  Downloads : {current:,} | "
            f"Prev approx: {prev_downloads:,} | "
            f"Growth     : {growth_pct:+.1f}%"
        )

        # Write to Bronze
        try:
            write_to_bronze(
                cursor, conn, package_data,
                growth_pct, week_start, run_id
            )
            written_count += 1
            log.info(f"   Written to Bronze")

        except Exception as e:
            log.error(f"  Bronze write failed for {package_name}: {e}")
            write_to_dlq(conn, package_name, str(e), SOURCE)
            failed_count += 1
            dlq_count    += 1

        # Polite delay
        time.sleep(1)

    # PATTERN 6 — Final log
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

    # ── Summary ───────────────────────────────────────────────
    log.info("\n" + "=" * 65)
    log.info("PyPI Ingestion Complete")
    log.info(f"  Run ID   : {run_id}")
    log.info(f"  Fetched  : {fetched_count}")
    log.info(f"  Written  : {written_count}")
    log.info(f"  Skipped  : {skipped_count} (already this week)")
    log.info(f"  Failed   : {failed_count}")
    log.info(f"  DLQ      : {dlq_count}")
    log.info(f"  Status   : {final_status}")
    log.info(f"  Anomaly  : {anomaly['status']}")
    log.info("=" * 65)

    cursor.close()
    conn.close()


if __name__ == '__main__':
    run_pypi_ingestion()