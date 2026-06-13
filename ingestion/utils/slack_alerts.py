"""
slack_alerts.py
---------------
All Slack notifications live here.
If SLACK_WEBHOOK_URL is empty, prints to console instead.
Pipeline NEVER crashes because Slack is not configured.

INTERVIEW Q: How do you handle optional dependencies in a pipeline?
A: Graceful degradation. The alerting layer is non-critical —
   if Slack is down or not configured, the pipeline continues.
   Only the data movement is critical. Always separate
   critical path from observability path.
"""

import os
import json
import logging
import requests
from dotenv import load_dotenv

load_dotenv('config/.env')
log = logging.getLogger(__name__)

SLACK_WEBHOOK_URL = os.getenv('SLACK_WEBHOOK_URL', '')


def _send(message: str):
    """Core send — webhook if configured, console if not."""
    if not SLACK_WEBHOOK_URL:
        log.info(f"[SLACK-CONSOLE] {message}")
        return
    try:
        requests.post(
            SLACK_WEBHOOK_URL,
            data=json.dumps({"text": message}),
            headers={"Content-Type": "application/json"},
            timeout=5
        )
    except Exception as e:
        log.warning(f"Slack send failed (non-critical): {e}")


def send_schema_drift_alert(source, removed_fields, added_fields):
    msg = (
        f"⚠️ *SCHEMA DRIFT DETECTED*\n"
        f"Source: `{source}`\n"
        f"Removed fields: `{removed_fields}` ← CRITICAL, pipeline filtered these\n"
        f"Added fields: `{added_fields}` ← logged, pipeline continued"
    )
    _send(msg)


def send_dlq_alert(source, record_id, error_message, retry_count):
    msg = (
        f"🚨 *DLQ RECORD*\n"
        f"Source: `{source}` | Record: `{record_id}`\n"
        f"Error: `{error_message}`\n"
        f"Retry count: {retry_count}"
    )
    _send(msg)


def send_freshness_breach_alert(source, hours_stale, last_ingest_time):
    msg = (
        f"🕐 *SLA BREACH — DATA FRESHNESS*\n"
        f"Source: `{source}`\n"
        f"Last ingestion: `{last_ingest_time}`\n"
        f"Hours stale: `{hours_stale:.1f}` hours"
    )
    _send(msg)


def send_daily_summary(total_runs, success_rate, total_records, dlq_count):
    msg = (
        f"📊 *DAILY PIPELINE SUMMARY*\n"
        f"Total runs: {total_runs} | "
        f"Success rate: {success_rate:.1%}\n"
        f"Total records: {total_records:,} | "
        f"DLQ count: {dlq_count}"
    )
    _send(msg)