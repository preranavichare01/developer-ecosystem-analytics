"""
anomaly_detector.py
-------------------
Detects row count anomalies after each ingestion run.

WHY THIS MATTERS:
A pipeline can succeed technically (no exceptions, exit code 0)
but still produce wrong data — half the repos missing, API
returned empty pages silently. Row count anomaly detection
catches silent failures that exceptions never would.

INTERVIEW Q: How do you detect data quality issues that don't
             raise exceptions?
A: Statistical process control on row counts. Compare current
   run to rolling 7-day average. Deviations beyond a threshold
   trigger alerts. This is how Netflix and Uber catch silent
   data loss in production.
"""

import logging

log = logging.getLogger(__name__)


def check_row_count_anomaly(source, current_count, conn):
    """
    Compares current ingestion count to 7-day rolling average.
    
    Returns:
        dict with keys: status, severity, message, avg_count
        
    Status values:
        NORMAL              — within expected range
        ANOMALY_LOW         — significantly fewer rows than usual
        ANOMALY_HIGH        — significantly more rows than usual
        INSUFFICIENT_HISTORY — fewer than 3 runs, cannot compare
    """
    from ingestion.utils.snowflake_utils import execute_query

    results = execute_query(
        conn,
        """
        SELECT RECORDS_WRITTEN
        FROM DEV_ECOSYSTEM_DB.MONITORING.INGESTION_LOG
        WHERE SOURCE = %s
          AND STATUS = 'SUCCESS'
          AND COMPLETED_AT >= DATEADD(day, -7, CURRENT_TIMESTAMP())
        ORDER BY COMPLETED_AT DESC
        LIMIT 7
        """,
        (source,)
    )

    if len(results) < 3:
        # Not enough history to make a meaningful comparison
        log.info(f"[{source}] Insufficient history for anomaly check "
                 f"({len(results)} runs). Skipping.")
        return {
            "status": "INSUFFICIENT_HISTORY",
            "severity": "LOW",
            "message": f"Only {len(results)} historical runs found",
            "avg_count": None
        }

    avg_count = sum(r[0] for r in results) / len(results)
    low_threshold  = avg_count * 0.70   # below 70% = anomaly
    high_threshold = avg_count * 2.00   # above 200% = anomaly

    if current_count < low_threshold:
        log.warning(
            f"[{source}] ANOMALY_LOW: {current_count} rows vs "
            f"avg {avg_count:.0f} (threshold {low_threshold:.0f})"
        )
        return {
            "status": "ANOMALY_LOW",
            "severity": "HIGH",
            "message": (f"Current {current_count} is below 70% of "
                        f"7-day avg {avg_count:.0f}"),
            "avg_count": avg_count
        }

    if current_count > high_threshold:
        log.warning(
            f"[{source}] ANOMALY_HIGH: {current_count} rows vs "
            f"avg {avg_count:.0f} (threshold {high_threshold:.0f})"
        )
        return {
            "status": "ANOMALY_HIGH",
            "severity": "MEDIUM",
            "message": (f"Current {current_count} is above 200% of "
                        f"7-day avg {avg_count:.0f}"),
            "avg_count": avg_count
        }

    log.info(
        f"[{source}] Row count NORMAL: {current_count} "
        f"(avg {avg_count:.0f})"
    )
    return {
        "status": "NORMAL",
        "severity": "NONE",
        "message": f"Within expected range",
        "avg_count": avg_count
    }