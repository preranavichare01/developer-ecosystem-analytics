"""
snowflake_utils.py
------------------
Central Snowflake utility module.
All ingesters import from here — single place to change connection logic.

WHY THIS EXISTS:
Junior engineers copy-paste connection code in every file.
When credentials change or you add connection pooling,
you change 10 files instead of 1. This is the DRY principle applied
to infrastructure code. At Atlassian scale this would be a
shared internal library published to your private PyPI.
"""

import os
import logging
from datetime import datetime, timezone
import snowflake.connector
from dotenv import load_dotenv

load_dotenv('config/.env')
log = logging.getLogger(__name__)


def get_snowflake_connection():
    """
    Returns a Snowflake connection using env vars.
    
    INTERVIEW Q: How do you manage database connections in a pipeline?
    A: Centralize connection creation in one utility. In production
       you'd add connection pooling (SQLAlchemy) or use Airflow's
       SnowflakeHook which manages the lifecycle automatically.
    """
    return snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        role=os.getenv('SNOWFLAKE_ROLE')
    )


def execute_query(conn, sql, params=None):
    """
    Safe query wrapper — always returns results, never crashes caller.
    
    WHY: Raw cursor.execute() raises exceptions that crash pipelines.
    This wrapper catches, logs, and returns empty list so caller
    can decide what to do. Separation of error detection from
    error handling — a key production pattern.
    """
    try:
        cursor = conn.cursor()
        cursor.execute(sql, params or ())
        results = cursor.fetchall()
        cursor.close()
        return results
    except Exception as e:
        log.error(f"Query failed: {e} | SQL: {sql[:100]}")
        return []


def bulk_insert(conn, table, records):
    """
    Efficient bulk insert using executemany.
    
    WHY NOT individual INSERTs:
    15 repos × 1 INSERT each = 15 round trips to Snowflake.
    executemany = 1 round trip. At 10,000 rows the difference
    is 3 seconds vs 45 seconds.
    
    INTERVIEW Q: How do you optimise write performance to Snowflake?
    A: Use COPY INTO from S3/stage for millions of rows.
       For thousands, executemany. Never row-by-row INSERT in a loop.
    """
    if not records:
        log.warning(f"bulk_insert called with empty records for {table}")
        return 0

    try:
        cursor = conn.cursor()
        placeholders = ','.join(['%s'] * len(records[0]))
        sql = f"INSERT INTO {table} VALUES ({placeholders})"
        cursor.executemany(sql, records)
        conn.commit()
        cursor.close()
        log.info(f"Bulk inserted {len(records)} rows into {table}")
        return len(records)
    except Exception as e:
        log.error(f"Bulk insert failed for {table}: {e}")
        return 0


def get_last_ingested_at(conn, source):
    """
    Returns the timestamp of last successful ingestion for a source.
    Used by incremental logic — fetch only records updated after this.
    
    Returns None if no previous successful run exists (first run).
    
    INTERVIEW Q: How do you implement incremental loads?
    A: Store the high-water mark (last successful run timestamp)
       in a metadata table. On next run, fetch only records where
       updated_at > high_water_mark. This is exactly what
       Fivetran and Airbyte do internally.
    """
    results = execute_query(
        conn,
        """
        SELECT MAX(COMPLETED_AT)
        FROM DEV_ECOSYSTEM_DB.MONITORING.INGESTION_LOG
        WHERE SOURCE = %s
        AND STATUS = 'SUCCESS'
        """,
        (source,)
    )
    if results and results[0][0]:
        return results[0][0]
    return None

def log_run_start(conn, run_id, source):
    """Inserts RUNNING status at pipeline start."""
    started_at = datetime.now(timezone.utc).isoformat()
    execute_query(
        conn,
        """
        INSERT INTO DEV_ECOSYSTEM_DB.MONITORING.INGESTION_LOG
        (RUN_ID, SOURCE, STARTED_AT, RECORDS_FETCHED,
         RECORDS_WRITTEN, RECORDS_FAILED, RECORDS_IN_DLQ, STATUS)
        SELECT %s, %s, %s, 0, 0, 0, 0, 'RUNNING'
        """,
        (run_id, source, started_at)
    )
    conn.commit()


def log_run_complete(conn, run_id, source, fetched,
                     written, failed, dlq, status):
    """Updates run log with final counts and status."""
    completed_at = datetime.now(timezone.utc).isoformat()
    execute_query(
        conn,
        """
        UPDATE DEV_ECOSYSTEM_DB.MONITORING.INGESTION_LOG
        SET COMPLETED_AT     = %s,
            RECORDS_FETCHED  = %s,
            RECORDS_WRITTEN  = %s,
            RECORDS_FAILED   = %s,
            RECORDS_IN_DLQ   = %s,
            STATUS           = %s
        WHERE RUN_ID = %s AND SOURCE = %s
        """,
        (completed_at, fetched, written,
         failed, dlq, status, run_id, source)
    )
    conn.commit()

