"""
schema_validator.py
-------------------
Validates incoming API response against expected schema.
Filters out unexpected fields before writing to Bronze.

WHY THIS EXISTS:
APIs change without warning. A new field appearing in GitHub API
response is harmless. A field disappearing means your Silver
transformation will fail silently with NULLs everywhere.

This validator catches both cases and handles them differently:
- Added fields   → log and continue (safe)
- Removed fields → alert immediately (dangerous)

INTERVIEW Q: How do you handle API schema changes gracefully?
A: Schema contracts. Define expected fields in config. Validate
   every response before writing. Route drift to an error table
   for human review. Never let schema changes silently corrupt
   downstream models. This is what dbt schema tests do at the
   transformation layer — we do it at ingestion.
"""

import yaml
import json
import logging
from datetime import datetime, timezone

log = logging.getLogger(__name__)


def load_expected_schema(source: str) -> dict:
    """Loads expected schema for a source from YAML config."""
    with open('config/expected_schemas.yaml', 'r') as f:
        schemas = yaml.safe_load(f)
    return schemas.get(source, {})


def validate_and_filter(data_dict: dict, source: str, conn) -> dict:
    """
    Validates data_dict against expected schema for source.

    Steps:
    1. Load expected fields from config/expected_schemas.yaml
    2. Find added and removed fields vs actual response
    3. If required fields removed → log drift to Snowflake + Slack alert
    4. If fields added → log silently, continue
    5. Return filtered dict with only expected fields

    Args:
        data_dict: Raw API response as dict
        source:    'github', 'reddit', or 'pypi'
        conn:      Active Snowflake connection

    Returns:
        Filtered dict containing only expected fields
    """
    from ingestion.utils.slack_alerts import send_schema_drift_alert
    from ingestion.utils.snowflake_utils import execute_query

    schema = load_expected_schema(source)
    if not schema:
        log.warning(
            f"No schema defined for source: {source}. "
            f"Passing data through unfiltered."
        )
        return data_dict

    required_fields = set(schema.get('required_fields', []))
    optional_fields = set(schema.get('optional_fields', []))
    all_expected = required_fields | optional_fields
    actual_fields = set(data_dict.keys())

    added_fields = actual_fields - all_expected
    removed_fields = required_fields - actual_fields

    # Handle removed required fields — this is dangerous
    if removed_fields:
        log.error(
            f"[{source}] SCHEMA DRIFT — Required fields REMOVED: "
            f"{removed_fields}"
        )

        # Log to Snowflake ERROR.SCHEMA_DRIFT_LOG
        execute_query(
            conn,
            """
            INSERT INTO DEV_ECOSYSTEM_DB.ERROR.SCHEMA_DRIFT_LOG
            (SOURCE, ADDED_FIELDS, REMOVED_FIELDS, CHANGED_TYPES, ALERT_SENT)
            SELECT %s, PARSE_JSON(%s), PARSE_JSON(%s), PARSE_JSON(%s), TRUE
            """,
            (
                source,
                json.dumps(list(added_fields)),
                json.dumps(list(removed_fields)),
                json.dumps([])
            )
        )
        conn.commit()

        # Send Slack alert
        send_schema_drift_alert(
            source,
            list(removed_fields),
            list(added_fields)
        )

    elif added_fields:
        # New fields added — safe, just log it
        log.info(
            f"[{source}] Schema note — new fields in API response "
            f"(filtered out): {added_fields}"
        )

    # Return only expected fields that actually exist in response
    filtered = {
        k: v for k, v in data_dict.items()
        if k in all_expected
    }

    return filtered