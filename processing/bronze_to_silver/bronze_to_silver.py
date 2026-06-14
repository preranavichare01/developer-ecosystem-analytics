import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

import re
import logging
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv('config/.env')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s'
)
log = logging.getLogger(__name__)

# ── Snowflake helpers ──────────────────────────────────────────
def get_conn():
    return snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        role=os.getenv('SNOWFLAKE_ROLE')
    )

def read_sf(query: str) -> pd.DataFrame:
    conn   = get_conn()
    cursor = conn.cursor()
    cursor.execute(query)
    cols = [desc[0].upper() for desc in cursor.description]
    rows = cursor.fetchall()
    conn.close()
    return pd.DataFrame(rows, columns=cols)

def write_sf(df: pd.DataFrame, table: str):
    conn   = get_conn()
    schema = table.split(".")[-2]
    tbl    = table.split(".")[-1]
    success, _, nrows, _ = write_pandas(
        conn, df, tbl,
        schema=schema,
        database="DEV_ECOSYSTEM_DB",
        auto_create_table=False,
        overwrite=False
    )
    conn.close()
    log.info(f"  ✅ Written {nrows} rows → {table}")

# ── Name maps ──────────────────────────────────────────────────
REPO_TO_FRAMEWORK = {
    "tiangolo/fastapi":                      "fastapi",
    "pallets/flask":                         "flask",
    "django/django":                         "django",
    "encode/httpx":                          "httpx",
    "pydantic/pydantic":                     "pydantic",
    "pandas-dev/pandas":                     "pandas",
    "numpy/numpy":                           "numpy",
    "scikit-learn/scikit-learn":             "scikit-learn",
    "pytorch/pytorch":                       "pytorch",
    "tensorflow/tensorflow":                 "tensorflow",
    "apache/airflow":                        "airflow",
    "dbt-labs/dbt-core":                     "dbt",
    "great-expectations/great_expectations": "great-expectations",
    "streamlit/streamlit":                   "streamlit",
    "apache/spark":                          "spark",
}

PYPI_TO_FRAMEWORK = {
    "fastapi":            "fastapi",
    "flask":              "flask",
    "django":             "django",
    "httpx":              "httpx",
    "pydantic":           "pydantic",
    "pandas":             "pandas",
    "numpy":              "numpy",
    "scikit-learn":       "scikit-learn",
    "torch":              "pytorch",
    "tensorflow":         "tensorflow",
    "apache-airflow":     "airflow",
    "dbt-core":           "dbt",
    "great-expectations": "great-expectations",
    "streamlit":          "streamlit",
    "pyspark":            "spark",
}


# ══════════════════════════════════════════════════════════════
# TRANSFORM 1 — GITHUB
# ══════════════════════════════════════════════════════════════
def transform_github():
    log.info("── GitHub Bronze → Silver ──")

    repos = read_sf("""
        SELECT
            REPO_FULL_NAME,
            INGESTION_DATE,
            RAW_DATA:stargazers_count::INT       AS STARS_TOTAL,
            RAW_DATA:forks_count::INT            AS FORKS_TOTAL,
            RAW_DATA:open_issues_count::INT      AS OPEN_ISSUES,
            RAW_DATA:language::VARCHAR           AS PRIMARY_LANGUAGE,
            RAW_DATA:_commit_count_last_30d::INT AS COMMITS_30D
        FROM DEV_ECOSYSTEM_DB.BRONZE.GITHUB_REPOS_RAW
    """)

    extended = read_sf("""
        SELECT
            REPO_FULL_NAME,
            ISSUES_OPENED_7D,
            ISSUES_CLOSED_7D,
            ISSUE_RESOLUTION_RATE,
            PRS_MERGED_7D,
            PR_MERGE_RATE,
            RELEASES_LAST_30D
        FROM DEV_ECOSYSTEM_DB.BRONZE.GITHUB_EXTENDED_RAW
    """)

    log.info(f"  Repos rows    : {len(repos)}")
    log.info(f"  Extended rows : {len(extended)}")

    repos["FRAMEWORK_NAME"]   = repos["REPO_FULL_NAME"].map(REPO_TO_FRAMEWORK).fillna("unknown")
    repos["PRIMARY_LANGUAGE"] = repos["PRIMARY_LANGUAGE"].fillna("Unknown")
    repos["COMMITS_30D"]      = repos["COMMITS_30D"].fillna(0).astype(int)
    repos["STARS_TOTAL"]      = repos["STARS_TOTAL"].fillna(0).astype(int)
    repos["FORKS_TOTAL"]      = repos["FORKS_TOTAL"].fillna(0).astype(int)
    repos["OPEN_ISSUES"]      = repos["OPEN_ISSUES"].fillna(0).astype(int)

    df = repos.merge(extended, on="REPO_FULL_NAME", how="left")

    for c in ["ISSUES_OPENED_7D","ISSUES_CLOSED_7D",
               "PRS_MERGED_7D","RELEASES_LAST_30D"]:
        df[c] = df[c].fillna(0).astype(int)
    for c in ["ISSUE_RESOLUTION_RATE","PR_MERGE_RATE"]:
        df[c] = df[c].fillna(0.0)

    df = df.sort_values("INGESTION_DATE", ascending=False)
    df = df.drop_duplicates(
        subset=["FRAMEWORK_NAME","INGESTION_DATE"],
        keep="first"
    )
    df = df.rename(columns={"INGESTION_DATE": "WEEK_START"})

    silver = df[[
        "FRAMEWORK_NAME",
        "WEEK_START",
        "STARS_TOTAL",
        "FORKS_TOTAL",
        "OPEN_ISSUES",
        "COMMITS_30D",
        "PRIMARY_LANGUAGE",
        "ISSUES_OPENED_7D",
        "ISSUES_CLOSED_7D",
        "ISSUE_RESOLUTION_RATE",
        "PRS_MERGED_7D",
        "PR_MERGE_RATE",
        "RELEASES_LAST_30D"
    ]].copy()

    silver = silver.rename(
        columns={"RELEASES_LAST_30D": "RELEASES_30D"}
    )

    silver["PROCESSED_AT"] = datetime.now(timezone.utc)

    

    assert (silver["STARS_TOTAL"] < 0).sum() == 0, \
        "Negative stars found"

    log.info(f"  Silver rows   : {len(silver)}")
    write_sf(silver, "DEV_ECOSYSTEM_DB.SILVER.FACT_GITHUB_WEEKLY")


# ══════════════════════════════════════════════════════════════
# TRANSFORM 2 — HACKERNEWS
# ══════════════════════════════════════════════════════════════
def transform_hackernews():
    log.info("── HackerNews Bronze → Silver ──")

    hn = read_sf("""
        SELECT
            POST_ID,
            FRAMEWORK_MENTIONED,
            TITLE,
            SCORE,
            COMMENT_COUNT,
            WEEK_START
        FROM DEV_ECOSYSTEM_DB.BRONZE.HACKERNEWS_POSTS_RAW
        WHERE SCORE IS NOT NULL
    """)

    log.info(f"  Raw rows      : {len(hn)}")

    # Remove spam
    hn = hn[hn["SCORE"] >= 2]
    log.info(f"  After spam    : {len(hn)}")

    # Exact word boundary match
    def exact_match(row):
        pattern = r'\b' + re.escape(
            str(row["FRAMEWORK_MENTIONED"]).lower()
        ) + r'\b'
        return bool(re.search(pattern, str(row["TITLE"]).lower()))

    hn = hn[hn.apply(exact_match, axis=1)]
    log.info(f"  After filter  : {len(hn)}")

    # Aggregate to weekly grain
    silver = (
        hn.groupby(["FRAMEWORK_MENTIONED","WEEK_START"])
        .agg(
            STORY_COUNT    = ("POST_ID",       "count"),
            AVG_SCORE      = ("SCORE",         "mean"),
            AVG_COMMENTS   = ("COMMENT_COUNT", "mean"),
            MAX_SCORE      = ("SCORE",         "max"),
            TOTAL_COMMENTS = ("COMMENT_COUNT", "sum")
        )
        .reset_index()
        .rename(columns={"FRAMEWORK_MENTIONED": "FRAMEWORK_NAME"})
    )

    silver["AVG_SCORE"]       = silver["AVG_SCORE"].round(2)
    silver["AVG_COMMENTS"]    = silver["AVG_COMMENTS"].round(2)
    silver["SENTIMENT_SCORE"] = (
        silver["AVG_SCORE"] / 500.0
    ).clip(upper=1.0).round(4)
    silver["PROCESSED_AT"]    = datetime.now(timezone.utc)

    log.info(f"  Silver rows   : {len(silver)}")
    write_sf(silver, "DEV_ECOSYSTEM_DB.SILVER.FACT_HN_WEEKLY")


# ══════════════════════════════════════════════════════════════
# TRANSFORM 3 — PYPI
# ══════════════════════════════════════════════════════════════
def transform_pypi():
    log.info("── PyPI Bronze → Silver ──")

    pypi = read_sf("""
        SELECT
            PACKAGE_NAME,
            WEEKLY_DOWNLOADS,
            DOWNLOAD_GROWTH_PCT,
            WEEK_START
        FROM DEV_ECOSYSTEM_DB.BRONZE.PYPI_DOWNLOADS_RAW
        WHERE WEEKLY_DOWNLOADS IS NOT NULL
          AND WEEKLY_DOWNLOADS > 0
    """)

    log.info(f"  Raw rows      : {len(pypi)}")

    pypi["DOWNLOAD_GROWTH_PCT"] = pypi["DOWNLOAD_GROWTH_PCT"].clip(
        lower=-100, upper=500
    )
    pypi["FRAMEWORK_NAME"] = pypi["PACKAGE_NAME"].map(
        PYPI_TO_FRAMEWORK
    ).fillna("unknown")

    silver = pypi[[
        "FRAMEWORK_NAME","WEEK_START",
        "WEEKLY_DOWNLOADS","DOWNLOAD_GROWTH_PCT"
    ]].copy()

    silver["PROCESSED_AT"] = datetime.now(timezone.utc)

    assert (silver["WEEKLY_DOWNLOADS"] < 0).sum() == 0, \
        "Negative downloads found"

    log.info(f"  Silver rows   : {len(silver)}")
    write_sf(silver, "DEV_ECOSYSTEM_DB.SILVER.FACT_PYPI_WEEKLY")


# ══════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════
def run_bronze_to_silver():
    log.info("=" * 55)
    log.info("Bronze → Silver Pipeline Started")
    log.info(f"Time: {datetime.now(timezone.utc).isoformat()}")
    log.info("=" * 55)

    try:
        transform_github()
        transform_hackernews()
        transform_pypi()

        log.info("=" * 55)
        log.info("Bronze → Silver Complete ✅")
        log.info("=" * 55)

    except AssertionError as e:
        log.error(f"DATA ASSERTION FAILED: {e}")
        raise

    except Exception as e:
        log.error(f"Pipeline failed: {e}")
        raise


if __name__ == '__main__':
    run_bronze_to_silver()