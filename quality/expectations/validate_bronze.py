import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

import great_expectations as gx
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
import logging

load_dotenv('config/.env')

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
log = logging.getLogger(__name__)

def get_conn():
    return snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        role=os.getenv('SNOWFLAKE_ROLE')
    )

def read_sf(query):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute(query)
    cols = [d[0].upper() for d in cursor.description]
    rows = cursor.fetchall()
    conn.close()
    return pd.DataFrame(rows, columns=cols)

def validate_github_bronze():
    log.info("── Validating BRONZE.GITHUB_REPOS_RAW ──")
    df = read_sf("""
        SELECT
            REPO_FULL_NAME,
            INGESTION_DATE,
            RAW_DATA:stargazers_count::INT AS STARS_TOTAL,
            RAW_DATA:forks_count::INT      AS FORKS_TOTAL,
            RAW_DATA:open_issues_count::INT AS OPEN_ISSUES
        FROM DEV_ECOSYSTEM_DB.BRONZE.GITHUB_REPOS_RAW
    """)

    context = gx.get_context()
    ds = context.sources.add_or_update_pandas("github_bronze")
    da = ds.add_dataframe_asset("github_repos")
    batch = da.build_batch_request(dataframe=df)
    suite = context.add_or_update_expectation_suite("github_bronze_suite")
    validator = context.get_validator(
        batch_request=batch,
        expectation_suite=suite
    )

    # Expectations
    validator.expect_column_values_to_not_be_null("REPO_FULL_NAME")
    validator.expect_column_values_to_not_be_null("INGESTION_DATE")
    validator.expect_column_values_to_not_be_null("STARS_TOTAL")
    validator.expect_column_values_to_be_between("STARS_TOTAL", min_value=0)
    validator.expect_column_values_to_be_between("FORKS_TOTAL", min_value=0)
    validator.expect_column_values_to_be_between("OPEN_ISSUES", min_value=0)
    validator.expect_table_row_count_to_be_between(min_value=1, max_value=10000)
    

    results = validator.validate()
    validator.save_expectation_suite()
    return results

def validate_hackernews_bronze():
    log.info("── Validating BRONZE.HACKERNEWS_POSTS_RAW ──")
    df = read_sf("""
        SELECT POST_ID, FRAMEWORK_MENTIONED, SCORE,
               COMMENT_COUNT, WEEK_START
        FROM DEV_ECOSYSTEM_DB.BRONZE.HACKERNEWS_POSTS_RAW
    """)

    context = gx.get_context()
    ds = context.sources.add_or_update_pandas("hn_bronze")
    da = ds.add_dataframe_asset("hn_posts")
    batch = da.build_batch_request(dataframe=df)
    suite = context.add_or_update_expectation_suite("hn_bronze_suite")
    validator = context.get_validator(
        batch_request=batch,
        expectation_suite=suite
    )

    validator.expect_column_values_to_not_be_null("POST_ID")
    validator.expect_column_values_to_not_be_null("FRAMEWORK_MENTIONED")
    validator.expect_column_values_to_be_between("SCORE", min_value=0)
    validator.expect_column_values_to_be_between("COMMENT_COUNT", min_value=0)
    validator.expect_table_row_count_to_be_between(min_value=100)

    results = validator.validate()
    validator.save_expectation_suite()
    return results

def validate_pypi_bronze():
    log.info("── Validating BRONZE.PYPI_DOWNLOADS_RAW ──")
    df = read_sf("""
        SELECT PACKAGE_NAME, WEEKLY_DOWNLOADS,
               DOWNLOAD_GROWTH_PCT, WEEK_START
        FROM DEV_ECOSYSTEM_DB.BRONZE.PYPI_DOWNLOADS_RAW
    """)

    context = gx.get_context()
    ds = context.sources.add_or_update_pandas("pypi_bronze")
    da = ds.add_dataframe_asset("pypi_downloads")
    batch = da.build_batch_request(dataframe=df)
    suite = context.add_or_update_expectation_suite("pypi_bronze_suite")
    validator = context.get_validator(
        batch_request=batch,
        expectation_suite=suite
    )

    validator.expect_column_values_to_not_be_null("PACKAGE_NAME")
    validator.expect_column_values_to_not_be_null("WEEKLY_DOWNLOADS")
    validator.expect_column_values_to_be_between("WEEKLY_DOWNLOADS", min_value=1)
    validator.expect_column_values_to_be_between(
        "DOWNLOAD_GROWTH_PCT", min_value=-100, max_value=500
    )
    validator.expect_table_row_count_to_be_between(min_value=1, max_value=1000)

    results = validator.validate()
    validator.save_expectation_suite()
    return results

def run_all_validations():
    log.info("=" * 55)
    log.info("Great Expectations Bronze Validation Started")
    log.info("=" * 55)

    results = {}
    all_passed = True

    for name, fn in [
        ("github",      validate_github_bronze),
        ("hackernews",  validate_hackernews_bronze),
        ("pypi",        validate_pypi_bronze),
    ]:
        try:
            result = fn()
            passed = result["success"]
            results[name] = passed
            status = " PASSED" if passed else " FAILED"
            log.info(f"  {name}: {status}")
            if not passed:
                all_passed = False
        except Exception as e:
            log.error(f"  {name}: ERROR — {e}")
            results[name] = False
            all_passed = False

    log.info("=" * 55)
    log.info(f"Overall: {' ALL PASSED' if all_passed else '❌ SOME FAILED'}")
    log.info("=" * 55)
    return all_passed

if __name__ == '__main__':
    success = run_all_validations()
    sys.exit(0 if success else 1)