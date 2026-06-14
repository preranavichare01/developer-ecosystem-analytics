with source as (
    select
        FRAMEWORK_NAME,
        WEEK_START,
        STARS_TOTAL,
        FORKS_TOTAL,
        OPEN_ISSUES,
        COMMITS_30D,
        PRIMARY_LANGUAGE,
        ISSUES_OPENED_7D,
        ISSUES_CLOSED_7D,
        ISSUE_RESOLUTION_RATE,
        PRS_MERGED_7D,
        PR_MERGE_RATE,
        RELEASES_30D,
        PROCESSED_AT
    from DEV_ECOSYSTEM_DB.SILVER.FACT_GITHUB_WEEKLY
    where FRAMEWORK_NAME != 'unknown'
)

select * from source