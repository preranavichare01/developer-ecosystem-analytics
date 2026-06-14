with source as (
    select
        FRAMEWORK_NAME,
        WEEK_START,
        STORY_COUNT,
        AVG_SCORE,
        AVG_COMMENTS,
        MAX_SCORE,
        TOTAL_COMMENTS,
        SENTIMENT_SCORE,
        PROCESSED_AT
    from DEV_ECOSYSTEM_DB.SILVER.FACT_HN_WEEKLY
    where FRAMEWORK_NAME != 'unknown'
)

select * from source