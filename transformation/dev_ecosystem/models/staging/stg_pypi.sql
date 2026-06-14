with source as (
    select
        FRAMEWORK_NAME,
        WEEK_START,
        WEEKLY_DOWNLOADS,
        DOWNLOAD_GROWTH_PCT,
        PROCESSED_AT
    from DEV_ECOSYSTEM_DB.SILVER.FACT_PYPI_WEEKLY
    where FRAMEWORK_NAME != 'unknown'
)

select * from source