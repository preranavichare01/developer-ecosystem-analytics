with metrics as (
    select * from {{ ref('fact_framework_weekly_metrics') }}
),

week_stats as (
    select
        WEEK_START,
        count(distinct FRAMEWORK_NAME)  as TOTAL_FRAMEWORKS,
        round(avg(HEALTH_INDEX), 4)     as AVG_HEALTH_INDEX
    from metrics
    group by WEEK_START
),

top_popularity as (
    select distinct
        WEEK_START,
        first_value(FRAMEWORK_NAME) over (
            partition by WEEK_START
            order by POPULARITY_SCORE desc
        ) as TOP_FRAMEWORK
    from metrics
),

top_growth as (
    select distinct
        WEEK_START,
        first_value(FRAMEWORK_NAME) over (
            partition by WEEK_START
            order by DOWNLOAD_GROWTH_PCT desc
        ) as FASTEST_GROWING
    from metrics
),

top_hn as (
    select distinct
        WEEK_START,
        first_value(FRAMEWORK_NAME) over (
            partition by WEEK_START
            order by HN_STORY_COUNT desc
        ) as MOST_DISCUSSED_HN
    from metrics
),

top_downloads as (
    select distinct
        WEEK_START,
        first_value(FRAMEWORK_NAME) over (
            partition by WEEK_START
            order by WEEKLY_DOWNLOADS desc
        ) as HIGHEST_DOWNLOADS
    from metrics
)

select
    w.WEEK_START,
    w.TOTAL_FRAMEWORKS,
    w.AVG_HEALTH_INDEX,
    p.TOP_FRAMEWORK,
    g.FASTEST_GROWING,
    h.MOST_DISCUSSED_HN,
    d.HIGHEST_DOWNLOADS,
    current_timestamp() as PROCESSED_AT
from week_stats w
left join top_popularity p on w.WEEK_START = p.WEEK_START
left join top_growth     g on w.WEEK_START = g.WEEK_START
left join top_hn         h on w.WEEK_START = h.WEEK_START
left join top_downloads  d on w.WEEK_START = d.WEEK_START