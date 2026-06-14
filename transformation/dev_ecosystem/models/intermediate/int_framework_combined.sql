with github as (
    select * from {{ ref('stg_github') }}
),

hackernews as (
    select * from {{ ref('stg_hackernews') }}
),

pypi as (
    select * from {{ ref('stg_pypi') }}
),

combined as (
    select
        g.FRAMEWORK_NAME,
        g.WEEK_START,

        -- GitHub signals
        g.STARS_TOTAL,
        g.FORKS_TOTAL,
        g.OPEN_ISSUES,
        g.COMMITS_30D,
        g.PRIMARY_LANGUAGE,
        g.ISSUES_OPENED_7D,
        g.ISSUES_CLOSED_7D,
        g.ISSUE_RESOLUTION_RATE,
        g.PRS_MERGED_7D,
        g.PR_MERGE_RATE,
        g.RELEASES_30D,

        -- HackerNews signals (null if no HN data for this framework)
        coalesce(h.STORY_COUNT, 0)      as HN_STORY_COUNT,
        coalesce(h.AVG_SCORE, 0)        as HN_AVG_SCORE,
        coalesce(h.AVG_COMMENTS, 0)     as HN_AVG_COMMENTS,
        coalesce(h.SENTIMENT_SCORE, 0)  as HN_SENTIMENT_SCORE,

        -- PyPI signals (null if no PyPI data)
        coalesce(p.WEEKLY_DOWNLOADS, 0)     as WEEKLY_DOWNLOADS,
        coalesce(p.DOWNLOAD_GROWTH_PCT, 0)  as DOWNLOAD_GROWTH_PCT

    from github g
    left join hackernews h
        on g.FRAMEWORK_NAME = h.FRAMEWORK_NAME
    left join pypi p
        on g.FRAMEWORK_NAME = p.FRAMEWORK_NAME
)

select * from combined