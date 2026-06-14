with base as (
    select * from {{ ref('int_framework_combined') }}
),

-- Min-max normalisation per metric
-- Normalise each signal to 0-1 range
-- so no single metric dominates the composite score
normalised as (
    select
        FRAMEWORK_NAME,
        WEEK_START,
        PRIMARY_LANGUAGE,

        -- Raw signals
        STARS_TOTAL,
        FORKS_TOTAL,
        OPEN_ISSUES,
        COMMITS_30D,
        ISSUE_RESOLUTION_RATE,
        PR_MERGE_RATE,
        RELEASES_30D,
        HN_STORY_COUNT,
        HN_AVG_SCORE,
        HN_SENTIMENT_SCORE,
        WEEKLY_DOWNLOADS,
        DOWNLOAD_GROWTH_PCT,

        -- Normalised signals (0 to 1)
        case
            when max(STARS_TOTAL) over () = 0 then 0
            else STARS_TOTAL / nullif(max(STARS_TOTAL) over (), 0)
        end as stars_norm,

        case
            when max(COMMITS_30D) over () = 0 then 0
            else COMMITS_30D / nullif(max(COMMITS_30D) over (), 0)
        end as commits_norm,

        case
            when max(WEEKLY_DOWNLOADS) over () = 0 then 0
            else WEEKLY_DOWNLOADS / nullif(max(WEEKLY_DOWNLOADS) over (), 0)
        end as downloads_norm,

        case
            when max(HN_STORY_COUNT) over () = 0 then 0
            else HN_STORY_COUNT / nullif(max(HN_STORY_COUNT) over (), 0)
        end as hn_story_norm,

        -- PR merge rate and issue resolution already 0-1
        PR_MERGE_RATE       as pr_merge_norm,
        ISSUE_RESOLUTION_RATE as issue_res_norm,
        HN_SENTIMENT_SCORE  as hn_sentiment_norm

    from base
),

scored as (
    select
        *,

        -- Popularity Score (0-1)
        -- What signals: how widely adopted is this framework?
        round(
            (stars_norm    * 0.35) +
            (downloads_norm * 0.25) +
            (commits_norm  * 0.20) +
            (hn_story_norm * 0.20),
        4) as POPULARITY_SCORE,

        -- Sentiment Score (0-1)
        -- What signals: how happy are developers using this?
        round(
            (hn_sentiment_norm * 0.50) +
            (issue_res_norm    * 0.30) +
            (pr_merge_norm     * 0.20),
        4) as SENTIMENT_SCORE

    from normalised
),

final as (
    select
        FRAMEWORK_NAME,
        WEEK_START,
        PRIMARY_LANGUAGE,
        STARS_TOTAL,
        FORKS_TOTAL,
        OPEN_ISSUES,
        COMMITS_30D,
        ISSUE_RESOLUTION_RATE,
        PR_MERGE_RATE,
        RELEASES_30D,
        HN_STORY_COUNT,
        HN_AVG_SCORE,
        HN_SENTIMENT_SCORE,
        WEEKLY_DOWNLOADS,
        DOWNLOAD_GROWTH_PCT,
        POPULARITY_SCORE,
        SENTIMENT_SCORE,

        -- Health Index (0-1)
        -- What signals: overall ecosystem health
        round(
            (POPULARITY_SCORE  * 0.40) +
            (SENTIMENT_SCORE   * 0.30) +
            (commits_norm      * 0.20) +
            ((1 - least(OPEN_ISSUES / nullif(
                max(OPEN_ISSUES) over (), 0), 1)) * 0.10),
        4) as HEALTH_INDEX,

        current_timestamp() as PROCESSED_AT

    from scored
)

select * from final