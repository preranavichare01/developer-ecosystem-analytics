{% snapshot framework_popularity_snapshot %}

{{
    config(
        target_schema='GOLD',
        unique_key='FRAMEWORK_NAME',
        strategy='check',
        check_cols=[
            'POPULARITY_SCORE',
            'HEALTH_INDEX',
            'SENTIMENT_SCORE'
        ]
    )
}}

select
    FRAMEWORK_NAME,
    WEEK_START,
    POPULARITY_SCORE,
    SENTIMENT_SCORE,
    HEALTH_INDEX,
    WEEKLY_DOWNLOADS,
    STARS_TOTAL
from {{ ref('fact_framework_weekly_metrics') }}

{% endsnapshot %}