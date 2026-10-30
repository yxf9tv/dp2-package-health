-- One row: the run time and the analysis window shared by every mart.
-- Run time = max(fetched_at) over the raw data, so a rebuild without new data gives
-- the same results. window_start comes from the dbt var (Task 3 passes DEV_START_DATE).
with fetched as (
    select fetched_at from {{ ref('stg_commits') }}
    union all select fetched_at from {{ ref('stg_pull_requests') }}
    union all select fetched_at from {{ ref('stg_pr_reviews') }}
    union all select fetched_at from {{ ref('stg_releases') }}
)

select
    max(fetched_at)                                              as run_time,
    '{{ var("window_start", "2024-01-01") }}'::timestamptz       as window_start,
    date_diff('second', '{{ var("window_start", "2024-01-01") }}'::timestamptz,
              max(fetched_at)) / 86400.0                         as window_days
from fetched
