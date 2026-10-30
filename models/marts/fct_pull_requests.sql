-- One row per PR created on or after the window start. Open PRs are kept (is_open) with
-- NULL merge/close durations; they are never dropped or given a fake close date.
-- Reviews count toward first_review_at only if they are not by the PR author or a bot.
with run as (
    select * from {{ ref('dim_run') }}
),

prs as (
    select p.*
    from {{ ref('stg_pull_requests') }} p
    cross join run
    where p.created_at >= run.window_start
),

human_reviews as (
    select
        r.pr_key,
        min(r.submitted_at)               as first_review_at,
        bool_or(r.state = 'APPROVED')     as has_approving_review,
        count(*)                          as review_count
    from {{ ref('stg_pr_reviews') }} r
    join prs p using (pr_key)
    where not r.is_bot
      and r.author_login is distinct from p.author_login
    group by r.pr_key
)

select
    p.repo,
    p.number,
    p.pr_key,
    p.author_login,
    p.is_bot,
    p.state,
    p.is_draft,
    p.created_at,
    p.updated_at,
    p.closed_at,
    p.merged_at,
    p.merged_at is not null                                         as is_merged,
    p.state = 'closed' and p.merged_at is null                      as is_closed_unmerged,
    p.state = 'open'                                                as is_open,
    hr.first_review_at,
    date_diff('second', p.created_at, hr.first_review_at) / 3600.0  as hours_to_first_review,
    coalesce(date_diff('second', p.created_at, hr.first_review_at) / 3600.0 <= 168, false)
                                                                    as reviewed_within_7d,
    date_diff('second', p.created_at, p.merged_at) / 3600.0         as hours_to_merge,
    date_diff('second', p.created_at, p.closed_at) / 3600.0         as hours_to_close,
    p.state = 'open' and p.updated_at < run.run_time - interval 180 day
                                                                    as is_stale,
    coalesce(hr.has_approving_review, false)                        as has_approving_review,
    coalesce(hr.review_count, 0)                                    as human_review_count
from prs p
cross join run
left join human_reviews hr using (pr_key)
