-- Repo x month, from the window-start month through the run month. The month spine is
-- crossed with the repos, so a month with no activity shows 0 instead of disappearing.
-- Each event counts in the month it happened. PR and commit measures use human
-- activity only (bot-authored PRs and bot commits are excluded); see schema.yml.
with run as (
    select * from {{ ref('dim_run') }}
),

months as (
    select unnest(generate_series(date_trunc('month', run.window_start),
                                  date_trunc('month', run.run_time),
                                  interval 1 month))::date        as month
    from run
),

repos as (
    select distinct repo from {{ ref('stg_pull_requests') }}
    union select distinct repo from {{ ref('stg_commits') }}
),

spine as (
    select r.repo, m.month from repos r cross join months m
),

prs as (
    select * from {{ ref('fct_pull_requests') }} where not is_bot
),

opened as (
    select repo, date_trunc('month', created_at)::date as month,
           count(*)                                     as prs_opened,
           median(hours_to_first_review)                as median_first_review_hours,
           count(distinct author_login)                 as active_contributors
    from prs group by all
),

merged as (
    select repo, date_trunc('month', merged_at)::date as month,
           count(*)                                    as prs_merged,
           median(hours_to_merge)                      as median_merge_hours
    from prs where is_merged group by all
),

closed_unmerged as (
    select repo, date_trunc('month', closed_at)::date as month,
           count(*)                                    as prs_closed_unmerged
    from prs where is_closed_unmerged group by all
),

commits as (
    select c.repo, date_trunc('month', c.committed_at)::date as month, count(*) as commits
    from {{ ref('stg_commits') }} c
    cross join run
    where not c.is_bot and c.committed_at >= run.window_start
    group by all
)

select
    s.repo,
    s.month,
    date_diff('month', date_trunc('month', run.window_start)::date, s.month) as month_index,
    coalesce(o.prs_opened, 0)                                    as prs_opened,
    coalesce(m.prs_merged, 0)                                    as prs_merged,
    coalesce(cu.prs_closed_unmerged, 0)                          as prs_closed_unmerged,
    coalesce(m.prs_merged, 0)
        / nullif(coalesce(m.prs_merged, 0) + coalesce(cu.prs_closed_unmerged, 0), 0)
                                                                 as merge_ratio,
    o.median_first_review_hours,
    m.median_merge_hours,
    coalesce(c.commits, 0)                                       as commits,
    coalesce(o.active_contributors, 0)                           as active_contributors,
    s.month = date_trunc('month', run.run_time)::date            as is_partial_month
from spine s
cross join run
left join opened o using (repo, month)
left join merged m using (repo, month)
left join closed_unmerged cu using (repo, month)
left join commits c using (repo, month)
