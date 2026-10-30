-- One row per repo: candidate health metrics for Task 5. Every column is a rate, share,
-- median or slope, never a raw count. PR metrics use human-authored PRs only; commit
-- metrics use human commits only. Task 5 chooses which of these it scores.
with run as (
    select * from {{ ref('dim_run') }}
),

repos as (
    select distinct repo from {{ ref('mart_repo_monthly') }}
),

prs as (
    select * from {{ ref('fct_pull_requests') }} where not is_bot
),

pr_metrics as (
    select
        p.repo,
        -- only PRs old enough to have had 7 days for a review
        avg(p.reviewed_within_7d::int) filter (
            where p.created_at <= run.run_time - interval 7 day)  as pct_reviewed_within_7d,
        median(p.hours_to_first_review)                          as median_hours_to_first_review,
        count(*) filter (where p.is_merged)
            / nullif(count(*) filter (where p.is_merged or p.is_closed_unmerged), 0)
                                                                 as merge_ratio,
        median(p.hours_to_merge)                                 as median_hours_to_merge,
        -- review quality: merged PRs that had a non-author, non-bot approval
        avg(p.has_approving_review::int) filter (where p.is_merged) as approving_review_share,
        -- rubric definition: share of all open PRs with no update in 180+ days
        count(*) filter (where p.is_stale)
            / nullif(count(*) filter (where p.is_open), 0)       as stale_open_share
    from prs p
    cross join run
    group by p.repo
),

monthly as (
    -- complete months only: the partial run month would drag the trend down
    select
        repo,
        regr_slope(commits, month_index)                           as commit_trend,
        regr_slope(commits, month_index) / nullif(avg(commits), 0) as commit_trend_pct,
        -- consistency: steady monthly activity scores better than bursts
        stddev_samp(commits) / nullif(avg(commits), 0)             as commit_cv,
        avg(active_contributors)                                   as avg_monthly_active_contributors
    from {{ ref('mart_repo_monthly') }}
    where not is_partial_month
    group by repo
),

human_commits as (
    select c.*
    from {{ ref('stg_commits') }} c
    cross join run
    where not c.is_bot and c.committed_at >= run.window_start
),

commit_metrics as (
    select hc.repo, count(*) / (any_value(run.window_days) / 7.0) as commits_per_week
    from human_commits hc
    cross join run
    group by hc.repo
),

author_share as (
    -- share of linked human commits made by the single most active author (bus factor)
    select repo, max(n) / sum(n) as top_author_commit_share
    from (
        select repo, author_login, count(*) as n
        from human_commits
        where author_login is not null
        group by all
    )
    group by repo
),

reviewer_share as (
    -- share of review work by the single most active reviewer (bus factor for reviews).
    -- Each (PR, reviewer) pair counts once, so back-and-forth rounds do not inflate it.
    -- Human PRs only; non-bot reviewers who are not the PR author.
    select repo, max(n) / sum(n) as top_reviewer_share
    from (
        select p.repo, r.author_login, count(distinct r.pr_key) as n
        from {{ ref('stg_pr_reviews') }} r
        join prs p using (pr_key)
        where not r.is_bot
          and r.author_login is not null
          and r.author_login is distinct from p.author_login
        group by all
    )
    group by repo
),

release_metrics as (
    select
        f.repo,
        count(*) / (any_value(run.window_days) / 365.25)         as releases_per_year,
        stddev_samp(f.days_since_previous)
            / nullif(avg(f.days_since_previous), 0)              as release_gap_cv
    from {{ ref('fct_releases') }} f
    cross join run
    group by f.repo
)

select
    r.repo,
    pm.pct_reviewed_within_7d,
    pm.median_hours_to_first_review,
    pm.merge_ratio,
    pm.median_hours_to_merge,
    pm.approving_review_share,
    pm.stale_open_share,
    cm.commits_per_week,
    m.commit_trend,
    m.commit_trend_pct,
    m.commit_cv,
    m.avg_monthly_active_contributors,
    a.top_author_commit_share,
    rs.top_reviewer_share,
    coalesce(rm.releases_per_year, 0)                            as releases_per_year,
    rm.release_gap_cv
from repos r
left join pr_metrics pm using (repo)
left join monthly m using (repo)
left join commit_metrics cm using (repo)
left join author_share a using (repo)
left join reviewer_share rs using (repo)
left join release_metrics rm using (repo)
