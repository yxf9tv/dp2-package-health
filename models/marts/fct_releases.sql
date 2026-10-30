-- One row per published release on or after the window start. The lag runs over ALL
-- releases first, so the first release in the window still measures its gap to the
-- release before it (which may be older than the window).
with all_releases as (
    select
        repo,
        release_id,
        tag_name,
        published_at,
        prerelease,
        date_diff('day',
                  lag(published_at) over (partition by repo order by published_at),
                  published_at)                         as days_since_previous
    from {{ ref('stg_releases') }}
)

select
    a.repo,
    a.release_id,
    a.tag_name,
    a.published_at,
    a.prerelease                                        as is_prerelease,
    a.days_since_previous
from all_releases a
cross join {{ ref('dim_run') }} run
where a.published_at >= run.window_start
