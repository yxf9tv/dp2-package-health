-- One row per submitted review (from GraphQL). PENDING reviews are dropped: they are
-- unsubmitted drafts with no submitted_at. pr_number comes from pullRequest.number
-- (GraphQL), so there is no pull_request_url to parse. Bot rule as in stg_pull_requests.
with latest as (
    select *
    from {{ source('raw', 'raw_pr_reviews') }}
    qualify row_number() over (partition by repo, id order by fetched_at desc) = 1
)

select
    repo,
    id::bigint                                            as review_id,
    (payload ->> '$.pullRequest.number')::integer         as pr_number,
    repo || '#' || (payload ->> '$.pullRequest.number')   as pr_key,
    payload ->> '$.author.login'                          as author_login,
    coalesce(payload ->> '$.author.__typename' = 'Bot'
             or payload ->> '$.author.login' like '%[bot]', false) as is_bot,
    payload ->> '$.state'                                 as state,
    (payload ->> '$.submittedAt')::timestamptz            as submitted_at,
    fetched_at::timestamptz                               as fetched_at
from latest
where payload ->> '$.state' <> 'PENDING'
