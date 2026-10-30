-- One row per PR (from GraphQL). Bot rule: author.__typename = 'Bot' (GraphQL's name for
-- REST user.type) OR author.login LIKE '%[bot]'. A deleted account gives a null author.
-- GraphQL state OPEN / CLOSED / MERGED is mapped to REST open / closed; merged_at tells
-- merged from closed-unmerged. An unknown state maps to null and fails the tests.
with latest as (
    select *
    from {{ source('raw', 'raw_pull_requests') }}
    qualify row_number() over (partition by repo, id order by fetched_at desc) = 1
)

select
    repo,
    id::integer                                           as number,
    repo || '#' || id                                     as pr_key,
    payload ->> '$.author.login'                          as author_login,
    coalesce(payload ->> '$.author.__typename' = 'Bot'
             or payload ->> '$.author.login' like '%[bot]', false) as is_bot,
    case payload ->> '$.state'
        when 'OPEN' then 'open'
        when 'CLOSED' then 'closed'
        when 'MERGED' then 'closed'
    end                                                   as state,
    (payload ->> '$.isDraft')::boolean                    as is_draft,
    (payload ->> '$.createdAt')::timestamptz              as created_at,
    (payload ->> '$.updatedAt')::timestamptz              as updated_at,
    (payload ->> '$.closedAt')::timestamptz               as closed_at,
    (payload ->> '$.mergedAt')::timestamptz               as merged_at,
    (payload ->> '$.reviews.totalCount')::integer         as review_count_reported,
    fetched_at::timestamptz                               as fetched_at
from latest
