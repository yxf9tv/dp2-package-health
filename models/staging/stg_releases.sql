-- One row per published release. Draft releases are dropped: they are not public
-- and have no published_at.
with latest as (
    select *
    from {{ source('raw', 'raw_releases') }}
    qualify row_number() over (partition by repo, id order by fetched_at desc) = 1
)

select
    repo,
    id::bigint                                            as release_id,
    payload ->> '$.tag_name'                              as tag_name,
    (payload ->> '$.published_at')::timestamptz           as published_at,
    (payload ->> '$.prerelease')::boolean                 as prerelease,
    fetched_at::timestamptz                               as fetched_at
from latest
where not (payload ->> '$.draft')::boolean
