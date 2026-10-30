-- One row per commit. Bot rule: author.type = 'Bot' OR author.login LIKE '%[bot]'.
-- author is the GitHub account linked to the commit email; it can be null (then is_bot = false).
with latest as (
    select *
    from {{ source('raw', 'raw_commits') }}
    qualify row_number() over (partition by repo, id order by fetched_at desc) = 1
)

select
    repo,
    id                                                    as sha,
    payload ->> '$.author.login'                          as author_login,
    coalesce(payload ->> '$.author.type' = 'Bot'
             or payload ->> '$.author.login' like '%[bot]', false) as is_bot,
    (payload ->> '$.commit.author.date')::timestamptz     as committed_at,
    fetched_at::timestamptz                               as fetched_at
from latest
