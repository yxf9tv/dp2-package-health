-- Fails (returns rows) if any review was submitted before its PR was created.
select r.repo, r.pr_number, r.review_id, p.created_at, r.submitted_at
from {{ ref('stg_pr_reviews') }} r
join {{ ref('stg_pull_requests') }} p using (pr_key)
where r.submitted_at < p.created_at
