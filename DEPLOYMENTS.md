# Prefect Deployments

All previous in-class examples of Prefect flows have run (executed) on student laptops, while their logging and output has been visible through the SDS Prefect server https://sds-prefect.pods.uvarc.io/.

However, an advanced feature of Prefect is that it can run scheduled or "detached" flows on the server itself. 

Here's how to set that up and enable it:

## Blocks / Secrets

1. Keep your entire flow script and all related files in a single repository. You will need the repo URL as part of the deployment.
2. Any private ENV variables your script depends on (such as your `GH_PAT`) must first be published to the Prefect Server as "Blocks" (i.e. a feature that can store secret variables you can call within your code).
   1. Go to https://sds-prefect.pods.uvarc.io/v2/blocks
   2. Select "+ New storage block"
   3. Select "Secret" (a generic secret), then "Create"
   4. Give the block a unique **name**, using your computing ID, such as `nem2p-pat` (lowercase letters, numbers, and dashes only). The value should be of the "password" type and contain the actual value of your PAT.
   5. Click "Save".
3. To use a Prefect Block within your flow, use this syntax:

    ```
    from prefect.blocks.system import Secret

    my_secret = Secret.load("nem2p-pat")

    # Access the stored secret in a reusable object
    secret = my_secret.get()
    ```

## Deployments

See the [reference example](https://gist.github.com/nmagee/177f916afefbc7ae5bcfa7e213ca5dd9) for all details and setup, but the crux is found in the `__main__` handler at the bottom:

```
if __name__ == "__main__":
    # runs midnight Eastern on the 28th of every month
    <flow_function_name>.from_source(
        source=GIT_URL,
        entrypoint="main.py:<flow_function_name>",
    ).deploy(
        name="package-health",
        work_pool_name=WORK_POOL,
        schedule=Cron("0 0 28 * *", timezone="America/New_York"),
        job_variables={
            "cpu_request": "2",
            "cpu_limit": "2",
            "memory_request": "2Gi",
            "memory_limit": "4Gi",
        },
    )
```

In the reference example a GitHub Gist repository was used, but any normal GitHub repo will work, using the `https://` prefix with the `.git` suffix.

### Private repositories

If your repo is private, the Prefect worker needs your PAT to clone it. A PAT with no scopes can only read public repositories, so generate a new one that can also read private repositories, and store it in your Secret block in place of the old one:

1. Go to GitHub → Settings → Developer settings → Personal access tokens → **Tokens (classic)** → "Generate new token (classic)".
2. Set an expiration that lasts past the end of the semester (60 days).
3. Check the **`repo`** scope. No other scopes are needed.
4. Copy the token and update the value of your Secret block (e.g. `nem2p-pat`).

A fine-grained token also works if you prefer narrower access: grant it access to your project repository with **Contents: Read-only** permission.

Instead of passing `GIT_URL` as a plain string, wrap it in a `GitRepository` object and pass your Secret block as the credential:

```
from prefect.blocks.system import Secret
from prefect.runner.storage import GitRepository

# be sure to use your PAT block below
if __name__ == "__main__":
    <flow_function_name>.from_source(
        source=GitRepository(
            url=GIT_URL,
            branch="main",
            credentials={"access_token": Secret.load("nem2p-pat")},
        ),
        entrypoint="main.py:<flow_function_name>",
    ).deploy(
        ...  # same deploy() arguments as above
    )
```

Pass the loaded `Secret` block itself, not `Secret.load(...).get()`. The deployment then stores only a reference to the block, and the worker fetches the token at run time, so the PAT is never written into the deployment's configuration.

The entrypoint should be the filename+primary flow function name, like this:

    main.py:package_health_flow

Keep the following values as written above:

- `WORK_POOL = "uvasds-k8s-pool"`
- `job_variables` and its values

To publish your deployment once written (and the entire code including your GIT_URL is correct and its content up-to-date), run this command from your project, pointing at the flow's entrypoint:

    prefect deploy ./main.py:<flow_function_name> \
        --name nem2p-deployment \
        --pool uvasds-k8s-pool

## Test your deployment

You should see your deployment listed in https://sds-prefect.pods.uvarc.io/v2/deployments

Select it to see details and any previous runs. Use the blue "Run" button in the upper-right to trigger a run, which you can then follow like any other run.

**For this project, trigger your deployment manually once.** The first scheduled run may not fall before the due date, so a manual run from the "Run" button is what you submit. Copy its log into `DEPLOYMENTS.md` along with your deployment command.

See the [NYC-Taxi-Monthly](https://sds-prefect.pods.uvarc.io/v2/deployments/deployment/987975de-1d75-48d3-810e-251d9e077b16?tab=Runs) sample deployment and try running it for yourself.

The savvy BSDS student will recognize that some outputs such as plots will not be available since they are built into the remote environment executing the code. This is okay ;^)

(In production the plots would be copied off to remote storage like S3, etc.)

---

## yxf9tv deployment (DS3022 DP2, Package Set B)

### Deployment command

The deployment is defined in `deploy()` in [main.py](main.py) (private repo, so `GitRepository` with the `yxf9tv-pat` Secret block as the credential). Register or update it from the project root:

```bash
uv run python main.py deploy
```

| Setting | Value |
|---|---|
| Deployment | `yxf9tv-package-health-flow/yxf9tv-deployment` |
| Source | `https://github.com/yxf9tv/dp2-package-health.git`, branch `main` |
| Entrypoint | `main.py:package_health_flow` |
| Work pool | `uvasds-k8s-pool` |
| Schedule | `0 0 28 * *`, `America/New_York` (midnight Eastern on the 28th of every month) |
| `job_variables` | `cpu_request` 2, `cpu_limit` 2, `memory_request` 2Gi, `memory_limit` 4Gi |

### Manually triggered run

Triggered from the **Run** button in the Prefect UI.

| Item | Value |
|---|---|
| Flow run | `brown-reindeer` (`01a19eee-27d8-7666-b344-7d0992da2597`) |
| Commit | `ebbcbf5` |
| Start → end (UTC) | 2026-11-03 01:19:50 → 01:24:29 |
| Run time | 4 min 21 s |
| Final state | **Completed** |
| API requests | 266 |

Each job pod starts with an empty working directory, so every server run is a full run (no `data/raw/` or `packages.duckdb` from a previous run). The plots are written inside the pod and are not kept.

Key lines:

```
01:20:02 Executing deployment step: git_clone
01:20:02 Deployment step 'git_clone' completed successfully
01:20:08 flow start: 3 repos, window 2024-01-01 -> now
01:24:25 Done. PASS=75 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=75
01:24:26 WARN commit months dask/dask: no commits in 2026-10-01; last commits in 2026-09-01 (1)
01:24:26 validation passed: all completeness checks OK
01:24:27 HEALTH RANKING (score = weighted sum of dimension subscores, 0-1)
                     repo  rank  responsiveness  contributor_base  activity  throughput  release_cadence  backlog  score
scikit-learn/scikit-learn     1            0.90              1.00      0.65        0.83             0.70     0.34   0.78
         plotly/plotly.py     2            0.82              0.85      0.42        0.92             0.68     0.66   0.73
                dask/dask     3            0.72              0.72      0.00        1.00             0.46     0.56   0.58
01:24:29 flow complete: API odometer = 266 requests {'graphql dask/dask': 29, 'graphql plotly/plotly.py': 18, 'graphql scikit-learn/scikit-learn': 138, 'rest dask/dask': 15, 'rest plotly/plotly.py': 31, 'rest scikit-learn/scikit-learn': 35}
01:24:29 Finished in state Completed()
```

The `WARN` line is real data, not an error: GitHub shows 0 dask commits in October 2026 (its last commit is on 2026-09-29). See the Task 4 section of the [README](README.md#task-4-validation-outliers-and-statistics).

<details>
<summary>Full run log (772 lines, times in UTC)</summary>

```
01:19:57 Worker 'KubernetesWorker 9b5579bd-3a84-4b0b-a05a-28e2a22d6e11' submitting flow run '01a19eee-27d8-7666-b344-7d0992da2597'
01:19:57 Creating Kubernetes job...
01:19:57 Kubernetes job 'brown-reindeer-n6m5t' created in namespace 'uvasds-services'
01:19:57 Flow run 'brown-reindeer' submitted to infrastructure
01:20:02 Running 1 deployment pull step(s)
01:20:02 Executing deployment step: git_clone
01:20:02 Deployment step 'git_clone' completed successfully
01:20:02 All deployment steps completed successfully
01:20:08 Beginning flow run 'brown-reindeer' for flow 'yxf9tv-package-health-flow'
01:20:08 flow start: 3 repos, window 2024-01-01 -> now
01:20:08 first run: no data/raw/, fetching the full window from 2024-01-01T00:00:00Z
01:20:08 watermarks: none (full window)
01:20:08 dask/dask: commits since 2024-01-01T00:00:00Z, pulls until updatedAt < 2024-01-01T00:00:00Z
01:20:08 scikit-learn/scikit-learn: commits since 2024-01-01T00:00:00Z, pulls until updatedAt < 2024-01-01T00:00:00Z
01:20:08 plotly/plotly.py: commits since 2024-01-01T00:00:00Z, pulls until updatedAt < 2024-01-01T00:00:00Z
01:20:08 Finished in state Completed()
01:20:08 Finished in state Completed()
01:20:08 Finished in state Completed()
01:20:09 Finished in state Completed()
01:20:09 Finished in state Completed()
01:20:09 Finished in state Completed()
01:20:09 Finished in state Completed()
01:20:09 Finished in state Completed()
01:20:09 Finished in state Completed()
01:20:09 Finished in state Completed()
01:20:10 Finished in state Completed()
01:20:10 Finished in state Completed()
01:20:10 Finished in state Completed()
01:20:10 Finished in state Completed()
01:20:10 Finished in state Completed()
01:20:10 Finished in state Completed()
01:20:10 Finished in state Completed()
01:20:10 Finished in state Completed()
01:20:11 Finished in state Completed()
01:20:11 Finished in state Completed()
01:20:11 Finished in state Completed()
01:20:11 Finished in state Completed()
01:20:11 Finished in state Completed()
01:20:11 Finished in state Completed()
01:20:11 Finished in state Completed()
01:20:11 Finished in state Completed()
01:20:12 Finished in state Completed()
01:20:12 Finished in state Completed()
01:20:12 Finished in state Completed()
01:20:12 Finished in state Completed()
01:20:12 Finished in state Completed()
01:20:12 Finished in state Completed()
01:20:12 Finished in state Completed()
01:20:12 Finished in state Completed()
01:20:12 Finished in state Completed()
01:20:13 Finished in state Completed()
01:20:13 Finished in state Completed()
01:20:13 Finished in state Completed()
01:20:13 Finished in state Completed()
01:20:13 Finished in state Completed()
01:20:13 Finished in state Completed()
01:20:13 Finished in state Completed()
01:20:14 Finished in state Completed()
01:20:14 Finished in state Completed()
01:20:14 Finished in state Completed()
01:20:14 Finished in state Completed()
01:20:14 Finished in state Completed()
01:20:14 Finished in state Completed()
01:20:14 Finished in state Completed()
01:20:14 Finished in state Completed()
01:20:15 Finished in state Completed()
01:20:15 Finished in state Completed()
01:20:15 Finished in state Completed()
01:20:15 Finished in state Completed()
01:20:15 Finished in state Completed()
01:20:15 Finished in state Completed()
01:20:15 Finished in state Completed()
01:20:16 Finished in state Completed()
01:20:16 Finished in state Completed()
01:20:16 Finished in state Completed()
01:20:16 Finished in state Completed()
01:20:16 Finished in state Completed()
01:20:16 Finished in state Completed()
01:20:17 Finished in state Completed()
01:20:17 Finished in state Completed()
01:20:17 Finished in state Completed()
01:20:17 Finished in state Completed()
01:20:17 Finished in state Completed()
01:20:17 Finished in state Completed()
01:20:17 Finished in state Completed()
01:20:18 Finished in state Completed()
01:20:18 Finished in state Completed()
01:20:18 Finished in state Completed()
01:20:18 Finished in state Completed()
01:20:18 Finished in state Completed()
01:20:18 Finished in state Completed()
01:20:19 Finished in state Completed()
01:20:19 Finished in state Completed()
01:20:19 Finished in state Completed()
01:20:19 Finished in state Completed()
01:20:20 Finished in state Completed()
01:20:20 Finished in state Completed()
01:20:20 Finished in state Completed()
01:20:20 Finished in state Completed()
01:20:21 Finished in state Completed()
01:20:21 Finished in state Completed()
01:20:21 Finished in state Completed()
01:20:21 Finished in state Completed()
01:20:21 Finished in state Completed()
01:20:22 Finished in state Completed()
01:20:22 Finished in state Completed()
01:20:22 Finished in state Completed()
01:20:23 Finished in state Completed()
01:20:23 Finished in state Completed()
01:20:24 Finished in state Completed()
01:20:24 Finished in state Completed()
01:20:25 Finished in state Completed()
01:20:25 Finished in state Completed()
01:20:25 Finished in state Completed()
01:20:26 Finished in state Completed()
01:20:27 Finished in state Completed()
01:20:27 Finished in state Completed()
01:20:27 Finished in state Completed()
01:20:28 Finished in state Completed()
01:20:28 Finished in state Completed()
01:20:29 Finished in state Completed()
01:20:30 Finished in state Completed()
01:20:30 Finished in state Completed()
01:20:30 Finished in state Completed()
01:20:31 Finished in state Completed()
01:20:31 Finished in state Completed()
01:20:32 Finished in state Completed()
01:20:33 Finished in state Completed()
01:20:33 Finished in state Completed()
01:20:34 Finished in state Completed()
01:20:35 Finished in state Completed()
01:20:35 Finished in state Completed()
01:20:35 Finished in state Completed()
01:20:36 Finished in state Completed()
01:20:37 Finished in state Completed()
01:20:37 Finished in state Completed()
01:20:37 Finished in state Completed()
01:20:38 Finished in state Completed()
01:20:38 Finished in state Completed()
01:20:38 Finished in state Completed()
01:20:39 Finished in state Completed()
01:20:40 Finished in state Completed()
01:20:40 Finished in state Completed()
01:20:41 Finished in state Completed()
01:20:41 Finished in state Completed()
01:20:41 plotly/plotly.py reviews: PR #4790 overflow -> 9 more reviews
01:20:41 plotly/plotly.py commits: 2827 records (29 pages fetched, 0 skipped)
01:20:41 plotly/plotly.py releases: 109 records (2 pages fetched, 0 skipped)
01:20:41 plotly/plotly.py pulls: 850 records (17 pages fetched, 0 skipped)
01:20:41 plotly/plotly.py review_overflow: 9 records (1 pages fetched, 0 skipped)
01:20:41 plotly/plotly.py: 1742 nested reviews, 1 PRs with >100 reviews, 33s
01:20:41 Finished in state Completed()
01:20:41 Finished in state Completed()
01:20:41 Finished in state Completed()
01:20:42 Finished in state Completed()
01:20:43 Finished in state Completed()
01:20:44 Finished in state Completed()
01:20:45 Finished in state Completed()
01:20:45 Finished in state Completed()
01:20:46 Finished in state Completed()
01:20:47 Finished in state Completed()
01:20:48 Finished in state Completed()
01:20:48 Finished in state Completed()
01:20:49 Finished in state Completed()
01:20:50 Finished in state Completed()
01:20:51 Finished in state Completed()
01:20:52 Finished in state Completed()
01:20:52 dask/dask commits: 1371 records (14 pages fetched, 0 skipped)
01:20:52 dask/dask releases: 44 records (1 pages fetched, 0 skipped)
01:20:52 dask/dask pulls: 1450 records (29 pages fetched, 0 skipped)
01:20:52 dask/dask review_overflow: 0 records (0 pages fetched, 0 skipped)
01:20:52 dask/dask: 2786 nested reviews, 0 PRs with >100 reviews, 44s
01:20:52 Finished in state Completed()
01:20:53 Finished in state Completed()
01:20:55 Finished in state Completed()
01:20:57 Finished in state Completed()
01:20:58 Finished in state Completed()
01:21:00 Finished in state Completed()
01:21:02 Finished in state Completed()
01:21:03 Finished in state Completed()
01:21:04 Finished in state Completed()
01:21:06 Finished in state Completed()
01:21:07 Finished in state Completed()
01:21:08 Finished in state Completed()
01:21:10 Finished in state Completed()
01:21:12 Finished in state Completed()
01:21:13 Finished in state Completed()
01:21:15 Finished in state Completed()
01:21:16 Finished in state Completed()
01:21:18 Finished in state Completed()
01:21:19 Finished in state Completed()
01:21:21 Finished in state Completed()
01:21:23 Finished in state Completed()
01:21:24 Finished in state Completed()
01:21:26 Finished in state Completed()
01:21:28 Finished in state Completed()
01:21:29 Finished in state Completed()
01:21:31 Finished in state Completed()
01:21:32 Finished in state Completed()
01:21:33 Finished in state Completed()
01:21:35 Finished in state Completed()
01:21:37 Finished in state Completed()
01:21:39 Finished in state Completed()
01:21:40 Finished in state Completed()
01:21:42 Finished in state Completed()
01:21:44 Finished in state Completed()
01:21:45 Finished in state Completed()
01:21:47 Finished in state Completed()
01:21:48 Finished in state Completed()
01:21:50 Finished in state Completed()
01:21:51 Finished in state Completed()
01:21:53 Finished in state Completed()
01:21:55 Finished in state Completed()
01:21:57 Finished in state Completed()
01:21:59 Finished in state Completed()
01:22:00 Finished in state Completed()
01:22:01 Finished in state Completed()
01:22:02 Finished in state Completed()
01:22:04 Finished in state Completed()
01:22:05 Finished in state Completed()
01:22:07 Finished in state Completed()
01:22:08 Finished in state Completed()
01:22:10 Finished in state Completed()
01:22:11 Finished in state Completed()
01:22:13 Finished in state Completed()
01:22:15 Finished in state Completed()
01:22:16 Finished in state Completed()
01:22:18 Finished in state Completed()
01:22:19 Finished in state Completed()
01:22:21 Finished in state Completed()
01:22:22 Finished in state Completed()
01:22:24 Finished in state Completed()
01:22:26 Finished in state Completed()
01:22:28 Finished in state Completed()
01:22:30 Finished in state Completed()
01:22:31 Finished in state Completed()
01:22:33 Finished in state Completed()
01:22:35 Finished in state Completed()
01:22:36 Finished in state Completed()
01:22:38 Finished in state Completed()
01:22:39 Finished in state Completed()
01:22:41 Finished in state Completed()
01:22:43 Finished in state Completed()
01:22:44 Finished in state Completed()
01:22:46 Finished in state Completed()
01:22:47 Finished in state Completed()
01:22:49 Finished in state Completed()
01:22:51 Finished in state Completed()
01:22:52 Finished in state Completed()
01:22:55 Finished in state Completed()
01:22:57 Finished in state Completed()
01:22:58 Finished in state Completed()
01:23:00 Finished in state Completed()
01:23:01 Finished in state Completed()
01:23:03 Finished in state Completed()
01:23:05 Finished in state Completed()
01:23:06 Finished in state Completed()
01:23:08 Finished in state Completed()
01:23:10 Finished in state Completed()
01:23:11 Finished in state Completed()
01:23:13 Finished in state Completed()
01:23:15 Finished in state Completed()
01:23:17 Finished in state Completed()
01:23:18 Finished in state Completed()
01:23:20 Finished in state Completed()
01:23:22 Finished in state Completed()
01:23:24 Finished in state Completed()
01:23:25 Finished in state Completed()
01:23:27 Finished in state Completed()
01:23:29 Finished in state Completed()
01:23:31 Finished in state Completed()
01:23:32 Finished in state Completed()
01:23:34 Finished in state Completed()
01:23:36 Finished in state Completed()
01:23:37 Finished in state Completed()
01:23:39 Finished in state Completed()
01:23:41 Finished in state Completed()
01:23:43 Finished in state Completed()
01:23:44 Finished in state Completed()
01:23:46 Finished in state Completed()
01:23:47 Finished in state Completed()
01:23:49 Finished in state Completed()
01:23:51 Finished in state Completed()
01:23:51 Finished in state Completed()
01:23:51 scikit-learn/scikit-learn reviews: PR #11950 overflow -> 7 more reviews
01:23:52 Finished in state Completed()
01:23:52 scikit-learn/scikit-learn reviews: PR #33354 overflow -> 23 more reviews
01:23:52 Finished in state Completed()
01:23:52 scikit-learn/scikit-learn reviews: PR #33407 overflow -> 17 more reviews
01:23:52 Finished in state Completed()
01:23:52 scikit-learn/scikit-learn reviews: PR #32119 overflow -> 29 more reviews
01:23:53 Finished in state Completed()
01:23:53 scikit-learn/scikit-learn reviews: PR #32644 overflow -> 34 more reviews
01:23:53 Finished in state Completed()
01:23:53 scikit-learn/scikit-learn reviews: PR #30399 overflow -> 2 more reviews
01:23:53 Finished in state Completed()
01:23:53 scikit-learn/scikit-learn reviews: PR #13649 overflow -> 14 more reviews
01:23:54 Finished in state Completed()
01:23:54 scikit-learn/scikit-learn reviews: PR #26120 overflow -> 98 more reviews
01:23:54 Finished in state Completed()
01:23:54 scikit-learn/scikit-learn reviews: PR #27904 overflow -> 3 more reviews
01:23:54 Finished in state Completed()
01:23:55 Finished in state Completed()
01:23:55 scikit-learn/scikit-learn reviews: PR #8602 overflow -> 149 more reviews
01:23:55 scikit-learn/scikit-learn commits: 3351 records (34 pages fetched, 0 skipped)
01:23:55 scikit-learn/scikit-learn releases: 51 records (1 pages fetched, 0 skipped)
01:23:55 scikit-learn/scikit-learn pulls: 6350 records (127 pages fetched, 0 skipped)
01:23:55 scikit-learn/scikit-learn review_overflow: 376 records (11 pages fetched, 0 skipped)
01:23:55 scikit-learn/scikit-learn: 25558 nested reviews, 10 PRs with >100 reviews, 227s
01:23:55 Finished in state Completed()
01:23:55 extract done in 227s: 266 requests this task; odometer {'graphql dask/dask': 29, 'graphql plotly/plotly.py': 18, 'graphql scikit-learn/scikit-learn': 138, 'rest dask/dask': 15, 'rest plotly/plotly.py': 31, 'rest scikit-learn/scikit-learn': 35}
01:23:55 Finished in state Completed()
01:23:56 read 266 landing files from data/raw
01:23:56 raw_commits: 7549 rows in files (7549 distinct keys); table 0 -> 7549 (+7549 new, 0 replaced)
01:23:56 raw_pull_requests: 8650 rows in files (8650 distinct keys); table 0 -> 8650 (+8650 new, 0 replaced)
01:23:56 raw_pr_reviews: 30471 rows in files (30471 distinct keys); table 0 -> 30471 (+30471 new, 0 replaced)
01:23:56 raw_releases: 204 rows in files (204 distinct keys); table 0 -> 204 (+204 new, 0 replaced)
01:23:57 _load_state dask/dask commits: max_updated_at 2026-09-29 10:11:02
01:23:57 _load_state dask/dask pulls: max_updated_at 2026-11-02 18:30:10
01:23:57 _load_state dask/dask releases: max_updated_at 2026-08-24 19:22:50
01:23:57 _load_state dask/dask reviews: max_updated_at 2026-09-29 10:10:55
01:23:57 _load_state plotly/plotly.py commits: max_updated_at 2026-10-30 22:37:41
01:23:57 _load_state plotly/plotly.py pulls: max_updated_at 2026-11-03 00:10:05
01:23:57 _load_state plotly/plotly.py releases: max_updated_at 2026-09-15 19:15:12
01:23:57 _load_state plotly/plotly.py reviews: max_updated_at 2026-10-31 19:39:07
01:23:57 _load_state scikit-learn/scikit-learn commits: max_updated_at 2026-11-02 16:13:25
01:23:57 _load_state scikit-learn/scikit-learn pulls: max_updated_at 2026-11-03 00:31:37
01:23:57 _load_state scikit-learn/scikit-learn releases: max_updated_at 2026-09-11 09:49:47
01:23:57 _load_state scikit-learn/scikit-learn reviews: max_updated_at 2026-11-03 00:31:19
01:23:59 touched data/raw/_LOADED: next run starts fresh from the watermarks
01:23:59 Finished in state Completed()
01:24:00 Running with dbt=1.12.6
01:24:00 Registered adapter: duckdb=1.11.0
01:24:00 Unable to do partial parsing because saved manifest not found. Starting full parse.
01:24:04 Found 9 models, 66 data tests, 4 sources, 500 macros
01:24:05 Concurrency: 1 threads (target='dev')
01:24:05 1 of 75 START sql view model main.stg_commits .................................. [RUN]
01:24:05 1 of 75 OK created sql view model main.stg_commits ............................. [[32mOK[0m in 0.13s]
01:24:05 2 of 75 START sql view model main.stg_pr_reviews ............................... [RUN]
01:24:05 2 of 75 OK created sql view model main.stg_pr_reviews .......................... [[32mOK[0m in 0.13s]
01:24:05 3 of 75 START sql view model main.stg_pull_requests ............................ [RUN]
01:24:05 3 of 75 OK created sql view model main.stg_pull_requests ....................... [[32mOK[0m in 0.07s]
01:24:05 4 of 75 START sql view model main.stg_releases ................................. [RUN]
01:24:05 4 of 75 OK created sql view model main.stg_releases ............................ [[32mOK[0m in 0.08s]
01:24:05 5 of 75 START test not_null_stg_commits_committed_at ........................... [RUN]
01:24:05 5 of 75 PASS not_null_stg_commits_committed_at ................................. [[32mPASS[0m in 0.29s]
01:24:05 6 of 75 START test not_null_stg_commits_is_bot ................................. [RUN]
01:24:06 6 of 75 PASS not_null_stg_commits_is_bot ....................................... [[32mPASS[0m in 0.24s]
01:24:06 7 of 75 START test not_null_stg_commits_repo ................................... [RUN]
01:24:06 7 of 75 PASS not_null_stg_commits_repo ......................................... [[32mPASS[0m in 0.08s]
01:24:06 8 of 75 START test not_null_stg_commits_sha .................................... [RUN]
01:24:06 8 of 75 PASS not_null_stg_commits_sha .......................................... [[32mPASS[0m in 0.09s]
01:24:06 9 of 75 START test unique_stg_commits_sha ...................................... [RUN]
01:24:06 9 of 75 PASS unique_stg_commits_sha ............................................ [[32mPASS[0m in 0.13s]
01:24:06 10 of 75 START test accepted_values_stg_pr_reviews_state__APPROVED__CHANGES_REQUESTED__COMMENTED__DISMISSED  [RUN]
01:24:06 10 of 75 PASS accepted_values_stg_pr_reviews_state__APPROVED__CHANGES_REQUESTED__COMMENTED__DISMISSED  [[32mPASS[0m in 0.19s]
01:24:06 11 of 75 START test not_null_stg_pr_reviews_is_bot ............................. [RUN]
01:24:06 11 of 75 PASS not_null_stg_pr_reviews_is_bot ................................... [[32mPASS[0m in 0.16s]
01:24:06 12 of 75 START test not_null_stg_pr_reviews_pr_key ............................. [RUN]
01:24:07 12 of 75 PASS not_null_stg_pr_reviews_pr_key ................................... [[32mPASS[0m in 0.30s]
01:24:07 13 of 75 START test not_null_stg_pr_reviews_pr_number .......................... [RUN]
01:24:07 Finished in state Completed()
01:24:07 13 of 75 PASS not_null_stg_pr_reviews_pr_number ................................ [[32mPASS[0m in 0.25s]
01:24:07 14 of 75 START test not_null_stg_pr_reviews_repo ............................... [RUN]
01:24:07 Finished in state Completed()
01:24:07 Finished in state Completed()
01:24:07 Finished in state Completed()
01:24:07 14 of 75 PASS not_null_stg_pr_reviews_repo ..................................... [[32mPASS[0m in 0.18s]
01:24:07 15 of 75 START test not_null_stg_pr_reviews_review_id .......................... [RUN]
01:24:07 15 of 75 PASS not_null_stg_pr_reviews_review_id ................................ [[32mPASS[0m in 0.22s]
01:24:07 16 of 75 START test not_null_stg_pr_reviews_state .............................. [RUN]
01:24:08 16 of 75 PASS not_null_stg_pr_reviews_state .................................... [[32mPASS[0m in 0.27s]
01:24:08 Finished in state Completed()
01:24:08 17 of 75 START test not_null_stg_pr_reviews_submitted_at ....................... [RUN]
01:24:08 17 of 75 PASS not_null_stg_pr_reviews_submitted_at ............................. [[32mPASS[0m in 0.14s]
01:24:08 18 of 75 START test unique_stg_pr_reviews_review_id ............................ [RUN]
01:24:08 18 of 75 PASS unique_stg_pr_reviews_review_id .................................. [[32mPASS[0m in 0.25s]
01:24:08 19 of 75 START test accepted_values_stg_pull_requests_state__open__closed ...... [RUN]
01:24:08 Finished in state Completed()
01:24:08 19 of 75 PASS accepted_values_stg_pull_requests_state__open__closed ............ [[32mPASS[0m in 0.22s]
01:24:08 20 of 75 START test assert_review_after_pr_created ............................. [RUN]
01:24:09 20 of 75 PASS assert_review_after_pr_created ................................... [[32mPASS[0m in 0.34s]
01:24:09 21 of 75 START test not_null_stg_pull_requests_created_at ...................... [RUN]
01:24:09 21 of 75 PASS not_null_stg_pull_requests_created_at ............................ [[32mPASS[0m in 0.21s]
01:24:09 22 of 75 START test not_null_stg_pull_requests_is_bot .......................... [RUN]
01:24:09 Finished in state Completed()
01:24:09 22 of 75 PASS not_null_stg_pull_requests_is_bot ................................ [[32mPASS[0m in 0.19s]
01:24:09 23 of 75 START test not_null_stg_pull_requests_number .......................... [RUN]
01:24:09 Finished in state Completed()
01:24:09 23 of 75 PASS not_null_stg_pull_requests_number ................................ [[32mPASS[0m in 0.21s]
01:24:09 24 of 75 START test not_null_stg_pull_requests_pr_key .......................... [RUN]
01:24:09 Finished in state Completed()
01:24:09 24 of 75 PASS not_null_stg_pull_requests_pr_key ................................ [[32mPASS[0m in 0.17s]
01:24:09 25 of 75 START test not_null_stg_pull_requests_repo ............................ [RUN]
01:24:09 Finished in state Completed()
01:24:10 Finished in state Completed()
01:24:10 25 of 75 PASS not_null_stg_pull_requests_repo .................................. [[32mPASS[0m in 0.37s]
01:24:10 26 of 75 START test not_null_stg_pull_requests_state ........................... [RUN]
01:24:10 Finished in state Completed()
01:24:10 Finished in state Completed()
01:24:10 Finished in state Completed()
01:24:10 26 of 75 PASS not_null_stg_pull_requests_state ................................. [[32mPASS[0m in 0.38s]
01:24:10 27 of 75 START test not_null_stg_pull_requests_updated_at ...................... [RUN]
01:24:10 27 of 75 PASS not_null_stg_pull_requests_updated_at ............................ [[32mPASS[0m in 0.20s]
01:24:10 28 of 75 START test relationships_stg_pr_reviews_pr_key__pr_key__ref_stg_pull_requests_  [RUN]
01:24:11 28 of 75 PASS relationships_stg_pr_reviews_pr_key__pr_key__ref_stg_pull_requests_  [[32mPASS[0m in 0.21s]
01:24:11 Finished in state Completed()
01:24:11 29 of 75 START test unique_stg_pull_requests_pr_key ............................ [RUN]
01:24:11 29 of 75 PASS unique_stg_pull_requests_pr_key .................................. [[32mPASS[0m in 0.11s]
01:24:11 30 of 75 START test not_null_stg_releases_prerelease ........................... [RUN]
01:24:11 30 of 75 PASS not_null_stg_releases_prerelease ................................. [[32mPASS[0m in 0.28s]
01:24:11 31 of 75 START test not_null_stg_releases_published_at ......................... [RUN]
01:24:11 Finished in state Completed()
01:24:11 Finished in state Completed()
01:24:11 31 of 75 PASS not_null_stg_releases_published_at ............................... [[32mPASS[0m in 0.20s]
01:24:11 32 of 75 START test not_null_stg_releases_release_id ........................... [RUN]
01:24:11 Finished in state Completed()
01:24:11 32 of 75 PASS not_null_stg_releases_release_id ................................. [[32mPASS[0m in 0.25s]
01:24:11 33 of 75 START test not_null_stg_releases_repo ................................. [RUN]
01:24:11 Finished in state Completed()
01:24:12 33 of 75 PASS not_null_stg_releases_repo ....................................... [[32mPASS[0m in 0.23s]
01:24:12 34 of 75 START test unique_stg_releases_release_id ............................. [RUN]
01:24:12 Finished in state Completed()
01:24:12 Finished in state Completed()
01:24:12 34 of 75 PASS unique_stg_releases_release_id ................................... [[32mPASS[0m in 0.25s]
01:24:12 35 of 75 START sql table model main.dim_run .................................... [RUN]
01:24:12 Finished in state Completed()
01:24:12 35 of 75 OK created sql table model main.dim_run ............................... [[32mOK[0m in 0.47s]
01:24:12 Finished in state Completed()
01:24:12 Finished in state Completed()
01:24:12 36 of 75 START test not_null_dim_run_run_time .................................. [RUN]
01:24:13 36 of 75 PASS not_null_dim_run_run_time ........................................ [[32mPASS[0m in 0.18s]
01:24:13 37 of 75 START test not_null_dim_run_window_days ............................... [RUN]
01:24:13 37 of 75 PASS not_null_dim_run_window_days ..................................... [[32mPASS[0m in 0.21s]
01:24:13 38 of 75 START test not_null_dim_run_window_start .............................. [RUN]
01:24:13 Finished in state Completed()
01:24:13 38 of 75 PASS not_null_dim_run_window_start .................................... [[32mPASS[0m in 0.26s]
01:24:13 39 of 75 START sql table model main.fct_pull_requests .......................... [RUN]
01:24:13 Finished in state Completed()
01:24:13 39 of 75 OK created sql table model main.fct_pull_requests ..................... [[32mOK[0m in 0.36s]
01:24:13 40 of 75 START sql table model main.fct_releases ............................... [RUN]
01:24:14 Finished in state Completed()
01:24:14 40 of 75 OK created sql table model main.fct_releases .......................... [[32mOK[0m in 0.22s]
01:24:14 41 of 75 START test accepted_values_fct_pull_requests_state__open__closed ...... [RUN]
01:24:14 41 of 75 PASS accepted_values_fct_pull_requests_state__open__closed ............ [[32mPASS[0m in 0.15s]
01:24:14 42 of 75 START test assert_pr_closed_after_created ............................. [RUN]
01:24:14 Finished in state Completed()
01:24:14 42 of 75 PASS assert_pr_closed_after_created ................................... [[32mPASS[0m in 0.23s]
01:24:14 43 of 75 START test not_null_fct_pull_requests_created_at ...................... [RUN]
01:24:14 Finished in state Completed()
01:24:14 Finished in state Completed()
01:24:14 Finished in state Completed()
01:24:14 43 of 75 PASS not_null_fct_pull_requests_created_at ............................ [[32mPASS[0m in 0.25s]
01:24:14 44 of 75 START test not_null_fct_pull_requests_has_approving_review ............ [RUN]
01:24:15 44 of 75 PASS not_null_fct_pull_requests_has_approving_review .................. [[32mPASS[0m in 0.21s]
01:24:15 45 of 75 START test not_null_fct_pull_requests_human_review_count .............. [RUN]
01:24:15 Finished in state Completed()
01:24:15 Finished in state Completed()
01:24:15 45 of 75 PASS not_null_fct_pull_requests_human_review_count .................... [[32mPASS[0m in 0.35s]
01:24:15 46 of 75 START test not_null_fct_pull_requests_is_bot .......................... [RUN]
01:24:15 Finished in state Completed()
01:24:15 46 of 75 PASS not_null_fct_pull_requests_is_bot ................................ [[32mPASS[0m in 0.28s]
01:24:15 47 of 75 START test not_null_fct_pull_requests_is_closed_unmerged .............. [RUN]
01:24:15 Finished in state Completed()
01:24:15 47 of 75 PASS not_null_fct_pull_requests_is_closed_unmerged .................... [[32mPASS[0m in 0.19s]
01:24:15 48 of 75 START test not_null_fct_pull_requests_is_merged ....................... [RUN]
01:24:16 Finished in state Completed()
01:24:16 48 of 75 PASS not_null_fct_pull_requests_is_merged ............................. [[32mPASS[0m in 0.35s]
01:24:16 49 of 75 START test not_null_fct_pull_requests_is_open ......................... [RUN]
01:24:16 49 of 75 PASS not_null_fct_pull_requests_is_open ............................... [[32mPASS[0m in 0.24s]
01:24:16 50 of 75 START test not_null_fct_pull_requests_is_stale ........................ [RUN]
01:24:16 Finished in state Completed()
01:24:16 Finished in state Completed()
01:24:16 50 of 75 PASS not_null_fct_pull_requests_is_stale .............................. [[32mPASS[0m in 0.26s]
01:24:16 51 of 75 START test not_null_fct_pull_requests_number .......................... [RUN]
01:24:16 Finished in state Completed()
01:24:17 51 of 75 PASS not_null_fct_pull_requests_number ................................ [[32mPASS[0m in 0.21s]
01:24:17 Finished in state Completed()
01:24:17 52 of 75 START test not_null_fct_pull_requests_pr_key .......................... [RUN]
01:24:17 Finished in state Completed()
01:24:17 52 of 75 PASS not_null_fct_pull_requests_pr_key ................................ [[32mPASS[0m in 0.26s]
01:24:17 53 of 75 START test not_null_fct_pull_requests_repo ............................ [RUN]
01:24:17 53 of 75 PASS not_null_fct_pull_requests_repo .................................. [[32mPASS[0m in 0.15s]
01:24:17 54 of 75 START test not_null_fct_pull_requests_reviewed_within_7d .............. [RUN]
01:24:17 54 of 75 PASS not_null_fct_pull_requests_reviewed_within_7d .................... [[32mPASS[0m in 0.29s]
01:24:17 55 of 75 START test not_null_fct_pull_requests_state ........................... [RUN]
01:24:17 Finished in state Completed()
01:24:18 Finished in state Completed()
01:24:18 55 of 75 PASS not_null_fct_pull_requests_state ................................. [[32mPASS[0m in 0.33s]
01:24:18 56 of 75 START test unique_fct_pull_requests_pr_key ............................ [RUN]
01:24:18 Finished in state Completed()
01:24:18 Finished in state Completed()
01:24:18 56 of 75 PASS unique_fct_pull_requests_pr_key .................................. [[32mPASS[0m in 0.35s]
01:24:18 57 of 75 START test not_null_fct_releases_is_prerelease ........................ [RUN]
01:24:18 57 of 75 PASS not_null_fct_releases_is_prerelease .............................. [[32mPASS[0m in 0.20s]
01:24:18 58 of 75 START test not_null_fct_releases_published_at ......................... [RUN]
01:24:18 Finished in state Completed()
01:24:18 58 of 75 PASS not_null_fct_releases_published_at ............................... [[32mPASS[0m in 0.18s]
01:24:18 59 of 75 START test not_null_fct_releases_release_id ........................... [RUN]
01:24:19 59 of 75 PASS not_null_fct_releases_release_id ................................. [[32mPASS[0m in 0.16s]
01:24:19 60 of 75 START test not_null_fct_releases_repo ................................. [RUN]
01:24:19 60 of 75 PASS not_null_fct_releases_repo ....................................... [[32mPASS[0m in 0.20s]
01:24:19 61 of 75 START test unique_fct_releases_release_id ............................. [RUN]
01:24:19 Finished in state Completed()
01:24:19 Finished in state Completed()
01:24:19 61 of 75 PASS unique_fct_releases_release_id ................................... [[32mPASS[0m in 0.23s]
01:24:19 62 of 75 START sql table model main.mart_repo_monthly .......................... [RUN]
01:24:19 Finished in state Completed()
01:24:20 62 of 75 OK created sql table model main.mart_repo_monthly ..................... [[32mOK[0m in 0.61s]
01:24:20 Finished in state Completed()
01:24:20 63 of 75 START test not_null_mart_repo_monthly_active_contributors ............. [RUN]
01:24:20 Finished in state Completed()
01:24:20 63 of 75 PASS not_null_mart_repo_monthly_active_contributors ................... [[32mPASS[0m in 0.14s]
01:24:20 64 of 75 START test not_null_mart_repo_monthly_commits ......................... [RUN]
01:24:20 Finished in state Completed()
01:24:20 Finished in state Completed()
01:24:20 64 of 75 PASS not_null_mart_repo_monthly_commits ............................... [[32mPASS[0m in 0.15s]
01:24:20 65 of 75 START test not_null_mart_repo_monthly_is_partial_month ................ [RUN]
01:24:20 65 of 75 PASS not_null_mart_repo_monthly_is_partial_month ...................... [[32mPASS[0m in 0.19s]
01:24:20 66 of 75 START test not_null_mart_repo_monthly_month ........................... [RUN]
01:24:20 Finished in state Completed()
01:24:20 66 of 75 PASS not_null_mart_repo_monthly_month ................................. [[32mPASS[0m in 0.17s]
01:24:20 67 of 75 START test not_null_mart_repo_monthly_month_index ..................... [RUN]
01:24:20 Finished in state Completed()
01:24:21 67 of 75 PASS not_null_mart_repo_monthly_month_index ........................... [[32mPASS[0m in 0.31s]
01:24:21 68 of 75 START test not_null_mart_repo_monthly_prs_closed_unmerged ............. [RUN]
01:24:21 68 of 75 PASS not_null_mart_repo_monthly_prs_closed_unmerged ................... [[32mPASS[0m in 0.21s]
01:24:21 69 of 75 START test not_null_mart_repo_monthly_prs_merged ...................... [RUN]
01:24:21 69 of 75 PASS not_null_mart_repo_monthly_prs_merged ............................ [[32mPASS[0m in 0.56s]
01:24:21 70 of 75 START test not_null_mart_repo_monthly_prs_opened ...................... [RUN]
01:24:22 Finished in state Completed()
01:24:22 Finished in state Completed()
01:24:22 70 of 75 PASS not_null_mart_repo_monthly_prs_opened ............................ [[32mPASS[0m in 0.26s]
01:24:22 71 of 75 START test not_null_mart_repo_monthly_repo ............................ [RUN]
01:24:22 Finished in state Completed()
01:24:22 Finished in state Completed()
01:24:22 71 of 75 PASS not_null_mart_repo_monthly_repo .................................. [[32mPASS[0m in 0.25s]
01:24:22 72 of 75 START test unique_mart_repo_monthly_repo_month ........................ [RUN]
01:24:22 Finished in state Completed()
01:24:22 72 of 75 PASS unique_mart_repo_monthly_repo_month .............................. [[32mPASS[0m in 0.21s]
01:24:22 73 of 75 START sql table model main.mart_health_inputs ......................... [RUN]
01:24:23 Finished in state Completed()
01:24:23 73 of 75 OK created sql table model main.mart_health_inputs .................... [[32mOK[0m in 0.48s]
01:24:23 Finished in state Completed()
01:24:23 74 of 75 START test not_null_mart_health_inputs_repo ........................... [RUN]
01:24:23 Finished in state Completed()
01:24:23 74 of 75 PASS not_null_mart_health_inputs_repo ................................. [[32mPASS[0m in 0.17s]
01:24:23 75 of 75 START test unique_mart_health_inputs_repo ............................. [RUN]
01:24:23 75 of 75 PASS unique_mart_health_inputs_repo ................................... [[32mPASS[0m in 0.15s]
01:24:24 Finished in state Completed()
01:24:24 Finished in state Completed()
01:24:24 Finished in state Completed()
01:24:24 Finished in state Completed()
01:24:24 Finished in state Completed()
01:24:24 Finished in state Completed()
01:24:24 Finished running 5 table models, 66 data tests, 4 view models in 0 hours 0 minutes and 18.84 seconds (18.84s).
01:24:24 Finished in state Completed()
01:24:24 Finished in state Completed()
01:24:24 Finished in state Completed()
01:24:25 Finished in state Completed()
01:24:25 Finished in state Completed()
01:24:25 Finished in state Completed()
01:24:25 [32mCompleted successfully[0m
01:24:25 Done. PASS=75 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=75
01:24:25 SUCCESS model stg_commits
01:24:25 SUCCESS model stg_pr_reviews
01:24:25 SUCCESS model stg_pull_requests
01:24:25 SUCCESS model stg_releases
01:24:25 PASS test not_null_stg_commits_committed_at
01:24:25 PASS test not_null_stg_commits_is_bot
01:24:25 PASS test not_null_stg_commits_repo
01:24:25 PASS test not_null_stg_commits_sha
01:24:25 PASS test unique_stg_commits_sha
01:24:25 PASS test accepted_values_stg_pr_reviews_state__APPROVED__CHANGES_REQUESTED__COMMENTED__DISMISSED
01:24:25 PASS test not_null_stg_pr_reviews_is_bot
01:24:25 PASS test not_null_stg_pr_reviews_pr_key
01:24:25 PASS test not_null_stg_pr_reviews_pr_number
01:24:25 PASS test not_null_stg_pr_reviews_repo
01:24:25 PASS test not_null_stg_pr_reviews_review_id
01:24:25 PASS test not_null_stg_pr_reviews_state
01:24:25 PASS test not_null_stg_pr_reviews_submitted_at
01:24:25 PASS test unique_stg_pr_reviews_review_id
01:24:25 PASS test accepted_values_stg_pull_requests_state__open__closed
01:24:25 PASS test assert_review_after_pr_created
01:24:25 PASS test not_null_stg_pull_requests_created_at
01:24:25 PASS test not_null_stg_pull_requests_is_bot
01:24:25 PASS test not_null_stg_pull_requests_number
01:24:25 PASS test not_null_stg_pull_requests_pr_key
01:24:25 PASS test not_null_stg_pull_requests_repo
01:24:25 PASS test not_null_stg_pull_requests_state
01:24:25 PASS test not_null_stg_pull_requests_updated_at
01:24:25 PASS test relationships_stg_pr_reviews_pr_key__pr_key__ref_stg_pull_requests_
01:24:25 PASS test unique_stg_pull_requests_pr_key
01:24:25 PASS test not_null_stg_releases_prerelease
01:24:25 PASS test not_null_stg_releases_published_at
01:24:25 PASS test not_null_stg_releases_release_id
01:24:25 PASS test not_null_stg_releases_repo
01:24:25 PASS test unique_stg_releases_release_id
01:24:25 SUCCESS model dim_run
01:24:25 PASS test not_null_dim_run_run_time
01:24:25 PASS test not_null_dim_run_window_days
01:24:25 PASS test not_null_dim_run_window_start
01:24:25 SUCCESS model fct_pull_requests
01:24:25 SUCCESS model fct_releases
01:24:25 PASS test accepted_values_fct_pull_requests_state__open__closed
01:24:25 PASS test assert_pr_closed_after_created
01:24:25 PASS test not_null_fct_pull_requests_created_at
01:24:25 PASS test not_null_fct_pull_requests_has_approving_review
01:24:25 PASS test not_null_fct_pull_requests_human_review_count
01:24:25 PASS test not_null_fct_pull_requests_is_bot
01:24:25 PASS test not_null_fct_pull_requests_is_closed_unmerged
01:24:25 PASS test not_null_fct_pull_requests_is_merged
01:24:25 PASS test not_null_fct_pull_requests_is_open
01:24:25 PASS test not_null_fct_pull_requests_is_stale
01:24:25 PASS test not_null_fct_pull_requests_number
01:24:25 PASS test not_null_fct_pull_requests_pr_key
01:24:25 PASS test not_null_fct_pull_requests_repo
01:24:25 PASS test not_null_fct_pull_requests_reviewed_within_7d
01:24:25 PASS test not_null_fct_pull_requests_state
01:24:25 PASS test unique_fct_pull_requests_pr_key
01:24:25 PASS test not_null_fct_releases_is_prerelease
01:24:25 PASS test not_null_fct_releases_published_at
01:24:25 PASS test not_null_fct_releases_release_id
01:24:25 PASS test not_null_fct_releases_repo
01:24:25 PASS test unique_fct_releases_release_id
01:24:25 SUCCESS model mart_repo_monthly
01:24:25 PASS test not_null_mart_repo_monthly_active_contributors
01:24:25 PASS test not_null_mart_repo_monthly_commits
01:24:25 PASS test not_null_mart_repo_monthly_is_partial_month
01:24:25 PASS test not_null_mart_repo_monthly_month
01:24:25 PASS test not_null_mart_repo_monthly_month_index
01:24:25 PASS test not_null_mart_repo_monthly_prs_closed_unmerged
01:24:25 PASS test not_null_mart_repo_monthly_prs_merged
01:24:25 PASS test not_null_mart_repo_monthly_prs_opened
01:24:25 PASS test not_null_mart_repo_monthly_repo
01:24:25 PASS test unique_mart_repo_monthly_repo_month
01:24:25 SUCCESS model mart_health_inputs
01:24:25 PASS test not_null_mart_health_inputs_repo
01:24:25 PASS test unique_mart_health_inputs_repo
01:24:25 dbt build ok (window_start=2024-01-01): {'success': 9, 'pass': 66}
01:24:25 Finished in state Completed()
01:24:26 PASS files->raw raw_commits dask/dask: 1371 distinct in files, 1371 found in raw
01:24:26 PASS files->raw raw_commits plotly/plotly.py: 2827 distinct in files, 2827 found in raw
01:24:26 PASS files->raw raw_commits scikit-learn/scikit-learn: 3351 distinct in files, 3351 found in raw
01:24:26 PASS files->raw raw_pull_requests dask/dask: 1450 distinct in files, 1450 found in raw
01:24:26 PASS files->raw raw_pull_requests plotly/plotly.py: 850 distinct in files, 850 found in raw
01:24:26 PASS files->raw raw_pull_requests scikit-learn/scikit-learn: 6350 distinct in files, 6350 found in raw
01:24:26 PASS files->raw raw_pr_reviews dask/dask: 2786 distinct in files, 2786 found in raw
01:24:26 PASS files->raw raw_pr_reviews plotly/plotly.py: 1751 distinct in files, 1751 found in raw
01:24:26 PASS files->raw raw_pr_reviews scikit-learn/scikit-learn: 25934 distinct in files, 25934 found in raw
01:24:26 PASS files->raw raw_releases dask/dask: 44 distinct in files, 44 found in raw
01:24:26 PASS files->raw raw_releases plotly/plotly.py: 109 distinct in files, 109 found in raw
01:24:26 PASS files->raw raw_releases scikit-learn/scikit-learn: 51 distinct in files, 51 found in raw
01:24:26 checked 266 landing files
01:24:26 PASS raw->staging stg_commits dask/dask: raw 1371, staging 1371 (filter: true)
01:24:26 PASS raw->staging stg_commits plotly/plotly.py: raw 2827, staging 2827 (filter: true)
01:24:26 PASS raw->staging stg_commits scikit-learn/scikit-learn: raw 3351, staging 3351 (filter: true)
01:24:26 PASS raw->staging stg_pull_requests dask/dask: raw 1450, staging 1450 (filter: true)
01:24:26 PASS raw->staging stg_pull_requests plotly/plotly.py: raw 850, staging 850 (filter: true)
01:24:26 PASS raw->staging stg_pull_requests scikit-learn/scikit-learn: raw 6350, staging 6350 (filter: true)
01:24:26 PASS raw->staging stg_pr_reviews dask/dask: raw 2786, staging 2786 (filter: payload ->> '$.state' <> 'PENDING')
01:24:26 PASS raw->staging stg_pr_reviews plotly/plotly.py: raw 1751, staging 1751 (filter: payload ->> '$.state' <> 'PENDING')
01:24:26 PASS raw->staging stg_pr_reviews scikit-learn/scikit-learn: raw 25934, staging 25934 (filter: payload ->> '$.state' <> 'PENDING')
01:24:26 PASS raw->staging stg_releases dask/dask: raw 44, staging 44 (filter: not (payload ->> '$.draft')::boolean)
01:24:26 PASS raw->staging stg_releases plotly/plotly.py: raw 109, staging 109 (filter: not (payload ->> '$.draft')::boolean)
01:24:26 PASS raw->staging stg_releases scikit-learn/scikit-learn: raw 51, staging 51 (filter: not (payload ->> '$.draft')::boolean)
01:24:26 PASS reviews dask/dask: GitHub reports 2786, loaded 2786
01:24:26 PASS reviews plotly/plotly.py: GitHub reports 1751, loaded 1751
01:24:26 PASS reviews scikit-learn/scikit-learn: GitHub reports 25934, loaded 25934
01:24:26 WARN commit months dask/dask: no commits in 2026-10-01; last commits in 2026-09-01 (1)
01:24:26 OUTLIERS hours to merge (merged human PRs): cutoff = Q3 + 3 x IQR per repo
                     repo    n  cutoff  flagged
                dask/dask  830   296.2       64
         plotly/plotly.py  466   954.9       56
scikit-learn/scikit-learn 3330   773.1      358
01:24:26 OUTLIERS hours to merge (merged human PRs): top 10 per repo (full list: outliers_hours_to_merge.csv)
                     repo   item   value  cutoff
                dask/dask #10812  5166.0   296.2
                dask/dask #11118  3851.8   296.2
                dask/dask #12010  2839.7   296.2
                dask/dask #12002  2779.1   296.2
                dask/dask #11965  2764.7   296.2
                dask/dask #12024  2319.5   296.2
                dask/dask #11128  2251.5   296.2
                dask/dask #12041  2251.2   296.2
                dask/dask #11875  2229.3   296.2
                dask/dask #11421  2194.3   296.2
         plotly/plotly.py  #5232 11797.0   954.9
         plotly/plotly.py  #5311 10237.4   954.9
         plotly/plotly.py  #5262 10234.3   954.9
         plotly/plotly.py  #5310 10220.5   954.9
         plotly/plotly.py  #5285  9821.6   954.9
         plotly/plotly.py  #5374  7060.2   954.9
         plotly/plotly.py  #5333  6310.5   954.9
         plotly/plotly.py  #4949  4486.3   954.9
         plotly/plotly.py  #5393  4225.1   954.9
         plotly/plotly.py  #4958  4032.4   954.9
scikit-learn/scikit-learn #28464 18651.3   773.1
scikit-learn/scikit-learn #29026 16646.3   773.1
scikit-learn/scikit-learn #28760 16489.8   773.1
scikit-learn/scikit-learn #29532 15041.9   773.1
scikit-learn/scikit-learn #29499 15034.5   773.1
scikit-learn/scikit-learn #29641 13715.5   773.1
scikit-learn/scikit-learn #28043 12980.6   773.1
scikit-learn/scikit-learn #29661 12292.6   773.1
scikit-learn/scikit-learn #30869 12027.8   773.1
scikit-learn/scikit-learn #29155 11800.7   773.1
01:24:26 OUTLIERS weekly commits (all commits): cutoff = Q3 + 3 x IQR per repo
                     repo   n  cutoff  flagged
                dask/dask 147    42.0        4
         plotly/plotly.py 147    89.0        1
scikit-learn/scikit-learn 147    55.0        1
01:24:26 OUTLIERS weekly commits (all commits): top 10 per repo (full list: outliers_weekly_commits.csv)
                     repo               item  value  cutoff
                dask/dask week of 2024-01-08   77.0    42.0
                dask/dask week of 2024-01-15   75.0    42.0
                dask/dask week of 2024-01-29   45.0    42.0
                dask/dask week of 2024-03-04   43.0    42.0
         plotly/plotly.py week of 2024-10-21   98.0    89.0
scikit-learn/scikit-learn week of 2025-10-06   83.0    55.0
01:24:26 outlier decision: KEEP all flagged records. The marts and Task 5 use medians and shares, which these extremes barely move, and they are real events (long-lived PRs, release-week bursts), not data errors.
01:24:26 DESCRIPTIVE STATISTICS (human PRs; hours; medians and 90th percentiles)
                     repo  human_prs  first_review_h_median  first_review_h_p90  merge_h_median  merge_h_p90  commits_per_week  releases_per_year
                dask/dask       1158                   11.5               223.1            12.3        223.2               8.9               15.5
         plotly/plotly.py        643                   16.2               701.3            41.5       1130.4              19.0               10.2
scikit-learn/scikit-learn       5189                   12.9               276.2            35.7        844.7              22.5                5.3
01:24:26 validation passed: all completeness checks OK
01:24:26 Finished in state Completed()
01:24:26 METRICS (anchors bad -> good, weight of the dimension)
  pct_reviewed_within_7d         responsiveness       0.2 -> 0.8         w=0.25  Share of human PRs (at least 7 days old) with a first non-author, non-bot review within 7 days.
  median_hours_to_first_review   responsiveness       168 -> 24     log  w=0.25  Median hours from PR creation to the first non-author, non-bot review.
  approving_review_share         responsiveness       0.5 -> 1           w=0.25  Share of merged human PRs with a non-author, non-bot APPROVED review (review quality, not rubber-stamp merges).
  merge_ratio                    throughput           0.3 -> 0.8         w=0.15  Merged / (merged + closed unmerged) over human PRs in the window.
  median_hours_to_merge          throughput           720 -> 24     log  w=0.15  Median hours from PR creation to merge, over merged human PRs.
  stale_open_share               backlog              0.5 -> 0           w=0.10  Share of open human PRs with no update in 180+ days (rubric definition).
  commit_trend_pct               activity           -0.05 -> 0.05        w=0.20  Slope of monthly human commits over complete months / mean monthly commits (percent change per month; the trend component).
  commit_cv                      activity               1 -> 0.2         w=0.20  Coefficient of variation of monthly human commits over complete months (lower = steadier activity).
  release_gap_cv                 release_cadence      1.5 -> 0.3         w=0.10  Coefficient of variation of days between releases (lower = more regular).
  top_author_commit_share        contributor_base     0.6 -> 0.2         w=0.20  Share of linked human commits by the single most active author (bus-factor risk).
  top_reviewer_share             contributor_base     0.6 -> 0.2         w=0.20  Share of (PR, reviewer) pairs on human PRs by the single most active non-bot, non-author reviewer (bus-factor risk for reviews; CHAOSS counts all contributions).
01:24:26 ANCHORED METRIC SCORES (0 = bad anchor, 1 = good anchor)
repo                          dask/dask  plotly/plotly.py  scikit-learn/scikit-learn
metric                                                                              
approving_review_share             0.56              0.84                       0.98
commit_cv                          0.00              0.41                       0.89
commit_trend_pct                   0.00              0.43                       0.40
median_hours_to_first_review       1.00              1.00                       1.00
median_hours_to_merge              1.00              0.84                       0.88
merge_ratio                        1.00              1.00                       0.79
pct_reviewed_within_7d             0.60              0.63                       0.73
release_gap_cv                     0.46              0.68                       0.70
stale_open_share                   0.56              0.66                       0.34
top_author_commit_share            0.52              0.74                       1.00
top_reviewer_share                 0.92              0.97                       1.00
01:24:27 HEALTH RANKING (score = weighted sum of dimension subscores, 0-1)
                     repo  rank  responsiveness  contributor_base  activity  throughput  release_cadence  backlog  score
scikit-learn/scikit-learn     1            0.90              1.00      0.65        0.83             0.70     0.34   0.78
         plotly/plotly.py     2            0.82              0.85      0.42        0.92             0.68     0.66   0.73
                dask/dask     3            0.72              0.72      0.00        1.00             0.46     0.56   0.58
01:24:27 SENSITIVITY: signed z-score ranking (same order)
 rank                      repo  score
    1 scikit-learn/scikit-learn   0.50
    2          plotly/plotly.py   0.01
    3                 dask/dask  -0.51
01:24:27 wrote health_ranking (3 rows) and health_metric_scores (33 rows) to packages.duckdb
01:24:27 Finished in state Completed()
01:24:29 wrote plots/1_dimension_profile.png
01:24:29 wrote plots/2_monthly_trends.png
01:24:29 wrote plots/3_review_vs_merge.png
01:24:29 Finished in state Completed()
01:24:29 flow complete: API odometer = 266 requests {'graphql dask/dask': 29, 'graphql plotly/plotly.py': 18, 'graphql scikit-learn/scikit-learn': 138, 'rest dask/dask': 15, 'rest plotly/plotly.py': 31, 'rest scikit-learn/scikit-learn': 35}
01:24:29 Finished in state Completed()
```

</details>
