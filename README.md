# mcl-helpers-r

R helper functions for collecting data through the
[Meta Content Library (MCL) API](https://developers.facebook.com/docs/content-library-and-api/).
The functions handle the API's rate limits, result caps and weekly query
budget, retry failed requests, save results to disk as they go, and can
resume interrupted runs.

The functions run inside the MCL Secure Research Environment and call the
official Python client (`metacontentlibraryapi`) through `reticulate`.

**Background:** The functions were written while collecting Facebook posts,
and the comments on those posts, from Danish members of parliament elected
in 2022. That's why the examples use folder names like `../MP_FV22/`. The
functions themselves are general and work for any producer list or set of
Facebook posts.

## Files

| File | Function | Purpose |
|---|---|---|
| `mcl_utils.R` | (internal helpers) | API limits, rate limiting, retries, parsing, job polling, budget checks. **Source first.** |
| `check_query_budget.R` | `check_query_budget()` | Shows used and remaining weekly query budget |
| `get_producer_list.R` | `get_producer_list()` | Loads a producer list shared from the Content Library UI |
| `get_comments_async_batched.R` | `get_comments_async_batched()` | Collects comments on Facebook posts with async search, packing posts from several pages into each job |
| `load_comments.R` | `load_comments()` | Combines the saved comment files into one tibble |

## Coming next

These functions are written and are being tested before they are added:

| Function | Purpose |
|---|---|
| `get_posts_sync()` | Facebook posts per producer within a time frame, synchronous search |
| `get_posts_async()` | Facebook posts per producer, async search, one job per producer |
| `get_posts_async_batched()` | Facebook posts, async search, several producers per job |
| `get_comments_sync()` | Comments per post, synchronous search |
| `get_comments_async()` | Comments with async search, jobs organised per page (also used to retry posts that failed in a batched run) |

## Requirements

- Access to the Meta Content Library and its Secure Research Environment
- R packages: `tidyverse`, `reticulate`, `jsonlite`

## API limits

All limits are defined once, in `.mcl_limits` in `mcl_utils.R`. If Meta
changes a limit, that's the only place to edit.

| Limit | Value |
|---|---|
| Synchronous requests | 60 per minute (each page of results is one request) |
| Results per sync page | 100 |
| Results per sync query | 1,000 |
| Asynchronous jobs | 1 per minute |
| Results per async job | 100,000 |
| Post IDs per comments job | 250 |
| Query budget | 500,000 records per user, rolling 7 days, shared between the UI and the API |

The rate limits are tracked for the whole R session, so they hold even when
you call several functions one after another.

## Running the functions: `<-` or not?

The **collection function** `get_comments_async_batched()` saves its results
to disk and creates the folders it needs: `save_dir`, and one subfolder per
page ID. You can call it without `<-`:

```r
get_comments_async_batched(client = client, posts_df = posts, save_dir = "../MP_FV22/comments/")
```

It returns a **run log** invisibly: one row per post, with its status.
Assign it with `<-` if you want to check afterwards what failed or was
skipped. That's recommended for long runs:

```r
log <- get_comments_async_batched(client = client, posts_df = posts, save_dir = "../MP_FV22/comments/")
filter(log, status != "ok")
```

The comments themselves are read back with `load_comments()`.

`get_producer_list()`, `check_query_budget()` and `load_comments()`
**return** their result, so assign them with `<-`.

## Workflow example

Collect all comments on a set of Facebook posts. `posts` is a tibble of
posts with at least the columns `id` (post ID), `post_owner.id` (page ID)
and, ideally, `statistics.comment_count`, as returned by the MCL posts
endpoints.

```r
library(tidyverse)
library(reticulate)

# --- 1. Initialise the client (inside the Secure Research Environment) ------
client <- import("metacontentlibraryapi")$MetaContentLibraryAPIClient
client$set_default_version(client$LATEST_VERSION)

# --- 2. Load the helper functions (mcl_utils.R first) -----------------------
source("mcl_utils.R")
walk(setdiff(list.files(pattern = "\\.R$"), "mcl_utils.R"), source)

# --- 3. Check the budget ----------------------------------------------------
check_query_budget(client)

# --- 4. Load your producer list (shared from the Content Library UI) -------
producers <- get_producer_list(
  client    = client,
  list_id   = "YOUR-LIST-ID",
  save_path = "../MP_FV22/producer_list.rds"
)

# --- 5. Will all comments on your posts fit in the remaining budget? --------
check_query_budget(
  client  = client,
  planned = sum(as.numeric(posts$statistics.comment_count), na.rm = TRUE)
)

# --- 6. Collect comments: ../MP_FV22/comments/<page_id>/<post_id>.rds -------
log_comments <- get_comments_async_batched(
  client     = client,
  posts_df   = posts,
  save_dir   = "../MP_FV22/comments/",
  max_budget = 400000                   # spend at most 400,000 records
)
count(log_comments, status)             # ok / truncated / skipped / error
filter(log_comments, status == "error") # see the error messages

# If the budget ran out, run the SAME call again later: posts that already
# have a file are skipped, so only the missing ones are collected.

# --- 7. Load the comments and attach post information -----------------------
comments <- load_comments("../MP_FV22/comments/") |>
  left_join(posts, by = join_by(parent_post_id == id), suffix = c("", "_post"))
```

## Shared behaviour

- **Output.** Comments are saved as one file per post, in a subfolder per
  page: `<save_dir>/<page_id>/<post_id>.rds`. Each job's results are saved
  as soon as the job finishes, so no budget is wasted if a run stops halfway.
  A post with no comments still gets an empty file, so "collected, nothing
  found" is different from "not collected".
- **Resumable.** Posts that already have a file are skipped. Re-run the
  same call after an interruption, or when the weekly budget has reset. Use
  `overwrite = TRUE` to collect them again.
- **Run log.** One row per post with `page_id`, `post_id`, `n` (comments
  saved), `status` (`ok`, `truncated`, `skipped`, `error`) and `note`.
- **Failed jobs.** The post IDs from failed jobs are written to
  `<save_dir>/failed_ids.rds` and skipped on later runs. Because one bad ID
  can break a whole job, retry them with smaller jobs, then delete the file.
- **Budget control.**
  - `max_budget`: the maximum number of records one call may use, e.g.
    `max_budget = 100000`. It is counted per call, so a re-run starts at 0.
    `NULL` (the default) means no limit beyond the weekly budget.
  - `check_budget = TRUE` (the default): also respect the remaining weekly
    budget reported by the API.

  Before each job, its expected number of comments (from the posts' comment
  counts) is checked against both limits. A job that doesn't fit is
  skipped, so smaller jobs later in the queue can still run, and the run
  stops when no budget is left. Skipped posts are logged as `skipped`
  ("not enough budget") and are picked up by the next run.

## Comment jobs and very large posts

- A comments job holds at most **250 posts** (API limit) and, based on the
  posts' `statistics.comment_count`, at most `max_comments_per_job`
  comments (default 90,000). The margin below the 100,000 cap is there
  because a job expected to exceed the cap is rejected outright, and comment
  counts can be out of date.
- Pages that fit in one job are packed together (greedy first-fit, largest
  pages first). Pages too big for one job are split into several jobs.
- A post with more comments than `max_comments_per_job` gets its own job.
  If it has more than `max_comments_per_post` comments (default 99,000),
  only the **first 99,000 by creation time** are collected. The function
  finds the cut-off with a binary search over `until` using the estimate
  endpoint (about 25 sync requests, which don't use budget). The post is
  logged as `truncated`, with the cut-off time in `note`.
