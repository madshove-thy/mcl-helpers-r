# =============================================================================
# get_comments_async_batched()
# =============================================================================
# Collect comments for every post in a posts dataframe using ASYNCHRONOUS
# search, PACKING POSTS FROM SEVERAL PAGES INTO EACH JOB to make the most of
# the 1 job/minute limit. This is the fastest way to collect comments.
# Comments are saved as one .rds file per post:
#
#   <save_dir>/<page_id>/<post_id>.rds
#
# How jobs are built:
#   - Each job holds at most 250 posts (API limit) and, if comment counts are
#     available, at most `max_comments_per_job` comments (default 90,000, a
#     margin below the 100,000-result job cap).
#   - Pages that fit in one job are packed together with greedy first-fit
#     bin packing (largest pages first, for a better fill). A page's posts
#     are not split across jobs if they fit in one.
#   - Pages too big for one job are split into several jobs.
#   - Posts with more comments than `max_comments_per_job` get a job of their
#     own, after all other jobs. If such a post has more than
#     `max_comments_per_post` (99,000) comments, only the FIRST 99,000 (by
#     creation time) are collected and the post is flagged as "truncated".
#
# LIMITS (defined in .mcl_limits in mcl_utils.R):
#   - 1 async job per minute.
#   - 250 post IDs per job (`parent_ids`).
#   - 100,000 comments per job (kept below via max_comments_per_job).
#   - Every comment returned counts towards the 500,000-record weekly budget.
#     Before each job, its expected comments (from the posts' comment counts)
#     are checked against `max_budget` (what you are willing to spend in this
#     run) and, with check_budget = TRUE, the remaining weekly budget. Jobs
#     that do not fit are skipped; the run ends when no budget is left.
#
# Each job's results are saved as soon as the job finishes, so a collection
# can span several runs (or weeks) without losing data. Posts that already
# have a file are skipped. Combine the results with load_comments(save_dir).
#
# Failed jobs: the post IDs in a failed job are written to
# <save_dir>/failed_ids.rds and skipped on later runs. Because a job holds
# posts from several pages, not all of them may be at fault. Retry them with
# get_comments_async() (jobs per page), which ignores failed_ids.rds:
#
#   failed <- readRDS("data/comments_FB/failed_ids.rds")
#   get_comments_async(client, filter(posts, id %in% failed),
#                      save_dir = "data/comments_FB")
#
# Then delete failed_ids.rds (or remove the IDs that now succeeded).
#
# Requires: mcl_utils.R
#
# Arguments:
#   client                 Initialised MCL API client
#   posts_df               Posts dataframe, e.g. the combined output of get_posts_*()
#   save_dir               Folder where the comments are saved (created if needed)
#   id_col                 Column in posts_df with post IDs (default "id")
#   page_col               Column in posts_df with page IDs (default "post_owner.id")
#   count_col              Column in posts_df with comment counts
#                          (default "statistics.comment_count")
#   time_col               Column in posts_df with post creation time
#                          (default "creation_time"; used for large posts)
#   comment_post_col       Column in the returned comments with the parent
#                          post ID, used to split results by post (default "post_id")
#   skip_zero              Skip posts with 0 comments (default TRUE)
#   max_comments_per_job   Target max comments per job (default 90,000)
#   max_comments_per_post  Max comments collected for a single large post
#                          (default 99,000)
#   max_budget             Max number of records (comments) this run may use,
#                          e.g. 100000. NULL (default) = no limit beyond the
#                          weekly budget. Counted per call: a re-run starts at 0.
#   check_budget           Skip jobs that exceed the remaining weekly budget
#   fetch_all              TRUE = all comment levels (replies included)
#   since, until           Optional date filters on comment creation time
#   fields                 Optional field list; NULL (default) returns all fields
#   poll_interval          Seconds between job status checks (default 30)
#   overwrite              If TRUE, re-collect posts that already have a file
#   verbose                Print progress messages
#
# Returns (invisibly):
#   A log tibble with one row per post: page_id, post_id, n, status
#   ("ok", "truncated", "skipped", "error") and note.
#
# Output:
#   The comments are SAVED TO DISK, not returned. The function creates
#   `save_dir` and one subfolder per page ID itself. What it returns
#   (invisibly) is the run log; assign it with `<-` to inspect it afterwards.
#   Read the saved comments into R with load_comments().
#
# Examples:
#   # 1. Basic run: collect comments for all posts, keep the run log
#   log <- get_comments_async_batched(
#     client   = client,
#     posts_df = posts,
#     save_dir = "../MP_FV22/comments/"
#   )
#
#   # 2. Inspect the log: what was collected, skipped, truncated or failed?
#   count(log, status)                         # overview
#   filter(log, status == "error")             # failed posts and the error message
#   filter(log, status == "truncated")         # posts with >99,000 comments (first 99,000 kept)
#
#   # 3. Limit the budget: spend at most 150,000 records in this run.
#   #    Re-run the same call next week; finished posts are skipped.
#   log <- get_comments_async_batched(
#     client     = client,
#     posts_df   = posts,
#     save_dir   = "../MP_FV22/comments/",
#     max_budget = 150000
#   )
#
#   # 4. Only top-level comments (no replies), written in a given period
#   log <- get_comments_async_batched(
#     client    = client,
#     posts_df  = posts,
#     save_dir  = "../MP_FV22/comments_toplevel/",
#     fetch_all = FALSE,
#     since     = "2022-10-05",
#     until     = "2022-11-01"
#   )
#
#   # 5. Retry posts from failed jobs with one job per page, then clear the log
#   failed <- readRDS("../MP_FV22/comments/failed_ids.rds")
#   log_retry <- get_comments_async(
#     client   = client,
#     posts_df = filter(posts, id %in% failed),
#     save_dir = "../MP_FV22/comments/"
#   )
#   file.remove("../MP_FV22/comments/failed_ids.rds")
#
#   # 6. Load all saved comments into one tibble
#   comments <- load_comments("../MP_FV22/comments/")
# =============================================================================

get_comments_async_batched <- function(
    client,
    posts_df,
    save_dir,
    id_col                = "id",
    page_col              = "post_owner.id",
    count_col             = "statistics.comment_count",
    time_col              = "creation_time",
    comment_post_col      = "post_id",
    skip_zero             = TRUE,
    max_comments_per_job  = 90000,
    max_comments_per_post = 99000,
    max_budget            = NULL,
    check_budget          = TRUE,
    fetch_all             = TRUE,
    since                 = NULL,
    until                 = NULL,
    fields                = NULL,
    poll_interval         = 30,
    overwrite             = FALSE,
    verbose               = TRUE
) {
  .check_max_budget(max_budget)
  dir.create(save_dir, recursive = TRUE, showWarnings = FALSE)

  # Standardise the posts and drop posts with no comments
  posts <- .prepare_posts(posts_df, id_col, page_col, count_col, skip_zero, time_col)
  log   <- list()

  # --- Remove posts that are already collected or previously failed ---------
  if (!overwrite) {
    done   <- filter(posts, post_id %in% .done_posts(save_dir))
    failed <- filter(posts, post_id %in% .read_failed(save_dir), !post_id %in% done$post_id)
    if (nrow(done) > 0) {
      if (verbose) message(nrow(done), " post(s) already collected, skipping.")
      log <- c(log, list(.log_posts(done, status = "skipped", note = "file exists")))
    }
    if (nrow(failed) > 0) {
      if (verbose) message(nrow(failed), " post(s) listed in failed_ids.rds, skipping.")
      log <- c(log, list(.log_posts(failed, status = "skipped", note = "in failed_ids.rds")))
    }
    posts <- filter(posts, !post_id %in% c(done$post_id, failed$post_id))
  }

  if (nrow(posts) == 0) {
    message("Nothing to collect.")
    return(invisible(bind_rows(log)))
  }

  # --- Build the job list ----------------------------------------------------
  # Large posts (more comments than fit in a normal job) are handled one by one
  is_large  <- replace_na(posts$n_comm > max_comments_per_job, FALSE)
  big_posts <- posts[is_large, ]
  jobs      <- .build_comment_jobs(posts[!is_large, ], max_comments_per_job)

  if (verbose) message(length(jobs), " job(s) planned for ", sum(!is_large), " post(s) on ",
                       n_distinct(posts$page_id[!is_large]), " page(s)",
                       if (nrow(big_posts) > 0) paste0(", plus ", nrow(big_posts),
                                                       " large post(s) with a job each"), ".")

  # Job parameters other than parent_ids; compact() drops NULL filters.
  # `until` is kept separately for large posts, which set their own cut-off.
  base_params   <- compact(list(fields = fields, fetch_all = fetch_all, since = since))
  job_params    <- compact(c(base_params, list(until = until)))
  out_of_budget <- FALSE
  used          <- 0                    # records collected in this run

  # --- Normal jobs -----------------------------------------------------------
  for (i in seq_along(jobs)) {
    job_posts <- jobs[[i]]
    if (verbose) message("\n[Job ", i, "/", length(jobs), "] ",
                         n_distinct(job_posts$page_id), " page(s) | ",
                         nrow(job_posts), " posts")

    # Check the job against max_budget and the remaining weekly budget
    decision <- .budget_decision(client, sum(job_posts$n_comm, na.rm = TRUE),
                                 used, max_budget, check_budget)
    if (decision == "stop") { out_of_budget <- TRUE; break }
    if (decision == "skip") next                            # a smaller job may still fit

    res  <- .comment_job(client, job_posts, job_params, save_dir,
                         comment_post_col, poll_interval, verbose)
    if (!res$ok) .log_failed(save_dir, job_posts$post_id)   # skip these on later runs
    log  <- c(log, list(res$log))
    used <- used + sum(res$log$n, na.rm = TRUE)             # records spent so far
  }

  # --- Large posts: one job each, capped at max_comments_per_post -----------
  if (!out_of_budget) {
    for (k in seq_len(nrow(big_posts))) {
      post <- big_posts[k, ]
      if (verbose) message("\n[Large post ", k, "/", nrow(big_posts), "] ", post$post_id,
                           " (page ", post$page_id, ", ~",
                           format(post$n_comm, big.mark = ","), " comments)")

      decision <- .budget_decision(client, min(post$n_comm, max_comments_per_post),
                                   used, max_budget, check_budget)
      if (decision == "stop") break
      if (decision == "skip") next

      res  <- .large_comment_job(client, post, base_params, since, until,
                                 max_comments_per_post, save_dir, poll_interval, verbose)
      if (!res$ok) .log_failed(save_dir, post$post_id)
      log  <- c(log, list(res$log))
      used <- used + sum(res$log$n, na.rm = TRUE)
    }
  }

  log <- .log_not_run(log, posts)        # posts left out because the budget ran out
  if (verbose) .report_log(log, "post(s)")
  invisible(log)
}


# Internal: build the job list for get_comments_async_batched().
#
# Returns a list of jobs; each job is a tibble of the posts it contains
# (post_id, page_id, n_comm, ...), with at most 250 posts and at most
# max_comments_per_job expected comments.
#
# Pages too big for a single job come first, split into consecutive chunks.
# Pages that fit in one job are then packed with greedy first-fit, largest first.
.build_comment_jobs <- function(posts, max_comments_per_job) {
  if (nrow(posts) == 0) return(list())
  max_posts <- .mcl_limits$async_max_parent_ids

  # Chunk every page's posts within the post and comment caps
  page_chunks <- split(posts, posts$page_id) |>
    map(~ .chunk_posts(.x$post_id, .x$n_comm, max_posts, max_comments_per_job))

  # Posts (as a tibble) for a vector of post IDs
  rows_of <- function(ids) filter(posts, post_id %in% ids)

  jobs <- list()

  # --- Pages that need more than one job: one job per chunk ----------------
  big <- names(page_chunks)[map_int(page_chunks, length) > 1]
  for (pid in big) {
    jobs <- c(jobs, map(page_chunks[[pid]], rows_of))
  }

  # --- Pages that fit in one job: greedy first-fit bin packing -------------
  small <- setdiff(names(page_chunks), big)
  sizes <- map_int(small, ~ length(page_chunks[[.x]][[1]])) |> set_names(small)
  small <- names(sort(sizes, decreasing = TRUE))          # largest pages first
  bins  <- list()                                          # each bin: list(ids, n_comm)

  for (pid in small) {
    ids  <- page_chunks[[pid]][[1]]
    comm <- sum(replace_na(rows_of(ids)$n_comm, 0))

    # Put the page in the first bin with room for both its posts and comments
    placed <- FALSE
    for (b in seq_along(bins)) {
      if (length(bins[[b]]$ids) + length(ids) <= max_posts &&
          bins[[b]]$n_comm + comm <= max_comments_per_job) {
        bins[[b]]$ids    <- c(bins[[b]]$ids, ids)
        bins[[b]]$n_comm <- bins[[b]]$n_comm + comm
        placed <- TRUE
        break
      }
    }

    # No bin had room: open a new one
    if (!placed) bins <- c(bins, list(list(ids = ids, n_comm = comm)))
  }

  c(jobs, map(bins, ~ rows_of(.x$ids)))
}
