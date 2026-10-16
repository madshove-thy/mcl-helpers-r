# =============================================================================
# mcl_utils.R
# =============================================================================
# Shared constants and internal helpers used by every MCL helper function.
# Source this file BEFORE any of the other scripts:
#
#   source("mcl_utils.R")
#   source("check_query_budget.R")
#   ...
#
# Functions prefixed with "." are internal: they are not meant to be called
# directly by the user, but are shared by the public functions.
#
# All scripts expect an initialised MCL API client, created inside the
# Secure Research Environment:
#
#   library(reticulate)
#   client <- import("metacontentlibraryapi")$MetaContentLibraryAPIClient
#   client$set_default_version(client$LATEST_VERSION)
# =============================================================================

library(tidyverse)


# =============================================================================
# API LIMITS
# =============================================================================
# Single source of truth for the MCL API limits. Every function reads its
# limits from here, so if Meta changes a limit you only need to edit this list.
#
# Source: Meta Content Library API documentation, "Rate limiting and query
# budgeting" and the search guides.
.mcl_limits <- list(
  # --- Synchronous search ----------------------------------------------------
  sync_requests_per_minute = 60,      # max sync requests (incl. each new page) per minute
  sync_max_results         = 1000,    # hard cap on results per sync query (all pages combined)
  sync_page_size           = 100,     # max results returned per page of a sync query

  # --- Asynchronous search ---------------------------------------------------
  async_jobs_per_minute    = 1,       # max async jobs submitted per minute
  async_max_results        = 100000,  # hard cap on results per async job
  async_max_parent_ids     = 250,     # max post IDs in `parent_ids` of one comments job

  # --- Query budget -----------------------------------------------------------
  weekly_budget            = 500000   # records per user per rolling 7-day window,
                                      # shared between the Content Library UI and API
)


# =============================================================================
# RATE-LIMIT STATE
# =============================================================================
# A private environment that remembers when the last sync request and the last
# async job were sent. Because it lives at the session level, the rate limits
# are respected even when you call several collection functions back-to-back.
.mcl_state <- new.env(parent = emptyenv())
.mcl_state$last_sync_request <- -Inf   # elapsed time (seconds) of last sync request
.mcl_state$last_job_submit   <- -Inf   # elapsed time (seconds) of last async job


# Internal: wait until another synchronous request is allowed.
# With 60 requests/minute this enforces at least 1 second between requests.
.throttle_sync <- function() {
  min_gap <- 60 / .mcl_limits$sync_requests_per_minute            # seconds between requests
  elapsed <- proc.time()[["elapsed"]] - .mcl_state$last_sync_request
  if (elapsed < min_gap) Sys.sleep(min_gap - elapsed)              # sleep only the remainder
  .mcl_state$last_sync_request <- proc.time()[["elapsed"]]         # record this request
}


# Internal: wait until another asynchronous job is allowed (1 job/minute).
.throttle_job <- function(verbose = TRUE) {
  min_gap <- 60 / .mcl_limits$async_jobs_per_minute               # seconds between jobs
  elapsed <- proc.time()[["elapsed"]] - .mcl_state$last_job_submit
  if (elapsed < min_gap) {
    wait <- ceiling(min_gap - elapsed)
    if (verbose) message("  Waiting ", wait, "s (async rate limit: ",
                         .mcl_limits$async_jobs_per_minute, " job/minute)...")
    Sys.sleep(wait)
  }
  .mcl_state$last_job_submit <- proc.time()[["elapsed"]]           # record this submission
}


# =============================================================================
# REQUEST HELPERS
# =============================================================================

# Internal: run a request function `f`, retrying up to `retries` times with
# `wait` seconds between attempts. 502 Bad Gateway errors are NOT retried:
# during development they did not resolve within a retry window, so the
# calling function fails fast and keeps any partial results instead.
.retry <- function(f, retries = 3, wait = 30) {
  for (attempt in seq_len(retries)) {
    result <- tryCatch(f(), error = function(e) e)               # capture errors instead of stopping
    if (!inherits(result, "error")) return(result)                # success: return the response
    if (grepl("502", conditionMessage(result))) stop(conditionMessage(result))
    if (attempt < retries) {
      message("  Request failed (attempt ", attempt, "/", retries, "): ",
              conditionMessage(result), "\n  Retrying in ", wait, "s...")
      Sys.sleep(wait)
    }
  }
  stop(conditionMessage(result))                                  # all attempts failed
}


# Internal: send a GET request through the rate limiter and retry wrapper.
# If there are no parameters, `params` is left out completely: reticulate
# converts an empty R list() to a Python list ([]) rather than a dict, and
# the client then fails with "'list' object has no attribute 'items'".
.get <- function(client, path, params = NULL) {
  .retry(function() {
    .throttle_sync()                                              # respect 60 req/min
    if (length(params) == 0) {
      client$get(path = path)                                     # no parameters
    } else {
      client$get(path = path, params = params)                    # named list -> Python dict
    }
  })
}


# =============================================================================
# PARSING HELPERS
# =============================================================================

# Internal: flatten a list of records (nested R lists) into a tibble.
# Records are flattened one at a time with unlist(), because
# jsonlite::fromJSON(flatten = TRUE) fails when nested fields (e.g. activities,
# link attachments) are empty for some records. make.unique() handles
# duplicate names, e.g. posts with several multimedia items.
# Nested names are joined with "." (e.g. owner.id, statistics.comment_count).
.flatten_records <- function(records) {
  if (is.null(records) || length(records) == 0) return(tibble())

  records |>
    map(function(r) {
      flat        <- unlist(r)                     # nested list -> named vector
      names(flat) <- make.unique(names(flat))      # de-duplicate repeated field names
      as_tibble(as.list(flat))                     # one-row tibble per record
    }) |>
    bind_rows() |>                                 # stack records; missing fields become NA
    .parse_times()
}


# Internal: convert known timestamp columns from ISO 8601 strings to POSIXct (UTC).
.parse_times <- function(df) {
  df |>
    mutate(across(
      any_of(c("creation_time", "modified_time", "view_date_last_refreshed")),
      ~ as.POSIXct(.x, format = "%Y-%m-%dT%H:%M:%S", tz = "UTC")
    ))
}


# Internal: parse a synchronous API response into a tibble.
# The response object is a Python object: the payload is raw JSON in
# `response$text` (there is no `response$data`).
.parse_response <- function(response) {
  records <- jsonlite::fromJSON(response$text, simplifyDataFrame = FALSE)$data
  .flatten_records(records)
}


# =============================================================================
# SYNCHRONOUS SEARCH
# =============================================================================

# Internal: run a synchronous query and follow pagination until there are no
# more pages or the sync result cap (1,000) is reached.
#
# Returns a list with:
#   $data    tibble of all records retrieved
#   $capped  TRUE if the 1,000-result cap was hit (results are incomplete)
#   $partial TRUE if pagination failed midway (results are incomplete)
#   $note    error message if pagination failed, otherwise NA
.sync_query <- function(client, path, params) {
  cap <- .mcl_limits$sync_max_results

  response    <- .get(client, path, params)              # first page (errors propagate)
  pages       <- list(.parse_response(response))
  n_collected <- nrow(pages[[1]])
  partial     <- FALSE
  note        <- NA_character_

  # Follow pagination. Both has_next_page() and query_next_page() need the
  # previous response object as an argument.
  while (isTRUE(client$has_next_page(response)) && n_collected < cap) {
    response <- tryCatch(
      .retry(function() {
        .throttle_sync()
        client$query_next_page(response)
      }),
      error = function(e) {
        # Keep what we have rather than discarding the whole query
        note <<- conditionMessage(e)
        NULL
      }
    )
    if (is.null(response)) { partial <- TRUE; break }

    page_data   <- .parse_response(response)
    pages       <- c(pages, list(page_data))
    n_collected <- n_collected + nrow(page_data)
  }

  list(
    data    = bind_rows(pages),
    capped  = n_collected >= cap,
    partial = partial,
    note    = note
  )
}


# =============================================================================
# ASYNCHRONOUS SEARCH
# =============================================================================

# Internal: ask an estimate endpoint how many records a query would return.
# Returns NA if the estimate is unavailable. Estimates are approximate.
.estimate <- function(client, path, params) {
  tryCatch({
    response <- .get(client, path, params)
    count    <- jsonlite::fromJSON(response$text)$data$estimate_count
    if (is.null(count)) NA_real_ else as.numeric(count)
  }, error = function(e) NA_real_)
}


# Internal: submit an async job, poll until it completes, and return a tibble.
#
# Arguments:
#   path          Job endpoint, e.g. "facebook/posts/job" or "facebook/comments/job"
#   params        Named list of query parameters
#   poll_interval Seconds between status checks
#   max_wait      Give up after this many seconds (the job keeps running on
#                 Meta's side, but this function stops waiting)
.run_job <- function(client, path, params, poll_interval = 30,
                     max_wait = 6 * 3600, verbose = TRUE) {
  .throttle_job(verbose)                                    # respect 1 job/minute

  # Submit the job. Not retried: a failed submission (e.g. >100,000 expected
  # results) should surface immediately rather than be re-submitted.
  job_id <- client$post(path = path, params = params)$json()$id
  if (is.null(job_id)) stop("Job submission failed: no job ID returned.")
  if (verbose) message("  Job ID: ", job_id, ". Polling every ", poll_interval, "s...")

  job     <- client$get_async_job(job_id)
  started <- proc.time()[["elapsed"]]

  # Poll the job status until it is COMPLETE, fails, or max_wait is exceeded
  repeat {
    Sys.sleep(poll_interval)
    status <- .retry(function() job$get_status())
    if (verbose) message("  Status: ", status)
    if (status == "COMPLETE") break
    if (status %in% c("FAILED", "ERROR", "CANCELLED", "CANCELED"))
      stop("Job ", job_id, " ended with status '", status, "'.")
    if (proc.time()[["elapsed"]] - started > max_wait)
      stop("Job ", job_id, " still '", status, "' after ", max_wait, "s; stopped waiting.")
  }

  # job$get_data() is already an R list of records (converted by reticulate):
  # pass it straight to the flattener, do not call $text on it.
  .flatten_records(.retry(function() job$get_data()))
}


# =============================================================================
# BUDGET HELPER
# =============================================================================

# Internal: return the remaining query budget (records), or NA if unavailable.
# Remaining = max_usage_limit - total_usage, where total_usage includes rows
# preallocated to queries that are still running.
.budget_remaining <- function(client) {
  tryCatch({
    b <- .budget_fetch(client)
    b$max_usage_limit - b$total_usage
  }, error = function(e) NA_real_)
}


# Internal: decide whether the next query fits in the budget. Two limits are
# checked:
#   1. max_budget   the number of records the user is willing to spend in
#                   this run (NULL = no limit). `used` is what the run has
#                   spent so far.
#   2. check_budget if TRUE, the remaining weekly budget reported by the API.
#
# Arguments:
#   expected  expected number of records for the next query (NA/0 if unknown)
#
# Returns:
#   "ok"    the query fits
#   "skip"  this query does not fit, but a smaller one might
#   "stop"  no budget left at all; the run should end
.budget_decision <- function(client, expected, used, max_budget, check_budget) {
  fmt      <- function(x) format(x, big.mark = ",", scientific = FALSE)
  expected <- replace_na(as.numeric(expected), 0)

  # --- 1. The user's own limit for this run --------------------------------
  if (!is.null(max_budget)) {
    left <- max_budget - used
    if (left <= 0) {
      message("  max_budget reached (", fmt(used), " of ", fmt(max_budget),
              " records used in this run). Stopping.")
      return("stop")
    }
    if (expected > left) return("skip")      # skipped queries are counted in the final summary
  }

  # --- 2. The remaining weekly budget, as reported by the API --------------
  if (check_budget) {
    remaining <- .budget_remaining(client)
    if (!is.na(remaining)) {
      if (remaining <= 0) {
        message("  Weekly budget used up. Stopping; re-run when budget is available.")
        return("stop")
      }
      if (expected > remaining) return("skip")
    }
  }

  "ok"
}


# Internal: budget allowance for a SYNC run, computed once at the start.
# Sync functions send one request per post/producer, so asking the API for
# the remaining budget before every request would halve their speed.
# Instead, the allowance is the smaller of max_budget and the remaining
# weekly budget at the start, and the run tracks its own usage against it.
# Returns NULL if there is no limit.
.sync_allowance <- function(client, max_budget, check_budget, verbose = TRUE) {
  remaining <- if (check_budget) .budget_remaining(client) else NA_real_
  limits    <- c(max_budget, remaining)
  limits    <- limits[!is.na(limits)]
  if (length(limits) == 0) return(NULL)
  allowance <- min(limits)
  if (verbose) message("Budget allowance for this run: ",
                       format(allowance, big.mark = ",", scientific = FALSE), " records.")
  allowance
}


# Internal: check `max_budget` before a run starts.
.check_max_budget <- function(max_budget) {
  if (!is.null(max_budget) && (!is.numeric(max_budget) || length(max_budget) != 1 || max_budget <= 0))
    stop("`max_budget` must be NULL or a single positive number of records.")
}


# Internal: GET the "budgets" endpoint and return the `queries` usage fields
# as a named list of numbers.
.budget_fetch <- function(client) {
  response <- .get(client, "budgets")
  parsed   <- jsonlite::fromJSON(response$text, simplifyVector = TRUE)

  # The usage fields sit in a `queries` object; look in `data` too, in case
  # the API wraps the payload.
  q <- parsed$queries %||% parsed$data$queries
  if (is.null(q)) stop("Unexpected response from 'budgets': no `queries` object found.")

  list(
    current_usage    = as.numeric(q$current_usage %||% NA),
    preallocated     = as.numeric(q$preallocated_rows_for_running_queries %||% 0),
    total_usage      = as.numeric(q$total_usage %||% NA),
    max_usage_limit  = as.numeric(q$max_usage_limit %||% .mcl_limits$weekly_budget)
  )
}


# =============================================================================
# COMMENT HELPERS
# =============================================================================

# Comments are saved as ONE FILE PER POST, in one subfolder per page:
#
#   <save_dir>/<page_id>/<post_id>.rds
#
# Each job's results are written to disk as soon as the job finishes, so no
# budget is wasted if a run stops halfway through a page (e.g. when the
# weekly budget runs out). Re-runs skip posts that already have a file.
# Combine everything with load_comments(save_dir).


# Internal: reduce a posts dataframe to the columns the comment functions
# need, with standard names:
#   post_id   post ID                      (from id_col)
#   page_id   producer/page ID             (from page_col)
#   n_comm    comment count                (from count_col; NA if missing)
#   created   post creation time, UNIX sec (from time_col;  NA if missing)
# Posts with 0 comments are dropped if skip_zero = TRUE, because querying
# them returns nothing but still costs a sync request or a slot in a job.
.prepare_posts <- function(posts_df, id_col, page_col, count_col, skip_zero,
                           time_col = "creation_time") {
  for (col in c(id_col, page_col)) {
    if (!col %in% names(posts_df)) stop("Column '", col, "' not found in posts_df.")
  }

  out <- tibble(
    post_id = as.character(posts_df[[id_col]]),
    page_id = as.character(posts_df[[page_col]]),
    n_comm  = if (count_col %in% names(posts_df)) {
      suppressWarnings(as.numeric(posts_df[[count_col]]))   # stored as character after loading
    } else {
      NA_real_
    },
    created = if (time_col %in% names(posts_df)) {
      map_dbl(as.character(posts_df[[time_col]]), .to_unix) # needed for large posts only
    } else {
      NA_real_
    }
  ) |>
    distinct(post_id, .keep_all = TRUE)                      # each post only once

  if (skip_zero && count_col %in% names(posts_df)) {
    n_zero <- sum(out$n_comm == 0, na.rm = TRUE)
    if (n_zero > 0) message(n_zero, " post(s) with 0 comments skipped.")
    out <- filter(out, is.na(n_comm) | n_comm > 0)           # keep unknown counts
  }
  out
}


# Internal: convert a date/time to UNIX seconds (numeric), or NA.
# Accepts UNIX timestamps, "YYYY-MM-DD", "YYYY-MM-DD HH:MM:SS" and
# "YYYY-MM-DDTHH:MM:SS" (all interpreted as UTC).
.to_unix <- function(x) {
  if (is.null(x) || length(x) == 0 || is.na(x)) return(NA_real_)
  if (inherits(x, "POSIXt")) return(as.numeric(x))
  x <- as.character(x)
  if (grepl("^[0-9]+$", x)) return(as.numeric(x))           # already a UNIX timestamp
  t <- suppressWarnings(as.POSIXct(
    x, tz = "UTC",
    tryFormats = c("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"),
    optional = TRUE
  ))
  as.numeric(t)
}


# Internal: posts that already have a comment file in save_dir.
# Returns a character vector of post IDs (file names without .rds, taken
# from the page subfolders).
.done_posts <- function(save_dir) {
  if (!dir.exists(save_dir)) return(character(0))
  files <- list.files(save_dir, pattern = "\\.rds$", recursive = TRUE)
  files <- files[dirname(files) != "."]                      # ignore files at top level (e.g. failed_ids.rds)
  tools::file_path_sans_ext(basename(files))
}


# Internal: split a job's comments by post and save one file per post in
# its page's subfolder. Posts that returned no comments get an empty file,
# so they are recognised as collected on later runs.
#
# Arguments:
#   comments          tibble of comments returned by a job or sync query
#   job_posts         tibble with post_id and page_id of the posts in the job
#   comment_post_col  column in `comments` holding the parent post ID
.save_comments_by_post <- function(comments, job_posts, save_dir, comment_post_col = "post_id") {
  if (nrow(job_posts) == 1) {
    # Only one post in the query: every comment belongs to it, no splitting needed
    by_post <- set_names(list(comments), job_posts$post_id)
  } else if (nrow(comments) == 0) {
    by_post <- list()
  } else {
    # Several posts: the parent post column is needed to split the results
    if (!comment_post_col %in% names(comments))
      stop("Column '", comment_post_col, "' not found in comments; cannot split by post. ",
           "Set `comment_post_col` to the correct column name.")
    ids <- as.character(comments[[comment_post_col]])

    # If NO comment matches a requested post, the ID formats almost certainly
    # differ: stop before writing empty files that would look "collected"
    if (!any(ids %in% job_posts$post_id))
      stop("None of the returned comments matched a requested post ID in column '",
           comment_post_col, "'. Nothing saved.")
    if (!all(ids %in% job_posts$post_id))
      warning(sum(!ids %in% job_posts$post_id), " comment(s) did not match a requested post ID ",
              "and were not saved.")

    by_post <- split(comments, ids)                          # list keyed by parent post ID
  }

  for (k in seq_len(nrow(job_posts))) {
    pid  <- job_posts$post_id[k]
    dir  <- file.path(save_dir, job_posts$page_id[k])
    dir.create(dir, recursive = TRUE, showWarnings = FALSE)
    data <- by_post[[pid]] %||% tibble()                     # empty tibble if no comments
    saveRDS(as_tibble(data), file.path(dir, paste0(pid, ".rds")))
  }
}


# Internal: one row per post of the run log returned by the comment functions.
.log_posts <- function(job_posts, n = NA_integer_, status = "ok", note = NA_character_) {
  tibble(
    page_id = job_posts$page_id,
    post_id = job_posts$post_id,
    n       = as.integer(n),
    status  = status,
    note    = as.character(note)
  )
}


# Internal: add a "skipped" log row for every post that was not attempted
# (because the run stopped when the budget ran out).
.log_not_run <- function(log, posts) {
  log     <- bind_rows(log)
  tried   <- if (nrow(log) > 0) log$post_id else character(0)
  not_run <- filter(posts, !post_id %in% tried)
  if (nrow(not_run) > 0) {
    log <- bind_rows(log, .log_posts(not_run, status = "skipped", note = "not enough budget"))
  }
  log
}


# Internal: count comments per post in a job result, in the order of job_posts.
.n_per_post <- function(comments, job_posts, comment_post_col = "post_id") {
  if (nrow(comments) == 0) return(rep(0L, nrow(job_posts)))
  if (nrow(job_posts) == 1) return(nrow(comments))           # single post: all comments are its own
  counts <- table(as.character(comments[[comment_post_col]]))
  as.integer(replace_na(as.numeric(counts[job_posts$post_id]), 0))
}


# Internal: run ONE async comments job for a set of posts and save the
# results per post. Errors are caught and returned in the log.
#
# Arguments:
#   job_posts    tibble of posts in the job (post_id, page_id), <= 250 rows
#   base_params  job parameters other than parent_ids (fields, fetch_all, since, until)
#
# Returns a list: $ok (TRUE/FALSE) and $log (one row per post)
.comment_job <- function(client, job_posts, base_params, save_dir,
                         comment_post_col, poll_interval, verbose) {
  params <- c(base_params, list(parent_ids = paste(job_posts$post_id, collapse = ",")))

  res <- tryCatch({
    data <- .run_job(client, "facebook/comments/job", params, poll_interval, verbose = verbose)
    .save_comments_by_post(data, job_posts, save_dir, comment_post_col)   # one file per post
    data
  }, error = function(e) e)

  if (inherits(res, "error")) {
    message("  ERROR: ", conditionMessage(res))
    return(list(ok = FALSE, log = .log_posts(job_posts, status = "error",
                                             note = conditionMessage(res))))
  }

  if (verbose) message("  ", nrow(res), " comments saved for ", nrow(job_posts), " post(s).")
  list(ok = TRUE, log = .log_posts(job_posts, .n_per_post(res, job_posts, comment_post_col), "ok"))
}


# Internal: wrapper around .collect_large_post() that saves the result and
# returns the same list($ok, $log) as .comment_job().
.large_comment_job <- function(client, post, base_params, since, until,
                               max_comments, save_dir, poll_interval, verbose) {
  res <- tryCatch(
    .collect_large_post(client, post, base_params, since, until,
                        max_comments, poll_interval, verbose),
    error = function(e) e
  )

  if (inherits(res, "error")) {
    message("  ERROR: ", conditionMessage(res))
    return(list(ok = FALSE, log = .log_posts(post, status = "error", note = conditionMessage(res))))
  }

  .save_comments_by_post(res$data, post, save_dir)
  if (verbose) message("  ", nrow(res$data), " comments saved (", res$status, ").")
  list(ok = TRUE, log = .log_posts(post, nrow(res$data), res$status, res$note))
}


# Internal: collect comments for ONE post that has too many comments for a
# normal job. If the post has fewer than `max_comments` comments it is
# collected in full. Otherwise only the FIRST `max_comments` comments (by
# creation time) are collected: a binary search over the `until` filter,
# using the estimate endpoint, finds the latest cut-off whose estimate stays
# at or below `max_comments`. Each step is one sync request (~25 steps);
# estimates do not count towards the query budget.
#
# Arguments:
#   post         one-row tibble from .prepare_posts() (post_id, page_id, created)
#   base_params  job parameters without parent_ids and until
#   since, until the user's date filters (may be NULL)
#
# Returns a list: $data (tibble), $status ("ok" or "truncated"), $note
.collect_large_post <- function(client, post, base_params, since, until,
                                max_comments, poll_interval, verbose) {
  params <- c(base_params, list(parent_ids = post$post_id))
  est_path <- "facebook/comments/estimate"

  # Estimate the full post (within the user's date filters)
  est_full <- .estimate(client, est_path, compact(c(params, list(until = until))))
  if (verbose) message("  Large post ", post$post_id, ": estimated ",
                       format(est_full, big.mark = ","), " comments.")

  # Small enough after all: collect in full
  if (!is.na(est_full) && est_full <= max_comments) {
    data <- .run_job(client, "facebook/comments/job", compact(c(params, list(until = until))),
                     poll_interval, verbose = verbose)
    return(list(data = data, status = "ok", note = NA_character_))
  }

  # Binary search for the cut-off time. Lower bound: post creation (or the
  # user's `since`, or 1970). Upper bound: the user's `until`, or now.
  lo <- max(c(post$created, .to_unix(since), 0), na.rm = TRUE)
  hi <- .to_unix(until)
  if (is.na(hi)) hi <- as.numeric(Sys.time())

  for (step in 1:40) {
    if (hi - lo <= 60) break                                 # 1-minute precision is enough
    mid <- floor((lo + hi) / 2)
    est <- .estimate(client, est_path, c(params, list(until = as.integer(mid))))
    if (is.na(est)) stop("Estimate unavailable; cannot find a cut-off for post ", post$post_id)
    if (est <= max_comments) lo <- mid else hi <- mid        # keep lo below the target
  }

  cutoff <- as.integer(lo)
  if (verbose) message("  Collecting comments up to ",
                       format(as.POSIXct(cutoff, origin = "1970-01-01", tz = "UTC")), " UTC.")

  data <- .run_job(client, "facebook/comments/job", c(params, list(until = cutoff)),
                   poll_interval, verbose = verbose)
  list(
    data   = data,
    status = "truncated",
    note   = paste0("first ", nrow(data), " comments only (until ",
                    format(as.POSIXct(cutoff, origin = "1970-01-01", tz = "UTC")), " UTC)")
  )
}


# Internal: split one page's posts into consecutive chunks so that every
# chunk has at most `max_posts` posts AND (if comment counts are known) at
# most `max_comments` comments. Returns a list of character vectors.
# A single post with more than max_comments comments gets a chunk of its own.
.chunk_posts <- function(post_ids, n_comm, max_posts, max_comments) {
  n_comm  <- replace_na(n_comm, 0)        # unknown counts do not constrain chunking
  chunks  <- list()
  current <- character(0)
  total   <- 0

  for (k in seq_along(post_ids)) {
    # Start a new chunk if adding this post would break either constraint
    if (length(current) > 0 &&
        (length(current) + 1 > max_posts || total + n_comm[k] > max_comments)) {
      chunks  <- c(chunks, list(current))
      current <- character(0)
      total   <- 0
    }
    current <- c(current, post_ids[k])
    total   <- total + n_comm[k]
  }
  c(chunks, list(current))                # add the last chunk
}


# Internal: add a post_id column to comments if the API did not return one,
# so that every saved comment can be linked back to its post.
.add_post_id <- function(comments, post_id) {
  if (nrow(comments) > 0 && !"post_id" %in% names(comments)) {
    comments <- mutate(comments, post_id = post_id, .before = 1)
  }
  comments
}


# =============================================================================
# FILE AND LOGGING HELPERS
# =============================================================================

# Internal: IDs already saved as <id>.rds in save_dir (used to resume runs).
# The failed-ID log file is excluded.
.done_ids <- function(save_dir) {
  if (!dir.exists(save_dir)) return(character(0))
  list.files(save_dir, pattern = "\\.rds$") |>
    tools::file_path_sans_ext() |>
    setdiff("failed_ids")
}


# Internal: save a tibble to <save_dir>/<id>.rds. Empty tibbles are saved
# too, so "collected but 0 results" can be told apart from "not collected".
.save_one <- function(df, save_dir, id) {
  saveRDS(df, file.path(save_dir, paste0(id, ".rds")))
}


# Internal: append IDs to <save_dir>/failed_ids.rds. Batched functions skip
# these IDs on later runs; delete the file (or remove IDs) to retry them.
.log_failed <- function(save_dir, ids) {
  path     <- file.path(save_dir, "failed_ids.rds")
  existing <- if (file.exists(path)) readRDS(path) else character(0)
  saveRDS(unique(c(existing, ids)), path)
}


# Internal: read the failed-ID log (empty vector if there is none).
.read_failed <- function(save_dir) {
  path <- file.path(save_dir, "failed_ids.rds")
  if (file.exists(path)) readRDS(path) else character(0)
}


# Internal: one row of the run log returned by every collection function.
#   id     producer/page ID
#   n      number of records saved
#   status "ok", "capped" (hit a result cap), "partial", "skipped", or "error"
#   note   extra detail (e.g. the error message)
.log_row <- function(id, n = NA_integer_, status = "ok", note = NA_character_) {
  tibble(id = as.character(id), n = as.integer(n), status = status, note = as.character(note))
}


# Internal: add a "skipped" log row for every producer that was not
# attempted (because the run stopped when the budget ran out).
.log_not_run_ids <- function(log, ids) {
  log     <- bind_rows(log)
  tried   <- if (nrow(log) > 0) log$id else character(0)
  not_run <- setdiff(ids, tried)
  if (length(not_run) > 0) {
    log <- bind_rows(log, .log_row(not_run, status = "skipped", note = "not enough budget"))
  }
  log
}


# Internal: print a short summary of a run log and warn about problems.
.report_log <- function(log, what = "producer(s)") {
  if (nrow(log) == 0) { message("\nDone. Nothing was collected."); return(invisible()) }
  counts <- count(log, status)
  message("\nDone. ", paste0(counts$n, " ", counts$status, collapse = ", "), " ", what, ".")
  n_budget <- sum(log$note %in% "not enough budget")
  if (n_budget > 0) message(n_budget, " ", what, " skipped because they did not fit in the budget. ",
                            "Re-run later (or with a higher max_budget) to collect them.")
  n_bad <- sum(log$status %in% c("error", "capped", "partial"))
  if (n_bad > 0) warning(
    n_bad, " ", what, " have errors or incomplete results. ",
    "Inspect the returned log: filter(log, status != 'ok')."
  )
}
