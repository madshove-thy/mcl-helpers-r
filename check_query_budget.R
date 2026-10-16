# =============================================================================
# check_query_budget()
# =============================================================================
# Check how much of the weekly MCL query budget has been used and how much is
# left. Optionally test whether a planned collection fits in what remains.
#
# The budget is 500,000 records per user over a rolling 7-day window (counted
# back from the current second), shared between the Content Library UI and
# the API. Every post or comment returned counts as one record.
#
# Requires: mcl_utils.R
#
# Arguments:
#   client   Initialised MCL API client
#   planned  Optional number of records you plan to collect, e.g. the total
#            comment count of the posts you want comments for. If supplied,
#            the function reports whether it fits in the remaining budget.
#   verbose  Print a summary to the console
#
# Returns (invisibly):
#   A one-row tibble with the budget fields:
#     max_usage_limit  total budget
#     current_usage    records already used in the last 7 days
#     preallocated     records reserved for async jobs that are still running
#     total_usage      current_usage + preallocated
#     remaining        max_usage_limit - total_usage
#     pct_used         share of the budget used (0-100)
#     planned, fits    only if `planned` was supplied
#
# Examples:
#   # 1. Print the current budget
#   check_query_budget(client)
#
#   # 2. Will collecting all comments on these posts fit?
#   check_query_budget(
#     client  = client,
#     planned = sum(as.numeric(posts$statistics.comment_count), na.rm = TRUE)
#   )
#
#   # 3. Use the remaining budget in code, e.g. spend at most half of it
#   budget <- check_query_budget(client, verbose = FALSE)
#   log <- get_comments_async_batched(client, posts, save_dir = "../MP_FV22/comments/",
#                                     max_budget = budget$remaining / 2)
# =============================================================================

check_query_budget <- function(client, planned = NULL, verbose = TRUE) {
  # Fetch current usage from the "budgets" endpoint
  b <- .budget_fetch(client)

  # Collect the fields into a tibble and derive the remaining budget
  out <- tibble(
    max_usage_limit = b$max_usage_limit,
    current_usage   = b$current_usage,
    preallocated    = b$preallocated,
    total_usage     = b$total_usage,
    remaining       = b$max_usage_limit - b$total_usage,
    pct_used        = round(100 * b$total_usage / b$max_usage_limit, 1)
  )

  # If a planned collection size is supplied, check whether it fits
  if (!is.null(planned)) {
    out <- out |> mutate(planned = planned, fits = planned <= remaining)
  }

  if (verbose) {
    fmt <- function(x) format(x, big.mark = ",", scientific = FALSE)   # 500000 -> "500,000"
    message("MCL query budget (rolling 7 days):")
    message("  Limit:        ", fmt(out$max_usage_limit))
    message("  Used:         ", fmt(out$current_usage))
    message("  Preallocated: ", fmt(out$preallocated), " (running async jobs)")
    message("  Remaining:    ", fmt(out$remaining), "  (", out$pct_used, "% used)")
    if (!is.null(planned)) {
      if (out$fits) {
        message("  Planned:      ", fmt(planned), " -> fits in the remaining budget.")
      } else {
        message("  Planned:      ", fmt(planned), " -> exceeds the remaining budget by ",
                fmt(planned - out$remaining), ".")
      }
    }
  }

  invisible(out)
}
