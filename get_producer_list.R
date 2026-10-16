# =============================================================================
# get_producer_list()
# =============================================================================
# Retrieve a producer list (a list of Facebook pages or other accounts) that
# was created in the Meta Content Library UI and shared with the API.
# Follows pagination if the list spans several pages.
#
# The list ID is shown in the Content Library UI when you share a producer
# list with the API (format: "YYYY-MM-DD-xxxx").
#
# Requires: mcl_utils.R
#
# Arguments:
#   client     Initialised MCL API client
#   list_id    Producer list ID, as shown in the Content Library UI
#   save_path  Optional file path ending in .rds. If supplied, the list is
#              saved there (folders are created if needed).
#   verbose    Print progress messages
#
# Returns:
#   A tibble with one row per producer. The `id` column holds the IDs to pass
#   to get_posts_sync(), get_posts_async() or get_posts_async_batched().
#
# Output:
#   Unlike the collection functions, this one RETURNS the list: assign it
#   with `<-`. Saving to disk is optional (save_path).
#
# Examples:
#   # 1. Load the list into R and save a copy
#   producers <- get_producer_list(
#     client    = client,
#     list_id   = "YOUR-LIST-ID",           # replace with your own list ID
#     save_path = "../MP_FV22/producer_list.rds"
#   )
#   glimpse(producers)
#
#   # 2. Later sessions: read the saved copy instead of calling the API
#   producers <- readRDS("../MP_FV22/producer_list.rds")
# =============================================================================

get_producer_list <- function(client, list_id, save_path = NULL, verbose = TRUE) {
  path <- paste0("lists/producers/", list_id)            # endpoint for this list
  if (verbose) message("Fetching producer list: ", path)

  # First page of the list
  response <- .get(client, path)
  pages    <- list(jsonlite::fromJSON(response$text, flatten = TRUE))

  # Follow pagination, if the list has more than one page
  while (isTRUE(client$has_next_page(response))) {
    response <- .retry(function() {
      .throttle_sync()                                   # respect 60 req/min
      client$query_next_page(response)
    })
    pages <- c(pages, list(jsonlite::fromJSON(response$text, flatten = TRUE)))
  }

  # Some responses wrap the records in `data`; others return them directly.
  # Extract the records from each page and stack them.
  producers <- pages |>
    map(~ if (is.list(.x) && !is.data.frame(.x) && !is.null(.x$data)) .x$data else .x) |>
    map(as_tibble) |>
    bind_rows()

  if (verbose) message("  ", nrow(producers), " producers retrieved.")

  # Save to disk if a path was given
  if (!is.null(save_path)) {
    dir.create(dirname(save_path), recursive = TRUE, showWarnings = FALSE)
    saveRDS(producers, save_path)
    if (verbose) message("  Saved to ", save_path)
  }

  producers
}
