# =============================================================================
# load_comments()
# =============================================================================
# Combine the per-post comment files saved by get_comments_sync(),
# get_comments_async() and get_comments_async_batched() into one tibble.
#
# Expects the folder layout used by those functions:
#
#   <save_dir>/<page_id>/<post_id>.rds
#
# All columns are converted to character before combining, because column
# types can differ between files (e.g. a column that is all NA in one file).
# Convert columns back to numbers/dates as needed after loading.
#
# Arguments:
#   save_dir  Folder the comments were saved to
#   pages     Optional character vector of page IDs to load (default: all)
#   verbose   Print a summary
#
# Returns:
#   A tibble with one row per comment, plus two columns taken from the
#   folder structure: `page_id` and `parent_post_id` (the post the file
#   belongs to).
#
# Examples:
#   # 1. All comments
#   comments <- load_comments("../MP_FV22/comments/")
#
#   # 2. Only selected pages
#   comments <- load_comments("../MP_FV22/comments/", pages = c("123456789", "987654321"))
#
#   # 3. Attach post information (e.g. post text, page name) to each comment
#   comments <- comments |>
#     left_join(posts, by = join_by(parent_post_id == id), suffix = c("", "_post"))
#
#   # 4. Comments per page
#   count(comments, page_id, sort = TRUE)
# =============================================================================

load_comments <- function(save_dir, pages = NULL, verbose = TRUE) {
  # All per-post files, i.e. .rds files inside a page subfolder
  files <- list.files(save_dir, pattern = "\\.rds$", recursive = TRUE)
  files <- files[dirname(files) != "."]                     # skip top-level files (failed_ids.rds)

  # Optionally restrict to selected pages
  if (!is.null(pages)) files <- files[dirname(files) %in% as.character(pages)]
  if (length(files) == 0) {
    message("No comment files found in ", save_dir)
    return(tibble())
  }

  # Read each file, cast to character, and record which page/post it came from
  comments <- map(files, function(f) {
    readRDS(file.path(save_dir, f)) |>
      mutate(across(everything(), as.character)) |>
      mutate(page_id        = dirname(f),
             parent_post_id = tools::file_path_sans_ext(basename(f)),
             .before = 1)
  }) |>
    bind_rows()

  if (verbose) message("Loaded ", format(nrow(comments), big.mark = ","), " comments from ",
                       length(files), " post file(s) on ",
                       n_distinct(dirname(files)), " page(s).")
  comments
}
