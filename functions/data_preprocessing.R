


# Functions to read in JSON, filter, and convert to parquet ---------------

# Helper function to read and filter both types (submissions & comments) of JSON files
read_and_filter <- function(file_path, keywords, columns_to_keep, is_submission) {
  # Read the JSON file as a data.table
  con <- file(file_path, "r")
  data <- stream_in(con, verbose = FALSE)
  close(con)
  
  # Select relevant columns before converting to data.table
  selected_cols <- intersect(names(data), columns_to_keep)
  data <- data[, selected_cols, drop = FALSE]  # subset before conversion
  
  # Convert to data.table only for relevant columns
  setDT(data)
  
  # Summarize the count of submissions by subreddit before filtering
  subreddit_summary <- data.table(subreddit = data$subreddit)
  subreddit_summary <- subreddit_summary[, .N, by = subreddit]  # N counts rows per subreddit
  
  month <- str_extract(file_path, pattern = "(\\d{2})(?=\\.json$)")
  file_type <- if_else(is_submission, "submissions_", "comments_")
  summary_output_path <- file.path(output_folder, paste0(file_type, "summary_", year, "_", month, ".parquet"))
  
  # Save the subreddit summary as a Parquet file
  arrow::write_parquet(subreddit_summary, summary_output_path, compression = "zstd")
  cat("Saved subreddit summary file:", summary_output_path, "\n")
  
  # Filter submissions or comments
  if (is_submission) {
    # Combine title and selftext, filter by keyword
    data[, text := paste(title, selftext, sep = ": ")]
    data[, c("title", "selftext") := NULL]
    data <- data[str_detect(text, keywords)]
  } else {
    # Load the master list of submission IDs as a data.table
    all_submission_ids <- fread(file.path(output_folder, "all_submission_ids.parquet"))$submission_id
    data[, submission_id := str_remove(link_id, "^t3_")]
    data <- data[submission_id %in% all_submission_ids]
  }
  
  # Convert all columns to characters to prevent incompatible types
  data <- data[, lapply(.SD, as.character)]  
  
  return(data)
}

# Function to process submissions data for a year, saving monthly files
process_year_monthly_submissions <- function(year, keywords, submission_columns) {
  cat("Processing submissions for year:", year, "\n\n")
  
  # Get all monthly submission files
  submission_files <- list.files(submissions_folder, pattern = paste0(year, ".*\\.json$"), full.names = TRUE)
  
  # Set up parallel plan
  plan(multisession, workers = 2)
  
  # Process and save monthly submissions
  future_walk(submission_files, function(file) {
    monthly_submissions <- read_and_filter(
      file_path = file,
      keywords = keywords,
      columns_to_keep = submission_columns,
      is_submission = TRUE
    )
    
    # Extract month
    month <- str_extract(file, pattern = "(\\d{2})(?=\\.json$)")
    output_path <- file.path(output_folder, paste0("submissions_", year, "_", month, ".parquet"))
    
    # Save as Parquet
    arrow::write_parquet(monthly_submissions, output_path, compression = "zstd")
    cat("Saved monthly submissions file:", output_path, "\n")
  })
  
  # Reset parallel plan
  plan(sequential)
}

# Function to process comments, filtered by submission ids
process_year_monthly_comments <- function(year, comment_columns) {
  cat("Processing comments for year:", year, "\n\n")
  
  # Set up parallel plan
  plan(multisession, workers = 4)
  
  # Get all monthly comment files
  comment_files <- list.files(comments_folder, pattern = paste0(year, ".*\\.json$"), full.names = TRUE)
  
  # Process and save monthly comments
  future_walk(comment_files, function(file) {
    monthly_comments <- read_and_filter(
      file_path = file,
      keywords = NULL,
      columns_to_keep = comment_columns,
      is_submission = FALSE
    )
    
    # Extract month
    month <- str_extract(file, pattern = "(\\d{2})(?=\\.json$)")
    output_path <- file.path(output_folder, paste0("comments_", year, "_", month, ".parquet"))
    
    # Save as Parquet
    arrow::write_parquet(monthly_comments, output_path, compression = "zstd")
    cat("Saved monthly comments file:", output_path, "\n")
  })
  # Reset parallel plan
  plan(sequential)
}


# Combine monthly files into yearly Parquet files
combine_yearly_files <- function(year, file_type) {
  cat("Combining monthly files for year:", year, "\n")
  
  # Read and filter all monthly submissions
  plan(multisession, workers = 4) # set the parallelization plan
  
  # Combine monthly submissions
  files <- list.files(output_folder, pattern = paste0(file_type, "_", year, "_.*\\.parquet$"), full.names = TRUE)
  combined_files <- future_map_dfr(files, arrow::read_parquet)
  arrow::write_parquet(combined_files, file.path(output_folder, paste0(file_type, "_", year, ".parquet")), compression = "zstd")
  cat("Combined and saved yearly", file_type ,"file for", year, "\n")
  
  # Handle subreddit summary files
  summary_files <- list.files(output_folder, pattern = paste0("submissions_summary_", year, ".*\\.parquet$"), full.names = TRUE)
  
  # Read and combine all summary files
  combined_summary <- future_map_dfr(summary_files, arrow::read_parquet)
  combined_summary <- combined_summary[, .(N = sum(N)), by = subreddit] # Aggregate by subreddit to get total counts
  
  # Write combined summary to Parquet
  summary_output_path <- file.path(output_folder, paste0("submissions_summary_", year, ".parquet"))
  arrow::write_parquet(combined_summary, summary_output_path, compression = "zstd")
  cat("Combined and saved yearly subreddit summary file for", year, "\n")
  
  # Reset parallel plan to avoid lingering workers
  plan(sequential)
}

# Get all unique submissions ids after all years processed
get_unique_submission_ids <- function(submission_files, output_path) {
  # Extract the 'id' column from all submission files
  submission_ids <- submission_files %>%
    future_map_dfr(~ arrow::read_parquet(.x) %>% select(id)) %>%
    distinct(id)  # Remove duplicates
  
  # Save the unique submission IDs to a Parquet file
  arrow::write_parquet(submission_ids, output_path, compression = "zstd")
  cat("Saved unique submission IDs to", output_path, "\n")
}

# Helper function to clean the text fields
clean_text <- function(text) {
  text |> 
    replace_html() |> # remove HTML tags
    replace_url() |>                   # Remove URLs
    replace_contraction() |>           # Expand contractions
    replace_internet_slang() |>        # Normalize slang
    replace_emoji() |>                 # Convert emojis to text (optional)
    replace_punctuation() |>           # Remove punctuation
    replace_word_elongation() |>       # Normalize elongated words
    replace_non_ascii() |>             # Remove non-ASCII characters
    replace_white() |>                 # Collapse multiple spaces
    tolower()                           # Normalize case
}






