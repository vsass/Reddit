
# This script takes the raw pushshift API data downloaded in monthly submission & comments files from 
# <https://academictorrents.com/details/ba051999301b109eab37d16f027b3f49ade2de13> and filters them
# for only submissions that contain keywords of interest, retains only necessary variables, 
# pulls comments from those submissions, and writes out the data in yearly parquet files


# Load packages -----------------------------------------------------------
library(arrow) # store filtered data efficiently
library(jsonlite) # read in json files
library(data.table) # more efficient for initial filtering of the data
library(tidyverse)
library(fs) # add .json extension to .zst extracted json files
library(textclean) # cleaning and normalizing text for analysis
library(furrr)

# source(file = "functions/yearly_data_summary_stats.R")
source(file = "functions/data_preprocessing.R")

# Define parameters -------------------------------------------------------

# number of cores
cores <- parallel::detectCores()
workers <- cores/2

## file paths
submissions_folder <- "data/raw/submissions/"
comments_folder <- "data/raw/comments/"
output_folder <- "data/filtered"

## keywords regex
keywords <- paste0(
  "(?i)",  # Case-insensitive matching
  "(",  # Start of group
  "weight|bmi|body mass index|", # weight-related words
  "fat|flab|obes(e|ity|ogen)|burly|",  # obesity
  "thin|skinny|slender|slim|(physically)?fit|svelte|lithe|lean|toned|muscular|", # culturally desirable body 
  "health|wellness|diet|appetite|cal(orie|s)|hung(er|r)|", # diet-culture 
  "exercise|active|movement|work[ -]?out|sedentary|", # movement
  "mental|stress|anxi(ety|ous)|depress(ion|ed|ing)|body[- ]?image|self[- ]?(esteem|worth)|", # mental health
  "eat|disorder|bing(e|ing)|anorexi(a|c)?|bulimi(a|c)?|orthorexi(a|c)?|(over|under)?[ -]?eat|",  # eating
  "muscle|bulk|size|shape|body|", # body 
  "gastric bypass|bariatric|sleeve gastr(oplasty|ectomy)|lap[- ]?band|", # surgical "treatments"
  # the following list came from <https://www.niddk.nih.gov/health-information/weight-management/prescription-medications-treat-overweight-obesity>
  # and is a combination of brand name and generics
  "contrave|naltrexone([ -]bupropion)?|tirzepatide|zepbound|", # weight-loss prescription drugs --> adults only
  "semaglutide|wegovy|saxenda|liraglutide|orlistat|xenical|alli|qsymia|phentermine([ -]topiramate)?|setmelanotide|", # 12 years old and up
  "setmelanotide|IMCIVREE|", # 6 years old and up
  "ozempic|rybelsus|victoza|mounjaro", # diabetes-related drugs (same generic, lower dose)
  ")"  # End of group 
)

# variables 
submission_columns <- c("id", "author", "created_utc", "title", "selftext", "num_comments", 
                        "score", "subreddit", "subreddit_type", "permalink", "retrieved_on", "removed")
comment_columns <- c("link_id", "id", "parent_id", "author", "created_utc", "body", 
                     "num_comments", "score", "subreddit", "subreddit_id", "permalink")

# enter manually to keep track of storage space
year <- "2013"

# Process each year -------------------------------------------------------

plan(multisession, workers = 4)

# Append .json on to files extracted from .zst using 7-zip (had to do manually) 
files_sub <- dir_ls(submissions_folder, type = "file", regexp = year)  # `fs::dir_ls()` lists all files
files_sub |> future_walk(~ file_move(.x, path_ext_set(.x, "json")))  # `file_move` renames, `path_ext_set` adds .json
plan(sequential)

# summary stats
#process_submissions_summaries(year)
#open_dataset("data/filtered/submissions_summary_2011.parquet") |> glimpse() #  submissions

# process monthly files and combine by year
process_year_monthly_submissions(2013, keywords = keywords, submission_columns = submission_columns)
combine_yearly_files(year = 2013, file_type = "submissions")

# check that combination worked
open_dataset("data/filtered/submissions_summary_2013.parquet") |> glimpse() #  submissions






# Get and save the full list of submissions ids for filtering comments
submission_files <- list.files(output_folder, pattern = "submissions_\\d{4}\\.parquet$", full.names = TRUE)
get_unique_submission_ids(submission_files, "path/to/all_submission_ids.parquet")

# After all submissions have been processed, move onto comments:

###############################################################################

plan(multisession, workers = workers)

# Append .json on to files extracted from .zst using 7-zip (had to do manually) 
files_sub <- dir_ls(comments_folder, type = "file", regexp = year)  # `fs::dir_ls()` lists all files
files_sub |> future_walk(~ file_move(.x, path_ext_set(.x, "json")))  # `file_move` renames, `path_ext_set` adds .json
plan(sequential)

future_walk(year, process_comment_summaries) # get stats before filtering 

process_year_monthly_comments(YEAR, keywords = keywords, comment_columns = comment_columns)
combine_yearly_files(year = YEAR, file_type = "comments")

# open_dataset("data/filtered/submissions_2005.parquet") |> summarise(n = n()) |> collect() # 1764 submissions
# open_dataset("data/filtered/submissions_2006.parquet") |> summarise(n = n()) |> collect() # 29187 submissions
# open_dataset("data/filtered/submissions_2007.parquet") |> summarise(n = n()) |> collect() # 95673 submissions
# open_dataset("data/filtered/submissions_2008.parquet") |> summarise(n = n()) |> collect() # 267373 submissions
# open_dataset("data/filtered/submissions_2009.parquet") |> summarise(n = n()) |> collect() # 628331 submissions
# open_dataset("data/filtered/submissions_2010.parquet") |> summarise(n = n()) |> collect() # 1169495 submissions
# open_dataset("data/filtered/submissions_2011.parquet") |> summarise(n = n()) |> collect() # 3286304 submissions
# open_dataset("data/filtered/submissions_2012.parquet") |> summarise(n = n()) |> collect() # 6649686 submissions
# open_dataset("data/filtered/submissions_2013.parquet") |> summarise(n = n()) |> collect() # 9612003 submissions
open_dataset("data/filtered/submissions_2014.parquet") |> summarise(n = n()) |> collect() #  submissions
open_dataset("data/filtered/submissions_2015.parquet") |> summarise(n = n()) |> collect() #  submissions
open_dataset("data/filtered/submissions_2016.parquet") |> summarise(n = n()) |> collect() #  submissions
open_dataset("data/filtered/submissions_2017.parquet") |> summarise(n = n()) |> collect() #  submissions
open_dataset("data/filtered/submissions_2018.parquet") |> summarise(n = n()) |> collect() #  submissions
open_dataset("data/filtered/submissions_2019.parquet") |> summarise(n = n()) |> collect() #  submissions
open_dataset("data/filtered/submissions_2020.parquet") |> summarise(n = n()) |> collect() #  submissions
open_dataset("data/filtered/submissions_2021.parquet") |> summarise(n = n()) |> collect() #  submissions
open_dataset("data/filtered/submissions_2022.parquet") |> summarise(n = n()) |> collect() #  submissions
open_dataset("data/filtered/submissions_2023.parquet") |> summarise(n = n()) |> collect() #  submissions
open_dataset("data/filtered/submissions_2024.parquet") |> summarise(n = n()) |> collect() #  submissions




open_dataset("data/filtered/submission_summary_2011.parquet") |> glimpse() |> collect()



















# Read lines 370 to 380 from the file
lines_to_check <- readLines("data/raw/submissions/RS_2013-02.json", skip = 0, n = 10000)  # Skips first 369 lines, reads 10 lines

fromJSON(lines_to_check)$title
fromJSON(lines_to_check)$selftext
