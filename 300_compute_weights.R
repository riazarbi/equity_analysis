##################################
# CONTEXT
# By the time this script runs you have scraped Bloomberg
# and saved the data in the correct format in the datalog folder. 
# The datalog data has been used to update the per-ticker datasets
# the constituent lists and the metadata arrays.
# So the purpose of this script is to use the datasets to build
# An in memory list of dataframes that can be passed to a backtester.
# This list of dataframes will have the following structure:
# Each dataframe will have a row for each date, and a column for each metric.
# Each NA value will be filled with the last known value
# There should be two more dataframes - 
# The constituent list dataframe has a list of all constituents for
# each date in the backtest
# The metadata array should have metadata for each constituent
##################################

## ENVIRONMENT SET UP
#Clear environment and load common functions
rm(list=ls())
source('shared_functions.R')
source("algorithm.R")

## BUILD DATASETS
# Load constituent list
constituent_list <- read_csv(
  file.path(dataset_directory, "constituent_list", "constituent_list.csv"), 
  col_types = cols(
    .default = "c",
    date = col_date(format = "%Y%m%d")
  )
)
# Filter constituent_list source to only include the parameter specified source
constituent_list <- dplyr::filter(constituent_list, grepl(data_source, source))
# Filter constituent_list index to only include the parameter specified index
constituent_list <- dplyr::filter(constituent_list, grepl(constituent_index, index))
# Filter constituent_list dates
# Check initial date range
max(constituent_list$date)
min(constituent_list$date)
# Perform the date selection
# Make sure to include the last constituent list before the backtest date
# otherwise the backtester won't know what the weights should be for the beginning
# of the backtest
first_constituent_date <- max((constituent_list %>%
                    filter( date <= start_backtest))$date)
constituent_list <- dplyr::filter(constituent_list, date >= first_constituent_date)
constituent_list <- dplyr::filter(constituent_list, date <= end_backtest)
rm(first_constituent_date)
# Verify that the list now only contains dates in the parameter range
max(constituent_list$date)
end_backtest
min(constituent_list$date)
start_backtest
# Issue with naming: we want to use the ticker names as dataframe names. 
# But the tickers all have whitespace.
# So wereplace ticker whitepace with _  
# so they can be legal variable names
constituent_list$ticker <- str_replace_all(constituent_list$ticker," ","_")

# Get a list of all the tickers in the constituent list
tickers <- constituent_list$ticker
tickers <- unique(tickers)

# Load metadata array
metadata <- read_csv(file.path(dataset_directory, "metadata_array", "metadata_array.csv"), 
                     col_types = cols(.default = "c"))
# Rename mtadata TICKER_AND_EXCH_CODE column to ticker
metadata <- rename(metadata, ticker = TICKER_AND_EXCH_CODE)
# Replace ticker whitepace with _
# so they can be legal variable names (as above)
metadata$ticker <- str_replace_all(metadata$ticker," ","_")
# Filter metadata to just tickers that exist in backtest universe
metadata <- dplyr::filter(metadata,  ticker %in% tickers)
# Cast metadata into flatfile form (col per variable, row per ticker)
metadata <- reshape2::dcast(metadata, ticker + timestamp + source ~ metric)
# Check what tickers didn't make it because they aren't in the metadata file
length(tickers)
length(metadata$ticker)
setdiff(tickers, metadata$ticker)
# Move from constituent-based list to metadata-based list
print(paste("Dropping", 
            setdiff(tickers, metadata$ticker),
            "tickers from constituent list because there is no metadata on them"))
# Remove tickers list
# No longer needed since we are using the metadata array
rm(tickers)

# At this point we have a metadata dataframe of every ticker in
# the backtest. We also have a long constituent membership dataframe
# that tells us which tikcers belong to the index for 
# a particular period.

# Build list of ticker dataframes
# Create a filename column in the metadata array for marketdata lookups.
metadata$marketdata_filename <- str_replace_all(metadata$ticker," ","_") %>% 
                                   paste("_Equity.csv", sep="")
# Infer fundamental filename for each ticker
metadata$fundamental_filename <- str_replace_all(metadata$ID_ISIN," ","_") %>%
                                   paste("_Equity.csv", sep="")

#########################
# NBNBNBNB: Metadata dataset will need to be timestamp filtered
# But I haven't done that here #
#########################

# Load each ticker into a dataframe
test_data <- list()
for (i in 1:length(metadata$ticker)){
  ticker <- (metadata$ticker[i])
  cat("\r", paste("Loading", ticker, "              "))
  # Define the file paths of the datasets
  marketdata_filename <- metadata$marketdata_filename[i]
  fundamental_filename <- metadata$fundamental_filename[i]
  market_data_filepath <- file.path(dataset_directory, 
                                    "ticker_market_data", marketdata_filename)
  fundamental_filepath <- file.path(dataset_directory, 
                                    "ticker_fundamental_data", fundamental_filename)
  # Load ticker data
  if (file.exists(market_data_filepath) & 
      file.exists(fundamental_filepath)) {
  market_data <- read_csv(market_data_filepath, 
                          col_types = cols(.default = "c", value = "d"))
  fundamental_data <- read_csv(fundamental_filepath, 
                               col_types = cols(.default = "c", value = "d"))
  ticker_data <- bind_rows(market_data, fundamental_data)
  rm(market_data)
  rm(fundamental_data)
  } else if (file.exists(market_data_filepath)) {
    ticker_data <- read_csv(market_data_filepath, 
                            col_types = cols(.default = "c", value = "d"))
  } else if (file.exists(fundamental_filepath)) {
    ticker_data <- read_csv(fundamental_filepath, 
                            col_types = cols(.default = "c", value = "d"))
  } else {print(paste("No data for", ticker))}
  # drop metrics that are not required for the backtest
  ticker_data <- dplyr::filter(ticker_data, metric %in% metrics)
  # drop dates that are outside of the range
  ticker_data <- dplyr::filter(ticker_data, date <= end_backtest)
  # use only the most recent timestamped value for each
  # metric for each date
  ticker_data <- ticker_data %>%
                        mutate(key=paste(date, metric, source, sep = "|")) %>%
                        arrange(desc(key)) %>%
                        filter(key != lag(key, default="0")) %>% 
                        select(-key, -timestamp)
  # append ticker datasets to list
  test_data[[ticker]] <- ticker_data
  rm(fundamental_filename)
  rm(fundamental_filepath)
  rm(ticker_data)
  rm(market_data_filepath)
  rm(marketdata_filename)
  rm(ticker)
}
rm(i)
# CLEANUP: Drop empty dataframes
test_data <- test_data[sapply(test_data, 
                              function(x) (dim(x)[1]) > 0)]
# Verify there are no dates after the backtest in
# the test_data list
max(unlist(lapply(test_data, function(x) max(x$date))))
end_backtest
# We dropped these tickers because there was no data on them
setdiff(metadata$ticker, names(test_data))
# Get object size of test data
print(paste("Backtest dataset object size", 
            format(object.size(test_data), units="auto", standard = "IEC")))


# Define a function to filter the test_data into a dataset to pass to 
# the backtester for a particular date
get_algo_dataset <- function(execution_date) {
    # Get most recent constituent members for the execution date
    index_members <- (constituent_list %>%
                    filter( date <= execution_date) %>%
                    filter( date == max(date)))
    # SURVIVORSHIP BIAS: 
    # FILTER: to only include time-appropriate index members
    algo_data <- test_data[names(test_data) %in% index_members$ticker]
    # CHECK: that constituent members and algo members are identical
    setdiff((index_members$ticker), names(algo_data))
    setdiff(names(algo_data), (index_members$ticker))
    # CHECK: what has been dropped from master dataset?
    setdiff(names(test_data), names(algo_data))
    # CHECK: How many constituents are we looking at now?
    length(test_data)
    length(algo_data)

    # TRANSFORM: into FLATFILE form
    # Why not earlier?
    algo_data <- sapply(algo_data, 
                              function(x)
                         x %>% reshape2::dcast(date + source ~ metric))
    # LOOKAHEAD BIAS: 
    # FILTER: any row entries after execution date 
    algo_data <- lapply(algo_data, 
                             function(x) 
                               (filter(x, date <= execution_date)))
    # CLEANUP: Drop empty dataframes
    algo_data <- algo_data[sapply(algo_data, 
                              function(x) (dim(x)[1]) > 0)]
    setdiff(names(test_data), names(algo_data))

    # CHECK: verify no look-ahead data
    execution_date
    max(unlist(lapply(algo_data, 
                  function(x) max(x$date))))
    # CLEANUP: drop any missing columns
    not_all_na <- function(x) {!all(is.na(x))}
    algo_data <- sapply(algo_data, 
                         function(x) x %>% select_if(not_all_na))
    # FILL: fill NA values with last known value
    algo_data <- lapply(algo_data, 
                    function(x) x %>% arrange(date)
                                  %>% fill(names(x))
                                  %>% arrange(desc(date)))
    # CHECK: Count number of NA values
    # there will be some because early values don't have any data to backfill from
    sapply(algo_data, function(x) sum(is.na(x)))
    return(algo_data)
}



# Here's how you test the backtest algorithm
# First, re-source so any changes can be incorporated
source("algorithm.R")
test_target_weight <- compute_weights(get_algo_dataset(sample(train,1)))
# Verify weights sum to 1
sum(test_target_weight$weights)
# Check how many consitutents there are
nrow(test_target_weight)
# Verify they are all unique (ie same numer as nrow)
length(unique(test_target_weight$portfolio_members))

# RUN THE ALGO ACROSS ALL THE DATA
# remove target_weights object is it exists
# to ensure we aren't appending to an old weight
if (exists("target_weights")) {
  rm(target_weights)
}
# Walk along the backtest dates creating ideal portfolio weights
for (i in seq_along(train)) {  
  # time each loop
  begin <- Sys.time()
  now <- train[i]
  # Get the dataset for the particular date 
  algo_data <- get_algo_dataset(now)
  if (!exists("target_weights")) {
  target_weights <- compute_weights(algo_data)
  target_weights$date <- now
  } else 
  {
    next_target_weights <- compute_weights(algo_data)
    next_target_weights$date <- now
    target_weights <- bind_rows(target_weights, next_target_weights)
    rm(next_target_weights)
  }
  end <- Sys.time()
  #print("Time Elapsed for this section:")
  #print(end - begin)
  message <- paste("Last date done: ", now, 
                   ". Speed approximately ", as.integer(end - begin), 
                   " seconds per iteration.                 ", sep="")
  cat("\r", message)
  sum(target_weights$weights)
  
 
}

# Save weights and companio algorithm to datalog
# Shared filename attributes
timestamp <- as.numeric(as.POSIXct(Sys.time()))*10^5
data_source <- "self"
data_type <- "backtest" 
algorithm_name <- "cap_weighted"
# Weights-specific filename attributes
weights_data_identifier <- paste(algorithm_name, "_weights", sep="") 
weights_file_string <- paste(timestamp, data_source, data_type, weights_data_identifier, sep = "__")
weights_file_string <- paste(weights_file_string, ".csv", sep = "")
weights_file_string <- file.path(datalog_directory, weights_file_string)
# Algorithm-specific filename attributes
algorithm_data_identifier <- paste(algorithm_name, "_algorithm", sep="")
algorithm_file_string <- paste(timestamp, data_source, data_type, algorithm_data_identifier, sep = "__")
algorithm_file_string <- paste(algorithm_file_string, ".R", sep = "")
algorithm_file_string <- file.path(datalog_directory, algorithm_file_string)

# Save the files
write.table(target_weights, weights_file_string, row.names = FALSE, sep = ",")
file.copy(from = "algorithm.R", to = datalog_directory, overwrite = TRUE)
file.rename(from = file.path(datalog_directory, "algorithm.R"), to=algorithm_file_string)
