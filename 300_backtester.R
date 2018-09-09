##################################
#CONTEXT
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
source("011_utils.R")

## SET PARAMETERS
# This section is where we set parameters for the backtest
constituent_index <- "JALSH"
data_source <- "bloomberg"
market_metrics <- c()
fundamental_metrics <- c() 
# Combine market and fundamental metrics
metrics <- c(market_metrics, fundamental_metrics)
# But if no metrics are selected, just take all possible fields
if (length(metrics)==0){
  metrics <- c(fundamental_fields, market_fields)
}

# This section sets the backtest dates
start_backtest <- "2010-01-01"
end_backtest <- "2016-12-31"
periodicity <- "month"
# And the splits
train_test_split <- 0.8
# Create training and testing date ranges
dates <- seq(as.Date(start_backtest), as.Date(end_backtest), by=periodicity)
date_split <- dates[as.integer(length(dates)*train_test_split)]
train <- dates[dates < date_split]
test <- dates[dates >= date_split]
rm(dates)
# Check that test and train don't overlap
# We expect this to be 0
intersect(test, train)

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
constituent_list <- dplyr::filter(constituent_list, date >= start_backtest)
constituent_list <- dplyr::filter(constituent_list, date <= end_backtest)
# Verify that the list now only contains dates in the parameter range
max(constituent_list$date)
end_backtest
min(constituent_list$date)
start_backtest
# Issue with naming: we want to use the ticke rnames as dataframe names. 
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
# Verify there are no dates after the backtest in
# the test_data list
max(unlist(lapply(test_data, function(x) max(x$date))))
end_backtest
# We dropped these tickers because there was no data on them
setdiff(metadata$ticker, names(test_data))
# Get object size of test data
print(paste("Backtest dataset object size", 
            format(object.size(test_data), units="auto", standard = "IEC")))

# Step through the backtest dates
##############################################
# this is wherew a for loop should start, scoring each date.
# for (execution date in train){
execution_date <- train[24]

# Get most recent constituent members for the execution date
index_members <- (constituent_list %>%
                    filter( date <= execution_date) %>%
                    filter( date == max(date)))
# Filter dataset to only include time-appropriate members
algo_data <- test_data[names(test_data) %in% index_members$ticker]
# Check that constituent members and algo members are identical
setdiff((index_members$ticker), names(algo_data))
setdiff(names(algo_data), (index_members$ticker))
# Check what has been dropped from master dataset
# to eliminate survivorship bias
setdiff(names(test_data), names(algo_data))
# How many constituents are we looking at now?
length(test_data)
length(algo_data)
# For each ticker, drop timestamp duplication
algo_data <- lapply(algo_data, 
                   function(x) 
                     mutate(x, key=paste(date, metric, source, sep = "|")) %>%
                     arrange(desc(key)) %>%
                     filter(key != lag(key, default="0")) %>% 
                     select(-key))
# Get algo_data object size
print(paste("Backtest dataset object size after dropping duplicates ", 
            format(object.size(algo_data), units="auto", standard = "IEC")))
# NOTE: Not sure why this is such a big difference. There shouldn't be many different 
# values. Compacting isn't working I think.

# 3. Cast ticker data into FLATFILE form
algo_data <- sapply(algo_data, 
                              function(x)
                     x %>% reshape2::dcast(date + timestamp + source ~ metric))
# 3. LOOKAHEAD BIAS: Drop any row entries after execution date 
algo_data <- lapply(algo_data, 
                             function(x) 
                               (filter(x, date <= execution_date)))
# 4. Drop empty dataframes
algo_data <- algo_data[sapply(algo_data, 
                              function(x) (dim(x)[1]) > 0)]
setdiff(names(test_data), names(algo_data))
# 5. Verify no look-ahead data
execution_date
max(unlist(lapply(algo_data, 
                  function(x) max(x$date))))
# 6. Drop any missing columns
not_all_na <- function(x) {!all(is.na(x))}
algo_data <- sapply(algo_data, 
                         function(x) x %>% select_if(not_all_na))
############# HERE I AM #################
# 7. Fill values up using https://tidyr.tidyverse.org/reference/fill.html
# fill function
# sorting constituents descending
ascending <- algo_data %>% arrange(date)
filled <- fill(ascending, names(ascending))
filled <- filled %>% arrange(desc(date))
View(filtered)
View(filled)

# When computing excess return, we should use
# a JALSH
# b Tickers including those without data
# c Tickers only inclusing those with complete data
# WHICH OF ABOVE?



# IDEA: SET PARAMETERS AND FUNCTION, AND THEN RUN BACKTEST AND GET A BACKTEST OBJECT BACK. 
# BACKTEST OBJECT SHOULD HAVE A WHOLE LOT OF STUFF, NOT JUST THE 
# RETURN. 
# WHAT?
# RISK STUFF
# RETURN STUFF
