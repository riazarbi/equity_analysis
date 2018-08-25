# Clear environment
rm(list=ls())
# BACKTESTER
source("011_utils.R")
#View(market_fields) 
#View(metadata_fields)
#View(fundamental_fields)

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

start_backtest <- "2010-01-01"
end_backtest <- "2016-12-31"
periodicity <- "month"
train_test_split <- 0.8

# Create training and testing date ranges
dates <- seq(as.Date(start_backtest), as.Date(end_backtest), by=periodicity)
date_split <- dates[as.integer(length(dates)*train_test_split)]
train <- dates[dates < date_split]
test <- dates[dates >= date_split]
# Check that test and train don't overlap
intersect(test, train)

# Load datasets to memory

# Load constituent list
constituent_list <- read_csv(
  file.path(dataset_directory, "constituent_list", "constituent_list.csv"), 
  col_types = cols(
    .default = "c",
    date = col_date(format = "%Y%m%d")
  )
)

# Filter constituent_list source
constituent_list <- dplyr::filter(constituent_list, grepl(data_source, source))
# Filter constituent_list index
constituent_list <- dplyr::filter(constituent_list, grepl(constituent_index, index))
# Filter constituent_list dates
max(constituent_list$date)
min(constituent_list$date)
constituent_list <- dplyr::filter(constituent_list, date >= start_backtest)
constituent_list <- dplyr::filter(constituent_list, date <= end_backtest)
max(constituent_list$date)
end_backtest
min(constituent_list$date)
start_backtest

# replace ticker whitepace with _ for so they can be legal variable names
constituent_list$ticker <- str_replace_all(constituent_list$ticker," ","_")

# BUILD DATASETS
# Rationale for this structure: build a partitioned dataset for each date of a backtest,
# that shows the backtester the best approximation of what data it would be presented with at
# that date.
# Step 0: Get list of tickers
tickers <- constituent_list$ticker
tickers <- unique(tickers)

# Step 1: Load metadata of ALL tickers
metadata <- read_csv(file.path(dataset_directory, "metadata_array", "metadata_array.csv"), 
                     col_types = cols(.default = "c"))

# Rename TICKER_AND_EXCH_CODE column to ticker
metadata <- rename(metadata, ticker = TICKER_AND_EXCH_CODE)
# replace ticker whitepace with _ for so they can be legal variable names
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

# Infer marketdata filename for each ticker
metadata$marketdata_filename <- str_replace_all(metadata$ticker," ","_")
metadata$marketdata_filename <- paste(metadata$marketdata_filename, "_Equity.csv", sep="")
# Infer fundamental filename for each ticker
metadata$fundamental_filename <- str_replace_all(metadata$ID_ISIN," ","_")
metadata$fundamental_filename <- paste(metadata$fundamental_filename, "_Equity.csv", sep="")

# Remove tickers list
rm(tickers)
#########################
#NBNBNBNB: Metadata dataset will need to be timestamp filtered
# But I haven't done that here #

# Step 2: Build an unfiltered dataset
# the idea for this loop is to create a list of data frames
# Each dataframe will be one ticker
# Each row will be one day
# Each column will be one variable (eg price, close, PE etc)

test_data <- list()
for (i in 1:length(metadata$ticker)){
  ticker <- (metadata$ticker[i])
  #print(paste("Adding", ticker, "to dataset"))
  marketdata_filename <- metadata$marketdata_filename[i]
  fundamental_filename <- metadata$fundamental_filename[i]
  market_data_filepath <- file.path(dataset_directory, "ticker_market_data", marketdata_filename)
  fundamental_filepath <- file.path(dataset_directory, "ticker_fundamental_data", fundamental_filename)
  if (!file.exists(market_data_filepath)) {
    print("No market data. Skipping")
  }
  else if (!file.exists(fundamental_filepath)) {
    print("No fundamental data. Skipping")
  }
  else {
  market_data <- read_csv(market_data_filepath, 
                       col_types = cols(.default = "c", value = "d"))
  fundamental_data <- read_csv(fundamental_filepath, 
                          col_types = cols(.default = "c", value = "d"))
  ticker_data <- bind_rows(market_data, fundamental_data)
  # Dropping metrics that are not required for the backtest
  ticker_data <- dplyr::filter(ticker_data, metric %in% metrics)
  # Dropping dates that are after the max_date
  ticker_data <- dplyr::filter(ticker_data, date <= end_backtest)
  # append ticker datasets to list
  test_data[[ticker]] <- ticker_data
  }
}

# Max date in test dataset
max(unlist(lapply(test_data, function(x) max(x$date))))
# Dropped these tickers because there was no data on them
setdiff( metadata$ticker, names(test_data))

# We now have a test set
# Get some object sizes
print(paste("Backtest dataset object size", 
            format(object.size(test_data), units="auto", standard = "IEC")))

end_backtest

################################################
# NOTE ON CLEANING AND INTERPOLATION
# NA values take very little space. So keep the NAs in the dataframe as long as possible
# See for proof:
m <- data.frame(matrix(1000, ncol = 5000, nrow = 5000))
format(object.size(m), units="auto", standard = "IEC")
n <- m[m == 1000] <- NA
format(object.size(n), units="auto", standard = "IEC")
rm(m)
rm(n)
# Because of this, we defer cleaning and interpolation
# until much later, when the datasets are smaller.
# The tradeoff is time, because we will repeate these cleaning operaitons often.
# THEN AGAIN
# Maybe everything from log directory to test should be done in-backtest...

# Remove all objects not needed for backtest from environment
rm(fundamental_data)
rm(market_data)
rm(ticker_data)
rm(fundamental_filename)
rm(fundamental_filepath)
rm(i)
rm(market_data_filepath)
rm(marketdata_filename)
rm(ticker)
rm(dates)

# Step through the backtest dates
##############################################
# this is wherew a for loop should start, scoring each date.
# for (execution date in train){
execution_date <- train[24]
# Get constituent list
# Get most recent ticker list for a particular date
# This code removes all dates from master constituent list
# That are greater than execution date.
# Then selects only those rows which are the max of the subsetted 
# dataframe. In plain language, it gets the most recent 
# ticker list for a given backtest date.
index_members <- (constituent_list %>%
                  filter( date <= execution_date) %>%
                  filter( date == max(date)))


# 1. SURVIVORSHIP BIAS: Subset tickers to last published
# constituent list
algo_data <- test_data[names(test_data) %in% index_members$ticker]
# Check that constituent memebers and algo members are itentical
setdiff((index_members$ticker), names(algo_data))
setdiff(names(algo_data), (index_members$ticker))
# Check what has been dropped from master dataset
# To eliminate survivorship bias
setdiff(names(test_data), names(algo_data))
# How many constituents are we looking at now?
length(test_data)
length(algo_data)

# 2. ISSUE: DROP TIMESTAMP DUPLICATION
# NB NOT DOUBLE CHECKED YET - MAKE SURE THIS WORKS

algo_data <- lapply(algo_data, 
                   function(x) 
                     mutate(x, key=paste(date, metric, source, sep = "|")) %>%
                     arrange(desc(key)) %>%
                     filter(key != lag(key, default="0")) %>% 
                     select(-key))
# Get some object sizes
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
