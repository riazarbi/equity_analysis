###################################################
# CONTEXT
# This script 
# (1) takes slow moving data which has been loaded into memory 
# and returns an appropriate runtime_ticker_data dataset for passing to the portfolio
# weighting algorithm.
# (2) passes the runtime_ticker_data to the portfolio weighting algorithm and 
# returns a target_weights dataframe 
####################################################

# LOAD LIBRARIES
library(magrittr)

# GET RUNTIME DATASET
# Expects: 
#   execution_date
#   constituent_list
#   ticker_data
# Returns:
#   runtime_ticker_data
get_runtime_dataset <- function(execution_date, constituent_list, ticker_data) {
  # Get most recent constituent members for the execution date
  print(paste("ACTION: Getting", constituent_index, "members at", execution_date, "according to", data_source))
  index_members <- (constituent_list %>%
                    filter(date <= execution_date) %>%
                    filter(date == max(date)))
  
  # SURVIVORSHIP BIAS
  print("ACTION: Filtering for SURVIVORSHIP BIAS...") 
  # FILTER: to only include time-appropriate index members
  runtime_ticker_data <- ticker_data[names(ticker_data) %in% index_members$ticker]
  # CHECK: that constituent members and algo members are identical
  print(paste("CHECKING FOR ISSUES:", (setdiff((index_members$ticker), names(runtime_ticker_data)))))
  print(paste("CHECKING FOR ISSUES:", (setdiff(names(runtime_ticker_data), (index_members$ticker)))))
  # CHECK: what has been dropped from master dataset?
  print(paste("DROPPED:", 
              length((setdiff(names(ticker_data), names(runtime_ticker_data)))),
              "/",
              length(ticker_data),
              "tickers from ticker_data because they are not in the",
              execution_date,
              "constituent list."))
  # LOOKAHEAD BIAS
  print("ACTION: Filtering for LOOKAHEAD BIAS...") 
  # FILTER: any row entries after execution date 
  runtime_ticker_data <- lapply(runtime_ticker_data, 
                      function(x) 
                        (filter(x, date <= execution_date)))
  # CLEANUP: Drop empty dataframes
  runtime_ticker_data <- runtime_ticker_data[sapply(runtime_ticker_data, 
                                function(x) (dim(x)[1]) > 0)]
  # CHECK: verify no look-ahead data
  print(paste("CHECK: execution date is:", execution_date))
  print(paste("CHECK: latest date in runtime_ticker_data is:", 
              zoo::as.Date(max(unlist(lapply(runtime_ticker_data, function(x) max(x$date)))))))
  # CLEANUP: drop any missing columns
  print("ACTION: Dropping ticker columns with no data...")
  not_all_na <- function(x) {!all(is.na(x))}
  runtime_ticker_data <- sapply(runtime_ticker_data, 
                      function(x) x %>% select_if(not_all_na))
  # FILL: fill NA values with last known value
  print("ACTION: Backfilling NA values with last known value")
  runtime_ticker_data <- lapply(runtime_ticker_data, 
                      function(x) x %>% arrange(date)
                      %>% fill(names(x))
                      %>% arrange(desc(date)))
  # CHECK: Count % of NA values
  # there will be some because early values don't have any data to backfill from
  #sort(sapply(runtime_ticker_data, function(x) round(sum(is.na(x)/(dim(x)[1]*dim(x)[2]))*100,0)), decreasing=TRUE)
  return(runtime_ticker_data)
}

#########################################################################################

