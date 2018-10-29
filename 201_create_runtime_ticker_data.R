##################################
# CONTEXT
# This script takes slow moving data which has been loaded into memory 
# and returns an appropriate runtime_ticker_data dataset for passing to the portfolio
# weighting algorithm.
##################################

## ENVIRONMENT SET UP
print("")
print(paste("Runtime date:", runtime_date))
# Assuming the necessary files have been loaded into the environment already.
print("NEXT: Preparing datasets for portfolio weight computation")
# Time the script
begin <- Sys.time()

# Running environment checks
print("Checking that slow moving data has been loaded to memory.")
# Making sure the slow moving data has been loaded
if(!exists("ticker_data_load_date")){
  print("WARNING: Slow moving data has not been loaded.")
  print("FIX: Reloading slow moving data.")
  source("200_load_slow_moving_data.R")
  } else {
    # making sure the slow moving data is not stale; if it is reload it
    if(difftime(begin, ticker_data_load_date,  units="days") >= 1) {
      print("WARNING: Slow moving data was loaded more than 24 hours ago, and might be stale.")
      print("FIX: Reloading slow moving data.")
      source("200_load_slow_moving_data.R")
    }
  }

get_runtime_dataset <- function(execution_date, constituent_list, ticker_data) {
  # Get most recent constituent members for the execution date
  index_members <- (constituent_list %>%
                    filter(date <= execution_date) %>%
                    filter(date == max(date)))
  # SURVIVORSHIP BIAS: 
  # FILTER: to only include time-appropriate index members
  runtime_ticker_data <- ticker_data[names(ticker_data) %in% index_members$ticker]
  # CHECK: that constituent members and algo members are identical
  #setdiff((index_members$ticker), names(runtime_ticker_data))
  #setdiff(names(runtime_ticker_data), (index_members$ticker))
  # CHECK: what has been dropped from master dataset?
  #setdiff(names(ticker_data), names(runtime_ticker_data))
  # CHECK: How many constituents are we looking at now?
  #length(ticker_data)
  #length(runtime_ticker_data)
  
  # TRANSFORM: into FLATFILE form
  # NB: IVE MOVED THIS EARLIER
  # runtime_ticker_data <- sapply(runtime_ticker_data, 
  #                    function(x)
  #                      x %>% reshape2::dcast(date + source ~ metric))
  
  # LOOKAHEAD BIAS: 
  # FILTER: any row entries after execution date 
  runtime_ticker_data <- lapply(runtime_ticker_data, 
                      function(x) 
                        (filter(x, date <= execution_date)))
  # CLEANUP: Drop empty dataframes
  runtime_ticker_data <- runtime_ticker_data[sapply(runtime_ticker_data, 
                                function(x) (dim(x)[1]) > 0)]
  #setdiff(names(ticker_data), names(runtime_ticker_data))
  
  # CHECK: verify no look-ahead data
  #execution_date
  #max(unlist(lapply(runtime_ticker_data, 
  #                  function(x) max(x$date))))
  # CLEANUP: drop any missing columns
  not_all_na <- function(x) {!all(is.na(x))}
  runtime_ticker_data <- sapply(runtime_ticker_data, 
                      function(x) x %>% select_if(not_all_na))
  # FILL: fill NA values with last known value
  runtime_ticker_data <- lapply(runtime_ticker_data, 
                      function(x) x %>% arrange(date)
                      %>% fill(names(x))
                      %>% arrange(desc(date)))
  # CHECK: Count number of NA values
  # there will be some because early values don't have any data to backfill from
  #sapply(runtime_ticker_data, function(x) sum(is.na(x)))
  return(runtime_ticker_data)
}


# Compute a new runtime_ticker_dataset only if it doesn't exist, 
# OR the runtime date has changed from the last time.
if(!exists("runtime_ticker_data") || 
   !exists("last_runtime_date") ||
   date(runtime_date) != last_runtime_date) {
  print("A new runtime_ticker_data dataframe needs to be generated.")
         runtime_ticker_data <- get_runtime_dataset(runtime_date, constituent_list, ticker_data)
         last_runtime_date <- date(runtime_date)
         }

# Print how long the script took to run
end <- Sys.time()
print(end - begin)