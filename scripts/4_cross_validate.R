# Define paths
rm(list=ls())
# Source the scripts and define functions needed to be able to compute portfolio values
source("R/set_paths.R")

# get stock price function
get_stock_last_price <- function(ticker, query_date) {
  last <- NA
  result = tryCatch({
  last <- price_data[[ticker]] %>% 
      filter(date<=as.Date(query_date)) %>% 
      filter(date==max(date)) %>% 
      select(last)
  last <- last[[1,1]]
  }, error = function(e) {
  })  
  return(last)
  }

#######################################################
# Get a list of results subdirectories
results_directories <- list.files(results_directory)

# For each results subdirectory, create a return vector
for (results_subdirectory in results_directories) {
  # Parse the directory name
  dirname_data <- str_split_fixed(results_subdirectory, "__", 3)
  new_data_source <- dirname_data[1]
  new_constituent_index <- dirname_data[2]
  # Load slow moving data - in particular, the price data
  # This is super inefficient to load each time
  if(!exists("price_data") ||
     !exists("data_source") ||
     !exists("constituent_index") ||
     data_source != new_data_source ||
     constituent_index != new_constituent_index) {
    data_source = new_data_source
    constituent_index = new_constituent_index
    source("scripts/trading/load_slow_moving_data.R")  
  }
  
  # Read in the logs
  runtime_log <- read_csv(
    file.path(results_directory, results_subdirectory, "runtime_log"))
  trade_history <- read_feather(
    file.path(results_directory, results_subdirectory, "trade_history.feather"))
  transaction_log <- read_feather(
    file.path(results_directory, results_subdirectory, "transaction_log.feather"))
  # Compute start and end date of backtest
  start_backtest <- as.Date(min(runtime_log$timestamp))
  end_backtest <- as.Date(max(runtime_log$timestamp))
  backtest_date_sequence <- seq(start_backtest, end_backtest, by = "day")
  portfolio_valuation <- as_data_frame(backtest_date_sequence) %>% 
    rename(date = value) %>%
    mutate(cash_balance = NA) %>%
    mutate(stock_value = NA) %>%
    mutate(portfolio_value = NA)
  
  # Work through each date
  for (i in seq_along(portfolio_valuation$date)) {
    valuation_date <- portfolio_valuation$date[i]
    # Get cash balance
    cash_balance <- sum(transaction_log %>% 
       dplyr::filter(as.Date(timestamp) <= valuation_date) %>%
          select(amount))
    # Get stock positions for each date
    stock_positions <- (trade_history %>% 
           dplyr::filter(as.Date(timestamp) <= valuation_date) %>%
           select(symbol, quantity) %>% 
           group_by(symbol) %>% 
           summarize(sum(quantity)) %>%
           rename(position = 'sum(quantity)')) %>%
          mutate(last_price = NA)
    # Add last prices
    # Not sure why this function wouldn't work in a mutate pipe...
    for (j in seq_along(stock_positions$symbol)) {
      ticker <- stock_positions$symbol[j]
      stock_positions$last_price[j] <- get_stock_last_price(ticker, valuation_date)
      rm(ticker)
    }
    
    # Compute stock value
    stock_positions <- stock_positions %>%
      mutate(value = position + last_price)
    stock_value <- sum(stock_positions$value)
   
    # Update data frame
    portfolio_valuation$cash_balance[i] <- cash_balance
    portfolio_valuation$stock_value[i] <- stock_value
    portfolio_valuation$portfolio_value[i] <- cash_balance + stock_value
    write_csv(portfolio_valuation,
      file.path(results_directory, results_subdirectory, "portfolio_valuation.csv"))
    
  }
}

####################################################




