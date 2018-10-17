# DEFINE THE ALGORITHM
# This function is the actual backtest rule. How will we weight this portfolio?
# This function takes in a list of dataframes, each dataframe being a ticker
# It performs some black box math, and returns a dataframe of target portfolio weights.
# It is the task of the researcher to figure out what black box math they want to use.
# This sample one just weights by last known market cap.
compute_weights <- function(algo_data) {
  # Drop all entries except the latest one
  algo_data <- algo_data %>% map(~filter(.x, date == max(date)))
  # Compute the index market cap
  index_mkt_cap <- algo_data %>% map(function(x) sum(x$CUR_MKT_CAP)) %>% reduce(`+`)
  weights <- sapply(algo_data, function(x) sum(x$CUR_MKT_CAP)/index_mkt_cap )  
  #str(weights)
  portfolio_members <- names(algo_data)
  #str(portfolio_members)
  target_weights <- data.frame(portfolio_members, weights)
  target_weights$portfolio_members <- as.character(target_weights$portfolio_members)
  sum(target_weights$weights)
  return(target_weights)
}

## SET PARAMETERS
# This section is where we set parameters for the backtest
constituent_index <- "JALSH"
data_source <- "bloomberg"
market_metrics <- c("CUR_MKT_CAP", "PX_LAST")
fundamental_metrics <- c() 
# This section sets the backtest dates
start_backtest <- "2010-01-01"
end_backtest <- "2016-12-31"
periodicity <- "month"
# And the splits
train_test_split <- 0.8

# PROCESS THE PARAMETERS
# Combine market and fundamental metrics
metrics <- c(market_metrics, fundamental_metrics)
# But if no metrics are selected, just take all possible fields
if (length(metrics)==0){
  metrics <- c(fundamental_fields, market_fields)
}
# Create training and testing date ranges
dates <- seq(as.Date(start_backtest), as.Date(end_backtest), by=periodicity)
date_split <- dates[as.integer(length(dates)*train_test_split)]
train <- dates[dates < date_split]
test <- dates[dates >= date_split]
rm(dates)
# Check that test and train don't overlap
# We expect this to be 0
intersect(test, train)
