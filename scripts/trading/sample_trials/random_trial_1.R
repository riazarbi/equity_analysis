# DEFINE THE ALGORITHM
# This function is the actual backtest rule. How will we weight this portfolio?
# This function takes in a list of dataframes, each dataframe being a ticker
# It performs some black box math, and returns a dataframe of target portfolio weights.
# It is the task of the researcher to figure out what black box math they want to use.
# This sample one just weights by last known market cap.
# EXPECTS: LIST OF DATAFRAMES 
# EACH ELEMENT OF LIST A TICKER NAME
# EACH ELEMENT OF LIST HAS -
# Date Column
# Arbitrary number of other columns, each of which is an attribute of the list element

# STRATEGY DESCRIPTION
# This function will assign random weights to constituents that sum to 1. 


compute_weights <- function(algo_data, metrics) {
  algo_start <- Sys.time()
  # 1. CUT THE DATASET DOWN TO SIZE
  # Keep only the necessary fields
  #algo_data <- algo_data %>% select(metrics)
  #print(colnames(algo_data))
  # Drop all entries except the latest one
  algo_data <- algo_data %>% map(~filter(.x, date == max(date)))
  # 2. COMPUTE AGGREGATE MEASURE
  number_tickers <- length(algo_data)
  raw_weights <- sample(runif(50))
  target_weight <- raw_weights/sum(raw_weights)
  # CREATE LIST OF TICKER NAMES
  portfolio_members <- names(algo_data)
  # PAIR EACH TICKER TO ITS WEIGHT
  target_weights <- data.frame(portfolio_members, target_weight)
  target_weights$portfolio_members <- as.character(target_weights$portfolio_members)
  algo_end <- Sys.time()
  print(paste("INFO: Algorithm runtime:", algo_end - algo_start, "seconds."))
  # RETURN TARGET WIEGHTS DATA FRAME
  return(target_weights)
}