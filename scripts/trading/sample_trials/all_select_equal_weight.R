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
# algo_data <- runtime_ticker_data
# sum(target_weights$target_weight)

# STRATEGY DESCRIPTION
compute_weights <- function(algo_data, metrics) {
  algo_start <- Sys.time()
  # Drop all entries except the latest one
  algo_data <- algo_data %>% map(~filter(.x, date == max(date)))

  # 2. EQUALLY WEIGHT TICKERS
  number_constituents <- length(algo_data)
  target_weight <- sapply(algo_data, function(x) 1/number_constituents )  

  # 3. CREATE LIST OF TICKER NAMES
  portfolio_members <- names(algo_data)
  
  # 4. PAIR EACH TICKER TO ITS WEIGHT
  target_weights <- data.frame(portfolio_members, target_weight)
  target_weights$portfolio_members <- as.character(target_weights$portfolio_members)
  algo_end <- Sys.time()
  print(paste("INFO: Algorithm runtime:", algo_end - algo_start, "seconds."))
  
  # 5. RETURN TARGET WIEGHTS DATA FRAME
  return(target_weights)}