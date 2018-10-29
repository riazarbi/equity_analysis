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