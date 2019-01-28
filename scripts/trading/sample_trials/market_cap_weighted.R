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
# This function computes the market cap of each ticker,
# And weights the ticker according to its proportion of total index market cap

compute_weights <- function(algo_data, metrics) {
  # specify the market_cap field for weighting purposes
  algo_data <- lapply(algo_data, function(x) x <- x %>%
                        mutate(market_cap = CUR_MKT_CAP))
  # Start timer. Good to see if you'll need to refactor this.
  algo_start <- Sys.time()
  # 1. SELECT DESIRABLE STOCKS
  # Keep only the necessary fields
  algo_data <- algo_data %>% select(metrics)
  # Create some rank measure
  # Eliminate stocks that don't make the rank
  
  # 2. WEIGHT THE SURVIVORS 
  # Drop all entries except the latest one
  algo_data <- algo_data %>% map(~filter(.x, date == max(date)))
  # Compute the index market cap
  index_mkt_cap <- algo_data %>% map(function(x) sum(x$market_cap)) %>% reduce(`+`)
  # ASSIGN WEIGHTS AS % OF AGGREGATE
  target_weight <- sapply(algo_data, function(x) sum(x$market_cap)/index_mkt_cap )  
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
