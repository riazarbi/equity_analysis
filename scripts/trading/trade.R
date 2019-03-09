# TIME THE BACKTEST #################################
all_begin <- Sys.time()

# CLEAR ENVIRONMENT AND LOAD ALGORITHM
# Define some custom functions
###############################################################
remove_if_exists <- function(obj, env = globalenv()) {
  obj <- deparse(substitute(obj))
  if(exists(obj, envir = env)) {
    rm(list = obj, envir = env)
  }
}

print("Resetting Environment")
# Remove any variables that may have persisited
remove_if_exists(last_rebalancing_date)
remove_if_exists(runtime_ticker_data)
remove_if_exists(activity_log)
remove_if_exists(compute_weights)
# Load the algorithm
print("Loading Algorithm")
source(trial_path)


# ACTUALLY TRADE  ###################################
con <- connect_to_broker()

# CREATE LOG HEADERS
write("timestamp, portfolio_value, value_trades_submitted, value_successful_buys, value_successful_sells", 
      file = file.path(results_path,"runtime_log"),append=TRUE)

# Create trading heartbeat
all_begin <- Sys.time()
# Set heartbeat count to 0
heartbeat_count <- 0

repeat{
  # If LIVE, runtime_date = now
  if(run_mode == "LIVE") {
  runtime_date <- Sys.Date()
  # If backtest, runtime_date = start backtest + heartbeat in seconds
  } else if (run_mode == "BACKTEST") {
  runtime_date <- as.POSIXct(start_backtest) + seconds(heartbeat_count)
  }
  
  print(paste("=========== Runtime date:", runtime_date, "==========="))
  runtime_begin <- Sys.time()
  # START LOGGING
  activity_log <- as.character(runtime_date)

  # THE BELOW CODE RUNS REGARDLESS OF TRADING MODE

  # 1. VERIFY SLOW MOVING DATA IS IN MEMORY
  print("CHECK: Has slow-moving data been loaded into memory?")
  # Load it if it hasn't already been
  if(!exists("ticker_data_load_date")){
    print("WARNING: Slow moving data has not been loaded. Loading now...")
    ticker_data <- readRDS("temp/ticker_data.Rds")
    price_data <- readRDS("temp/price_data.Rds")
    metadata <- readRDS("temp/metadata.Rds")
    constituent_list <- readRDS("temp/constituent_list.Rds")
    ticker_data_load_date <- Sys.time() # TODO make this more elegant. Last modified time perhaps?
  # or load it if it was last loaded more than a day ago
  } else if (difftime(all_begin, ticker_data_load_date,  units="days") >= 1) {
    print("WARNING: Slow moving data was loaded more than 24 hours ago, and might be stale. Reloading...")
    ticker_data <- readRDS("temp/ticker_data.Rds")
    price_data <- readRDS("temp/price_data.Rds")
    metadata <- readRDS("temp/metadata.Rds")
    constituent_list <- readRDS("temp/constituent_list.Rds")
    ticker_data_load_date <- Sys.time() # TODO make this more elegant. Last modified time perhaps?
  } else { 
    print("OK: Slow moving data is loaded and not stale.")}
  
  # 2. VERIFY RUNTIME TICKER DATA IS HEALTY
  # Compute a new runtime_ticker_dataset only if it doesn't exist, 
  # OR if rebalancing_periodicity + last_rebalancing_date <= runtime_date.
  print("CHECK: Is runtime_ticker_data stale?")
  if(!exists("runtime_ticker_data") || 
     !exists("last_rebalancing_date") ||
     runtime_date >= (last_rebalancing_date + rebalancing_periodicity)) {
    print("WARNING: A new runtime_ticker_data dataframe needs to be generated.")
    # Compute new runtime ticker data
    runtime_ticker_data <- get_runtime_dataset(runtime_date, constituent_list, ticker_data)
    # Update the last runtime date
    last_rebalancing_date <- runtime_date
    # Remove target weights if it exists so it can be re-computed
    if (exists("target_weights")) {
      rm(target_weights)
    }
  } else {
    print("OK: runtime_ticker_data is fine.")}

  # 3. VERIFY IDEAL PORTFOLIO WEIGHTS ARE NOT STALE
  # This will run whenever new runtime ticker data is generated 
  # because the generation code deletes the target_weights object
  print("CHECK: Are target ticker weights healthy?")
  if(!exists("target_weights")) {
    print("WARNING: New target weights need to be generated.")
    # Compute new target weights
    print("ACTION: Running algorithm.R.")
    target_weights <- compute_weights(runtime_ticker_data, metrics)
    # Validate new target weights
    weight_validation <- sum(target_weights$target_weight)
    print(paste("CHECK: Sum of all weights is", round(weight_validation, 5), "(rounded to 5 decimal points)."))
    if(round(weight_validation, 5) != 1) {
      stop("ERROR: Weights don't sum to 1")
    }
  } else {
    print("OK: Target weights are fine.")}
  
  #### NEXT: TRADE SUBMISSION
  print("ACTION: Loading Transaction Log")
  transaction_log <- get_transaction_log()
  print("ACTION: Loading Trade History")
  trade_history <- get_trade_history()
  print("ACTION: Computing Current Positions")
  positions <- compute_positions(transaction_log, trade_history)
  # obtain valuation
  # log total value
  valuation <- portfolio_valuation(positions)
  total_value <- sum(valuation$value)
  activity_log <- paste(activity_log, total_value, sep = ", ")
  print("ACTION: Creating Order List")
  trades <- compute_trades(target_weights, positions)
  # log value of trades
  value_of_trades <- sum(trades$order_value) - (trades %>% filter(portfolio_members == "CASH"))$order_value
  activity_log <- paste(activity_log, value_of_trades, sep = ", ")
  if(nrow(trades)!=0) {
    print("ACTION: Submitting Trades")
    trade_success_val <- submit_orders(trades)
  } else {
    trade_success_val <- "0,0"}
  activity_log <- paste(activity_log, trade_success_val, sep = ", ")
  rm(transaction_log, trade_history, positions, trades)
  
  # RUNTIME PROCESSING TIME
  runtime_end <- Sys.time()
  print(paste("INFO: Heartbeat processing time:", runtime_end - runtime_begin, "seconds"))
  
  # WRITE OUT TO LOG
  write(activity_log, file = file.path(results_path,"runtime_log"),append=TRUE)
  
  # SLEEP CONDITIONS
  # Sleep the runtime if trading mode is live
  if(run_mode == "LIVE") {
    print(paste("Heartbeat count:", heartbeat_count, "(seconds)."))
    print(paste("Sleeping for", heartbeat_duration, "seconds."))
    Sys.sleep(heartbeat_duration)
    } 
  
  # BREAK CONDITIONS
  # Break the runtime if the backtest ends
  if(run_mode == "BACKTEST" & runtime_date >= end_backtest) {
    print("Backtest simulation complete.")
    break}
  # Break the runtime if live has run more than 1000 times
  if(run_mode == "LIVE" & heartbeat_count >= 120) {
    print("Live trading stopped.")
    break}
  heartbeat_count <- heartbeat_count + heartbeat_duration
}  

# remove last rebalancing date - otherwise successive
# trials won't rebalance
rm(last_rebalancing_date)

all_end <- Sys.time()
print(all_end - all_begin)