# LOAD ENVIRONMENT ##################################
all_begin <- Sys.time()
#####################################################
# TRADE SCRIPTS #####################################
# Load the parameters and the algorithm
source("scripts/trading/parameters.R")
source(trial_path)
# Load trading functions
source("scripts/trading/trading_functions.R")
# Clean out environment so we know this script works in a clean environment

# CHECK PARAMETER HEALTH
if(!(run_mode %in% allowed_modes)) {
  stop("Set a correct mode in parameters.R: Either LIVE or BACKTEST.")
} 
print(paste("Running in", run_mode, "mode."))
#####################################################
# ACTUALLY TRADE  ###################################
con <- connect_to_broker()

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
  log <- as.character(runtime_date)
  # THE BELOW CODE RUNS REGARDLESS OF TRADING MODE
  # 1. VERIFY SLOW MOVING DATA IS IN MEMORY
  print("CHECK: Has slow-moving data been loaded into memory?")
  # Load it if it hasn't already been
  if(!exists("ticker_data_load_date")){
    print("WARNING: Slow moving data has not been loaded.")
    source("scripts/trading/load_slow_moving_data.R")
  } else if (difftime(begin, ticker_data_load_date,  units="days") >= 1) {
    print("WARNING: Slow moving data was loaded more than 24 hours ago, and might be stale.")
    source("scripts/trading/load_slow_moving_data.R")
  } else { print("OK: Slow moving data is loaded and not stale.")}
  
  # 2. VERIFY RUNTIME TICKER DATA IS HEALTY
  # Compute a new runtime_ticker_dataset only if it doesn't exist, 
  # OR the runtime date has changed from the last time.
  print("CHECK: Is runtime_ticker_data stale?")
  if(!exists("runtime_ticker_data") || 
     !exists("last_runtime_date") ||
     date(runtime_date) != last_runtime_date) {
    print("WARNING: A new runtime_ticker_data dataframe needs to be generated.")
    # Compute new runtime ticker data
    runtime_ticker_data <- get_runtime_dataset(runtime_date, constituent_list, ticker_data)
    # Update the last runtime date
    last_runtime_date <- date(runtime_date)
    # Remove target weights if it exists so it can be re-computed
    if (exists("target_weights")) {
      rm(target_weights)
    }
  } else {
    print("OK: runtime_ticker_data is fine.")}

  # 3. VERIFY IDEAL PORTFOLIO WEIGHTS ARE NOT STALE
  # This will run whenever new runtime ticker data is generated 
  # because the generation code delletes the target_weights object
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
  log <- paste(log, total_value, sep = ", ")
  print("ACTION: Creating Order List")
  trades <- compute_trades(target_weights, positions)
  # log value of trades
  value_of_trades <- sum(trades$order_value) - (trades %>% filter(portfolio_members == "CASH"))$order_value
  log <- paste(log, value_of_trades, sep = ", ")
  print("ACTION: Submitting Trades")
  submit_orders(trades)
  rm(transaction_log, trade_history, positions, trades)
  
  # RUNTIME PROCESSING TIME
  runtime_end <- Sys.time()
  print(paste("INFO: Heartbeat processing time:", runtime_end - runtime_begin, "seconds"))
  
  # WRITE OUT TO LOG
  write(log, file = file.path(results_path,"runtime_log"),append=TRUE)
  
  # SLEEP CONDITIONS
  # Sleep the runtime if trading mode is live
  if(run_mode == "LIVE") {
    print(paste("Heartbeat count:", heartbeat_count, "(seconds)."))
    print(paste("Sleeping for", heartbeat_duration, "seconds."))
    Sys.sleep(heartbeat_duration)
    } 
  
  # BREAK CONDITIONS
  # Break the runtime if the backtest ends
  if(runtime_date >= end_backtest & run_mode == "BACKTEST") {
    print("Backtest simulation complete.")
    break}
  # Break the runtime if live has run more than 1000 times
  if(heartbeat_count >= 120 & run_mode == "LIVE") {
    print("Live trading stopped.")
    break}
  heartbeat_count <- heartbeat_count + heartbeat_duration
}  

all_end <- Sys.time()
print(all_end - all_begin)