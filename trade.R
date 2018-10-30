# LOAD ENVIRONMENT ##################################
all_begin <- Sys.time()
# data_pipeline_scripts - ONCE DAILY
# All these scripts can be run independently - they actually clear the
# environment before running.
# These scripts are not parametrized, and they don't need to be.
source("data_pipeline_scripts/100_bloombergUSD_to_datalog.R")
source("data_pipeline_scripts/100_bloombergZAR_to_datalog.R")
source("data_pipeline_scripts/101_datalog_csv_to_feather.R")
source("data_pipeline_scripts/102_constituents_to_dataset.R")
source("data_pipeline_scripts/102_metadata_to_dataset.R")
source("data_pipeline_scripts/102_ticker_logs_to_dataset.R")
# Clean out environment so we know this script works in a clean environment
rm(list=ls())
source("load_slow_moving_data.R")

#####################################################
# TRADE SCRIPTS #####################################
# Check the trading mode in parameters.R
source("data_pipeline_functions.R")
source("parameters.R")
source("algorithm.R")
source("trading_functions.R")
source("initialize_portfolio.R")

if(!(run_mode %in% allowed_modes)) {
  print("Set a correct mode in parameters.R: Either LIVE or BACKTEST.")
} else {
  
# Create trading heartbeat
print(paste("Running in", run_mode, "mode."))
all_begin <- Sys.time()
# Set heartbeat count to 0
heartbeat_count <- 0
repeat{
  # If LIVE, runtime_date = now
  if(run_mode == "LIVE") {
  runtime_date <- Sys.Date()
  # If backtest, runtime_date = start backtest + heartbeat in seconds
  } else if (run_mode == "BACKTEST") {
  runtime_date <- start_backtest + seconds(heartbeat_count)
  }
  
  print(paste("=========== Runtime date:", runtime_date, "==========="))
  
  # THE BELOW CODE RUNS REGARDLESS OF TRADING MODE
  # 1. VERIFY SLOW MOVING DATA IS IN MEMORY
  print("CHECK: Has slow-moving data been loaded into memory?")
  # Load it if it hasn't already been
  if(!exists("ticker_data_load_date")){
    print("WARNING: Slow moving data has not been loaded.")
    source("load_slow_moving_data.R")
  } else if (difftime(begin, ticker_data_load_date,  units="days") >= 1) {
    print("WARNING: Slow moving data was loaded more than 24 hours ago, and might be stale.")
    source("load_slow_moving_data.R")
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
    weight_validation <- sum(target_weights$weights)
    print(paste("CHECK: Sum of all weights is", round(weight_validation, 5), "(rounded to 5 decimal points)."))
    if(round(weight_validation, 5) != 1) {
      stop("ERROR: Weights don't sum to 1")
    }
  } else {
    print("OK: Target weights are fine.")}
  
  # 4. Compute current portfolio weights
  # 5. Compute required trades
  # 6. Submit orders -> save to feather? 
  #    Another process - the trader - can read feather and trade
  
  
  # SLEEP CONDITIONS
  # Sleep the runtime if trading mode is live
  if(run_mode == "LIVE") {
    print(paste("Heartbeat count:", heartbeat_count, "(seconds)."))
    print(paste("Sleeping for", heartbeat_duration, "seconds."))
    Sys.sleep(heartbeat_duration)}
  
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
}

# CREATE A TARGET PORTFOLIO - ALSO ONCE DAILY

#What does a trader do? 
#  2. Check target portfolio weights
#  3. Check existing portfolio
#  4. Compute required trades
#  5. Trade to resolve
#  6. Repeat every 10 min

  



# TRADE TOWARDS THE TARGET PORTFOLIO
# source("400_trade.R")
# Restore output to console
#sink() 
#sink(type="message")

# And look at the log...
#cat(readLines("test.log"), sep="\n")
