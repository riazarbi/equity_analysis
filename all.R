# LOAD ENVIRONMENT ##################################

# Check the trading mode in parameters.R
source("parameters.R")
allowed_modes <- c("LIVE", "BACKTEST")
if(!(run_mode %in% allowed_modes)) {
  print("Set a correct mode in parameters.R: Either LIVE or BACKTEST.")
} else {

# If mode is set correctly, run through the scripts in a procedural manner
print(paste("Running in", run_mode, "mode."))
all_begin <- Sys.time()

# LOAD DATA - ONCE DAILY
# All these scripts can be run independently - they actually clear the
# environment before running.
# These scripts are not parametrized, and they don't need to be.
source("100_bloombergUSD_to_datalog.R")
source("100_bloombergZAR_to_datalog.R")
source("101_datalog_csv_to_feather.R")
source("102_constituents_to_dataset.R")
source("102_metadata_to_dataset.R")
source("102_ticker_logs_to_dataset.R")
# Clean out environment so we know this script works in a clean environment
rm(list=ls())
source("200_load_slow_moving_data.R")
}

#####################################################
# TRADE ############################################

# Maybe this is a separate script?
# Check the mode in parameters.R
source("parameters.R")
source("algorithm.R")
allowed_modes <- c("LIVE", "BACKTEST")
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
  # FOR RUNTIME DATE,
  # 1. CREATE A HISTORICAL DATASET
  source("201_create_runtime_ticker_data.R")
  # 2. COMPUTE THE IDEAL PORTFOLIO WEIGHTS
  #source("300_compute_weights.R")
  # 
  
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

What does a trader do? 
  2. Check target portfolio weights
  3. Check existing portfolio
  4. Compute required trades
  5. Trade to resolve
  6. Repeat every 10 min

  



# TRADE TOWARDS THE TARGET PORTFOLIO
# source("400_trade.R")

