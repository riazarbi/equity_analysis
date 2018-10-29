# Check the mode in parameters.R
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
# These scripts are parametrized - filter on index, which is specified in paramters.R
rm(list=ls())
source("200_load_slow_moving_data.R")

# TRADE
# Now we actually move to simulating reality. 

# Get the runtime date
heartbeat_count <- 0
repeat{
  if(run_mode == "LIVE") {
  runtime_date <- Sys.Date()
  } else if (run_mode == "BACKTEST") {
  runtime_date <- start_backtest + minutes(heartbeat_count)
  }
  # This this chunk of scripts use runtime_date
  # Filter ticker_data appropriately
  source("201_create_runtime_ticker_data.R")
  # Compute ideal weights
  
  # Break the runtime if the backtest ends
  if(runtime_date >= end_backtest & run_mode == "BACKTEST") {
    print("Backtest simulation complete.")
    break}
  # Break the runtime if live has run more than 1000 times
  if(heartbeat_count >= 10000 & run_mode == "LIVE") {
    print("Live trading stopped.")
    break}
  heartbeat_count <- heartbeat_count + 10
}  


# CREATE A TARGET PORTFOLIO - ALSO ONCE DAILY
source("300_compute_weight.R")

What does a trader do? 
  2. Check target portfolio weights
  3. Check existing portfolio
  4. Compute required trades
  5. Trade to resolve
  6. Repeat every 10 min

  



# TRADE TOWARDS THE TARGET PORTFOLIO
# source("400_trade.R")

all_end <- Sys.time()
print(all_end - all_begin)
}