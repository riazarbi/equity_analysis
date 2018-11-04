# Define paths
#rm(list=ls())
source("scripts/data_pipeline/set_paths.R")

# Define parameters
#################################################################
# GLOBAL PARAMETERS
# Set seed for reproducibility
set.seed(42)

# Mode - either LIVE or BACKTEST
run_mode <- "BACKTEST"
# Heartbeat duration: how long between heartbeats (seconds)
heartbeat_duration <- 1200

# Universe
constituent_index <- "FIFTY_STOCKS"
data_source <- "simulated"
market_metrics <- c()
fundamental_metrics <- c() 

# Timeframe
start_backtest <- "2017-01-05" # inclusive
end_backtest <- "2017-01-06" # not inclusive

# Portfolio characteristics
portfolio_starting_configs <- c("CASH", "STOCK")
portfolio_starting_config <- "CASH"
portfolio_starting_value <- 10000
cash_buffer_percentage <- 0

# Trading characteristics
commission_rate <- 0.01
minimum_commission <- 0
standard_spread <- 0

# Process parameters
# * DON'T MODIFY * #
allowed_modes <- c("LIVE", "BACKTEST")
# Convert to date format
library(lubridate)
start_backtest <- ymd(start_backtest)
end_backtest <- ymd(end_backtest)

# Check parameter health
###############################################################
if(!(run_mode %in% allowed_modes)) {
  stop("Set a correct mode in parameters.R: Either LIVE or BACKTEST.")
} 
print(paste("Running in", run_mode, "mode."))

# Load trading functions
###############################################################
# Depends on run mode
source("scripts/trading/trading_functions.R")

# Run trials
###############################################################
# Get a list of trials
trials <- list.files(trial_directory)
# If there are trials, copy in an example
if(length(trials) == 0) {
  print("No trials found. Copying algorithm.R from 'scripts/trading' directory as an example.")
  file.copy(file.path("scripts/trading/example_trial.R"), trial_directory, overwrite = TRUE)
}
# Get a list of trials
trials <- list.files(trial_directory)

# For each trial
for (trial in trials) {
  print(paste("INFO: Running trial:", trial))
  trial_path <- file.path(trial_directory, trial)
  # clearing out results folder
  print("INFO: Deleting results directory if it exists already.")
  results_path <- file.path(results_directory, tools::file_path_sans_ext(trial))
  unlink(results_path, recursive=T )
  print("INFO: Creating new results directory.")
  dir.create(results_path, showWarnings = FALSE)
  # trade for the length of the backtest
  print(paste("INFO: Running backtest for", trial))
  source("scripts/trading/trade.R")
  print("Moving the trial script to the results directory")
  file.rename(from=trial_path, to=file.path(results_path, trial))
}
