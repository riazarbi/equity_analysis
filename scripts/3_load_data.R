## ENVIRONMENT SET UP
# Clear environment and load common functions
rm(list=ls())
source("R/set_paths.R")
source("scripts/parameters.R")
source("R/data_pipeline_functions.R")

# Load trading functions
###############################################################
source("R/trading_functions.R")
# Some functions depend on run mode
if (run_mode == "BACKTEST") { 
  source("R/backtest_trading_functions.R") 
} else {
  source("R/live_trading_functions.R")}

###############################################################
# This script will load slow moving data
# This slow moving data is a filtered version of the 
# datasets that is suitable for running backtests against.
# The slow-moving data is saved as an .RData binary in
# "temp/slow_moving_data.RData
source("scripts/data_loading/load_slow_moving_data.R")

###############################################################
# This scipt loads slow_moving_data.RData and
# Compiles a report on the health and quality of
# the data for reporting purposes.
# It does not modify slow_moving_data.RData
source("scripts/data_loading/compile_data_report.R")
