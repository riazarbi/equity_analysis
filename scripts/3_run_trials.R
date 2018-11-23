# Define paths and load parameters
rm(list=ls())
source("R/set_paths.R")
source("scripts/parameters.R")

# Load trading functions
###############################################################
source("R/trading_functions.R")
# Some functions depend on run mode
if (run_mode == "BACKTEST") {
source("R/backtest_trading_functions.R") 
  } else {
source("R/live_trading_functions.R")}
# Run trials
###############################################################
all_trials_begin <- Sys.time()
# Get a list of trials
trials <- list.files(trial_directory)
# If there are trials, copy in an example
if(length(trials) == 0) {
  stop("No trials found. You can copy a sample directory of trials from scripts/data_processing.")
}
# Get a list of trials
trials <- list.files(trial_directory)

# For each trial
for (trial in trials) {
  print(paste("INFO: Running trial:", trial))
  trial_path <- file.path(trial_directory, trial)
  # clearing out results folder
  print("INFO: Deleting results directory if it exists already.")
  results_path <- file.path(results_directory, 
                            paste(data_source, constituent_index, tools::file_path_sans_ext(trial), sep="__"))
  unlink(results_path, recursive=T )
  print("INFO: Creating new results directory.")
  dir.create(results_path, showWarnings = FALSE)
  # trade for the length of the backtest
  print(paste("INFO: Running backtest for", trial))
  source("scripts/trading/trade.R")
  print("Moving the trial script to the results directory")
  file.rename(from=trial_path, to=file.path(results_path, trial))
  gc(verbose=T)
}

all_trials_end <- Sys.time()

print(all_trials_end - all_trials_begin)
