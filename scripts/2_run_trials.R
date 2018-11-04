# Define paths
rm(list=ls())
source("scripts/data_pipeline/set_paths.R")
# Get a list of trials
trials <- list.files(trial_directory)
# If there are trials, copy in an example
if(length(trials) == 0) {
  print("No trials found. Copying algorithm.R from scripts/trading directory as an example.")
  file.copy(file.path("scripts/trading/example_trial.R"), trial_directory, overwrite = TRUE)
}
# Get a list of trials
trials <- list.files(trial_directory)

# For each trial
for (trial in trials) {
  print(paste("INFO: Running trial:", trial))
  trial_path <- file.path(trial_directory, trial)
  print("INFO: Creating results directory.")
  results_path <- file.path(results_directory, tools::file_path_sans_ext(trial))
  dir.create(results_path, showWarnings = FALSE)
  source("scripts/trading/trade.R")
}



