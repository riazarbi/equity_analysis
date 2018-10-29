##################################
# CONTEXT
# This script assumes that the following dataframes have been loaded into memory:
# runtime_ticker_data
##################################

source("algorithm.R")
start <- Sys.time()

# Here's how you test the backtest algorithm
# First, re-source so any changes can be incorporated
source("algorithm.R")
test_target_weight <- compute_weights(get_algo_dataset(sample(train,1)))
# Verify weights sum to 1
sum(test_target_weight$weights)
# Check how many consitutents there are
nrow(test_target_weight)
# Verify they are all unique (ie same numer as nrow)
length(unique(test_target_weight$portfolio_members))

# RUN THE ALGO ACROSS ALL THE DATA
# remove target_weights object is it exists
# to ensure we aren't appending to an old weight
if (exists("target_weights")) {
  rm(target_weights)
}
# Walk along the backtest dates creating ideal portfolio weights
for (i in seq_along(train)) {  
  # time each loop
  begin <- Sys.time()
  now <- train[i]
  # Get the dataset for the particular date 
  algo_data <- get_algo_dataset(now)
  if (!exists("target_weights")) {
  target_weights <- compute_weights(algo_data)
  target_weights$date <- now
  } else 
  {
    next_target_weights <- compute_weights(algo_data)
    next_target_weights$date <- now
    target_weights <- bind_rows(target_weights, next_target_weights)
    rm(next_target_weights)
  }
  end <- Sys.time()
  #print("Time Elapsed for this section:")
  #print(end - begin)
  message <- paste("Last date done: ", now, 
                   ". Speed approximately ", as.integer(end - begin), 
                   " seconds per iteration.                 ", sep="")
  cat("\r", message)
  sum(target_weights$weights)
  
 
}

# Save weights and companio algorithm to datalog
# Shared filename attributes
timestamp <- as.numeric(as.POSIXct(Sys.time()))*10^5
data_source <- "self"
data_type <- "backtest" 
algorithm_name <- "cap_weighted"
# Weights-specific filename attributes
weights_data_identifier <- paste(algorithm_name, "_weights", sep="") 
weights_file_string <- paste(timestamp, data_source, data_type, weights_data_identifier, sep = "__")
weights_file_string <- paste(weights_file_string, ".feather", sep = "")
weights_file_string <- file.path(datalog_directory, weights_file_string)
# Algorithm-specific filename attributes
algorithm_data_identifier <- paste(algorithm_name, "_algorithm", sep="")
algorithm_file_string <- paste(timestamp, data_source, data_type, algorithm_data_identifier, sep = "__")
algorithm_file_string <- paste(algorithm_file_string, ".R", sep = "")
algorithm_file_string <- file.path(datalog_directory, algorithm_file_string)

# Save the files
write_feather(target_weights, weights_file_string)
file.copy(from = "algorithm.R", to = datalog_directory, overwrite = TRUE)
file.rename(from = file.path(datalog_directory, "algorithm.R"), to=algorithm_file_string)

end <- Sys.time()
print(end - start)