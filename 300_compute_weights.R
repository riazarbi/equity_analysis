##################################
# CONTEXT
# This script assumes that the following dataframes have been loaded into memory:
# runtime_ticker_data
##################################

start <- Sys.time()

# RUN THE ALGO ACROSS ALL THE DATA
# remove target_weights object is it exists
# to ensure we aren't appending to an old weight
if (exists("target_weights")) {
  rm(target_weights)
}
target_weights <- compute_weights(runtime_ticker_data)
weight_validation <- sum(target_weights$weights)

weight_validation

end <- Sys.time()
print(end - start)

# IMPORTANT AT THE END

# Save weights and companio algorithm to datalog
# Shared filename attributes
#timestamp <- as.numeric(as.POSIXct(Sys.time()))*10^5
#data_source <- "self"
#data_type <- "backtest" 
#algorithm_name <- "cap_weighted"
# Weights-specific filename attributes
#weights_data_identifier <- paste(algorithm_name, "_weights", sep="") 
#weights_file_string <- paste(timestamp, data_source, data_type, weights_data_identifier, sep = "__")
#weights_file_string <- paste(weights_file_string, ".feather", sep = "")
#weights_file_string <- file.path(datalog_directory, weights_file_string)
# Algorithm-specific filename attributes
#algorithm_data_identifier <- paste(algorithm_name, "_algorithm", sep="")
#algorithm_file_string <- paste(timestamp, data_source, data_type, algorithm_data_identifier, sep = "__")
#algorithm_file_string <- paste(algorithm_file_string, ".R", sep = "")
#algorithm_file_string <- file.path(datalog_directory, algorithm_file_string)

# Save the files
#write_feather(target_weights, weights_file_string)
#file.copy(from = "algorithm.R", to = datalog_directory, overwrite = TRUE)
#file.rename(from = file.path(datalog_directory, "algorithm.R"), to=algorithm_file_string)

