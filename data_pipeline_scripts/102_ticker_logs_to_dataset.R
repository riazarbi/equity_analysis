print("")
print("NEXT: Creating individual ticker datasets...")
# Clear environment
rm(list=ls())
# Read in data_pipeline functions
source("set_paths.R")
source("data_pipeline_scripts/data_pipeline_functions.R")
# Time the script
begin <- Sys.time()

#########################################################################################
# Start function here
# This function accepts a data_log and a dataset
# data_log is a dataframe created by convert_datalog to dataframe() from data_pipeline_functions.R
# dataset is either ticker_fundamental_data or ticke_market_data
ticker_datalog_to_dataset <- function(data_log, dataset) {
  # load libraries
  library(foreach)
  library(doParallel)
  
  # Create dataset save location if it does not exist
  dataset_folder <- file.path(dataset_directory, dataset)
  dir.create(dataset_folder, showWarnings = FALSE)
  
  # Read in new log data
  # Filter the log to show just filtered data files)
  filtered_data_log <- data_log %>% 
    dplyr::filter(ext == "feather") %>% 
    dplyr::filter(grepl(dataset, data_type))
  
  # Break the logs down into separate tickers
  number_of_tickers <- length(unique(filtered_data_log$data_label))
  
  tickers <- sort(unique(filtered_data_log$data_label))
  
  # Register a parallel processing cluster
  n_cores <- detectCores() - 1
  cl <- makeCluster(n_cores, outfile="")
  registerDoParallel(cl)
  
  foreach(i = 1:length(tickers), .packages = c("magrittr", "dplyr", "feather")) %dopar% {
    source("data_pipeline_scripts/data_pipeline_functions.R")
    # Define table name for the ticker
    table_name <- stringr::str_replace_all(tickers[i],"[[:punct:]\\s]+","_")
    # Get list of files relating to the ticker
    data_files <- dplyr::filter(filtered_data_log, grepl(tickers[i],data_label))
    
    # build a master dataframe
    # read in the first data file
    all_data <- feather::read_feather(paste("datalog", data_files$filename[1], sep = "/")) 
    # Check if the data has any rows
    if (nrow(all_data) >= 1) {
      # add timestamp ID and source
      all_data$timestamp <- data_files$timestamp[1]
      all_data$source <- data_files$source[1]
      # Melt the data into the correct format
      all_data <- all_data %>% 
        tidyr::gather(metric, value, -timestamp, -source, -date)
    }

    # Append other files data if they exist
    # Check if there more files for that ticker
    if (nrow(data_files) >=2) {
      # For each additional file
      for (j in 2:nrow(data_files)) {
        # Read the file data
        table_data <- feather::read_feather(paste("datalog", data_files$filename[j], sep = "/")) 
        # if the file has data
        if (nrow(table_data) >= 1) {
          # Melt the data into the correct format
          table_data$timestamp <- data_files$timestamp[j]
          table_data$source <- data_files$source[j]
          table_data <- table_data %>% 
            tidyr::gather(metric, value, -timestamp, -source, -date) 
          # Bind the data
          all_data <- bind_rows(all_data, table_data)
          # Drop NA values and format date as date
        }
      }
    }
    
    # Drop NA values in new data
    all_data <- all_data  %<>%
      mutate(value = as.numeric(value)) %>%
      mutate(date = lubridate::ymd(date)) %>% 
      tidyr::drop_na()
    
    # Check if ticker has an existing dataset
    persistent_storage <- file.path(dataset_folder, paste(table_name, "feather", sep = "."))
    # If ticker does exist, merge new data with esixting dataset
    if (file.exists(persistent_storage)) {
      message <- paste(i, "/", number_of_tickers, table_name, "Merging data               ", sep=" ")
      cat("\r", message)
      # Read in existing dataset
      persistent_data  <- feather::read_feather(persistent_storage)
      # Bind the two datasets
      all_data <- bind_rows(all_data, persistent_data)
    }
    
    # Compact the dataframe by excluding repetitive data
    message <- paste(i, "/", number_of_tickers, table_name, "Compacting data               ", sep=" ")
    cat("\r", message)
    all_data <- compact_dataset(all_data)
    # Finally, write dataframe to disk
    write_feather(all_data, persistent_storage)
    message <- paste(i, "/", number_of_tickers, table_name, "Wrote data to disk        ", sep=" ")  
    cat("\r", message)
  }
  
  # Stop the cluster
  stopCluster(cl)
}
# End function here
############################################################################################

# Create a dataframe of data log files
data_log <- convert_datalog_to_dataframe()

# Extract data from fundamental data logs and save to ISIN-labeled datasets
print("Converting fundamental datalogs to ticker datasets...")
ticker_datalog_to_dataset(data_log, "ticker_fundamental_data")

# Extract data from market data logs and save to ticker-labeled datasets
print("Converting market datalogs to ticker datasets...")
ticker_datalog_to_dataset(data_log, "ticker_market_data")

end <- Sys.time()
print(end - begin)