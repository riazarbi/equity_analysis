# Start function here
ticker_datalog_to_dataset <- function(dataset) {
  # Create dataset save location if it does not exist
  dataset_folder <- file.path(dataset_directory, dataset)
  dir.create(dataset_folder, showWarnings = FALSE)
  
  # Read in new log data
  # Filter the log to show just filtered data files
  filtered_data_log <- dplyr::filter(data_log, grepl(dataset, data_type))
  
  # Break the logs down into separate tickers
  number_of_tickers <- length(unique(filtered_data_log$data_label))
  
  tickers <- sort(unique(filtered_data_log$data_label))
  for (i in 1:length(tickers)) {
    # Define table name for the ticker
    table_name <- str_replace_all(tickers[i],"[[:punct:]\\s]+","_")
    # Get list of files relating to the ticker
    data_files <- dplyr::filter(filtered_data_log, grepl(tickers[i],data_label))
    # build a master dataframe
    # read in the first data file
    all_data <- read_csv(paste("datalog", data_files$filename[1], sep = "/"), col_types = cols(.default = "d", date = "c"))
    # Check if the data has any rows
    if (nrow(all_data) >= 1) {
      # add timestamp ID and source
      all_data$timestamp <- data_files$timestamp[1]
      all_data$source <- data_files$source[1]
      # Melt the data into the correct format
      all_data <- all_data %>% gather(metric, value, -timestamp, -source, -date)  }
    else {table_data <- table_data %>% gather(metric, value, -date)}
    
    # Append other files data if they exist
    # Check if there more files for that ticker
    if (nrow(data_files) >=2) {
      # For each additional file
      for (j in 2:nrow(data_files)) {
        # Read the file data
        table_data <- read_csv(paste("datalog", data_files$filename[j], sep = "/"), col_types = cols(.default = "d", date = "c"))
        # if the file has data
        if (nrow(table_data) >= 1) {
          # Melt the data into the correct format
          table_data$timestamp <- data_files$timestamp[j]
          table_data$source <- data_files$source[j]
          table_data <- table_data %>% gather(metric, value, -timestamp, -source, -date)
          # Bind the data
          all_data <- bind_rows(all_data, table_data)
          # Drop NA values
          all_data <- all_data %>% drop_na()
          # Compact the dataframe by excluding repetitive data
          message <- paste(i, "/", number_of_tickers, table_name, "Compacting data               ", sep=" ")
          cat("\r", message)
          all_data <- compact_dataset(all_data)
        }
      }
    }
    # Check if ticker has an existing dataset
    persistent_storage <- file.path(dataset_folder, paste(table_name, "csv", sep = "."))
    # If ticker does exist, merge new data with esixting dataset
    if (file.exists(persistent_storage)) {
      message <- paste(i, "/", number_of_tickers, table_name, "Merging data               ", sep=" ")
      cat("\r", message)
      # Read in existing dataset
      persistent_data  <- read_csv(persistent_storage, col_types = cols(.default = "c", value = "d"))
      # Bind the two datasets
      all_data <- bind_rows(all_data, persistent_data)
      # Compact the merged dataframe to exlude repetitive data
      # Note: this for any data that has not changed, this has the effect of
      # Updating the timestamp to reflect when last updated
      all_data <- compact_dataset(all_data)
    }
    
    # Finally, write dataframe to disk
    write_csv(all_data, persistent_storage, append = FALSE)
    message <- paste(i, "/", number_of_tickers, table_name, "Wrote data to disk        ", sep=" ")  
    cat("\r", message)
  }
}
# End function here