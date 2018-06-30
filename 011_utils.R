# Libraries
library(tidyverse)

###################################################################
# Get working directory
working_directory <- getwd()

# Define directory paths
dimensions_directory <- file.path(working_directory, "dimensions")
data_directory <- file.path(working_directory, "datalog")
dataset_folder_root <- file.path(working_directory, "datasets")
dir.create(dataset_folder_root, showWarnings = FALSE)


##################################################################
# FUNCTIONS
# Convert datalog file to data frame
convert_datalog_to_dataframe <- function() {
  file_list <- list.files(data_directory)
  data_log <- as.data.frame(str_split_fixed(file_list, "__", 4), stringsAsFactors=FALSE)
  colnames(data_log) <- c("timestamp", "source", "data_type", "data_label")
  data_log$data_label <- tools::file_path_sans_ext(data_log$data_label)
  data_log$filename <- file_list
  return(data_log)
  
}

# filter datalog
filter_datalog <- function(data_log, 
                           timestamp_days_age_limit, 
                           source_substring, 
                           data_type_substring, 
                           data_label_substring) 
{  
  
  timestamp_days_age_limit <- timestamp_days_age_limit * 8640000000
  timestamp_days_age_limit <- as.numeric(as.POSIXct(Sys.time()))*10^5 - timestamp_days_age_limit
  
  data_log <- dplyr::filter(data_log, timestamp >= timestamp_days_age_limit)
  data_log <- dplyr::filter(data_log, grepl(source_substring,source))
  data_log <- dplyr::filter(data_log, grepl(data_type_substring, data_type))
  data_log <- dplyr::filter(data_log, grepl(data_label_substring, data_label))
  return(data_log)
}

################################################################
# Partition dataframes files into types

