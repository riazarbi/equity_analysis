##################################################################
# DATALOG FUNCTIONS
##################################################################
# CONVERT DATALOG FILES INTO DATAFRAME
convert_datalog_to_dataframe <- function() {
  file_list <- list.files(datalog_directory)
  data_log <- as.data.frame(str_split_fixed(file_list, "__", 4), stringsAsFactors=FALSE)
  colnames(data_log) <- c("timestamp", "source", "data_type", "data_label")
  data_log$data_label <- tools::file_path_sans_ext(data_log$data_label)
  data_log$filename <- file_list
  data_log$ext <- tools::file_ext(data_log$filename)
  return(data_log)
  
}

# REMOVE USELESS FILES FROM DATALOG DIRECTORY
# the intention of this functon is to remove all poorly-formed logs.
# Currently the function:
# 1. takes a dataframe which has a list of filenames in the datalog directory
# 2. removes all log files with no data
# 3. replaces the dataframe with an updated one
remove_empty_csvs <- function(data_log) {
  for (datalog in data_log$filename) { 
    if(tools::file_ext(datalog) == "csv") {
      filepath <- file.path("datalog",datalog)
      loglength <- length(readLines(filepath))
      message <- paste(filepath, "                 ")
      cat("\r",message)
      if (loglength == 1) {
        message = paste("Empty file. Removing", datalog)
        cat("\n",message)
        file.remove(filepath)
      }
    }
  }
  data_log <- convert_datalog_to_dataframe()
  return(data_log)
}

# FILTER DATALOG
# NOT CURRENTLY USED BECUAE THE TIMESTAMP FILTER DOESN't WORK.
# CONSIDER DELETING
# this function takes string arguments for each fiels in a datalog dataframe
# it filters the dataframe to retun one where the fields match the 
# substing arguments
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

# LOG COMPACTION 
# This function expects to be fed a dataframe with the following columns:
# timestamp, date, source, metric, value
# These are the standard columns of a ticker dataset in the datasets folder.
# For two sequential dates, if the value, source, ticker and metric are the same, drop the later date.
# So we always have ONLY the EARLIEST date that a value was entered for a particular metric.
# For two sequential timestamps, if all the fields are the same, delete the earlier timestamp.
# So we will always have ONLY the LATEST ACCESS TIME of a particular date/field combination.
compact_dataset <- function(dataframe) {
  
  # Drop old timestamps of duplicate data
  timestamp_filtered <- dataframe %>% 
    mutate(key=paste(date, metric, value, source, sep = "|")) %>%
    arrange(desc(key)) %>%
    filter(key != lag(key, default="0")) %>% 
    select(-key)
  
  # Drop row where the data doesn't change from day to day
  compact_data <- timestamp_filtered %>% 
    mutate(key=paste(value, metric, source, sep = "|")) %>%
    arrange(date) %>%
    filter(key != lag(key, default="0")) %>% 
    select(-key)
  
  return(compact_data)
  
}

################################################################
# LOAD DIMENSIONS
################################################################
# Read in the fundamental data fields dimension
#fundamental_fields_file <- file.path(dimensions_directory, "fundamental_fields.csv")
#fundamental_fields <- read.csv(fundamental_fields_file, header = FALSE, colClasses = "character")
#fundamental_fields <- fundamental_fields[,1]
#rm(fundamental_fields_file)

# Read in the marketdata dimension
#market_fields_file <- file.path(dimensions_directory, "market_fields.csv")
#market_fields <- read.csv(market_fields_file, header = FALSE, colClasses = "character")
#market_fields <- market_fields[,1]
#rm(market_fields_file)

# Read in the metadata dimension
#metadata_fields_file <- file.path(dimensions_directory, "metadata_fields.csv")
#metadata_fields <- read.csv(metadata_fields_file, header = FALSE, colClasses = "character")
#metadata_fields <- metadata_fields[,1]
#rm(metadata_fields_file)
