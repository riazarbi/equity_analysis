
begin <- Sys.time()

source('011_utils.R')
source('011_compact_log.R')
data_log <- convert_datalog_to_dataframe()

# TO DO: NEED TO DEVELOP AUTOMATIC FILTERING, WHERE YOU OMIT LOGFILES 
# WHOSE TEXT STRINGS ARE NOT WELL FORMED.

#unique(data_log$source)
#unique(data_log$data_label)

###########################################################################################################################################
# UPDATING TICKER MARKET DATA
# Define the dataset
#unique(data_log$data_type)
dataset <- "ticker_market_data"

# Create dataset save location if it does not exist
dataset_folder <- file.path(dataset_folder_root, dataset)
dir.create(dataset_folder, showWarnings = FALSE)

# Read in new log data
# Filter the log to show just market data files
market_data_log <- dplyr::filter(data_log, grepl(dataset, data_type))
#View(market_data_log$ticker)
#market_data_log$data_label <- tools::file_path_sans_ext(market_data_log$data_label)
#market_data_log$ticker <- paste(str_split_fixed(market_data_log$ticker, " ", 3)[,1], str_split_fixed(market_data_log$ticker, " ", 3)[,2])
#market_data_log$latest_date <- str_split_fixed(market_data_log$data_label, "__", 4)[,1]
#head(market_data_log)

# Break the logs down into separate tickers
length((unique(market_data_log$data_label)))
tickers <- sort(unique(market_data_log$data_label))
for (i in 1:length(tickers)) {
  # Define table name for the ticker
  table_name <- str_replace_all(tickers[i],"[[:punct:]\\s]+","_")
  # Get list of files relating to the ticker
  data_files <- dplyr::filter(market_data_log, grepl(tickers[i],data_label))
  # build a master dataframe
  # read in the first data file
  all_data <- read_csv(paste("datalog", data_files$filename[1], sep = "/"), col_types = cols(.default = "d", date = "c"))
  # add timestamp ID and source
  all_data$timestamp <- data_files$timestamp[1]
  all_data$source <- data_files$source[1]
  all_data <- all_data %>% gather(metric, value, -timestamp, -source, -date)
  # Append other files data if they exist
  if (nrow(data_files) >=2) {
    for (j in 2:nrow(data_files)-1) {
      table_data <- read_csv(paste("datalog", data_files$filename[j], sep = "/"), col_types = cols(.default = "d", date = "c"))
      if (nrow(table_data >= 1)) {
        table_data$timestamp <- data_files$timestamp[j]
        table_data$source <- data_files$source[j]
        table_data <- table_data %>% gather(metric, value, -timestamp, -source, -date)
      }
      all_data <- bind_rows(all_data, table_data)
      all_data <- all_data %>% drop_na()
      
    }
    
  }

  #all_data <- compact_log(all_data)
  persistent_storage <- file.path(dataset_folder, paste(table_name, "csv", sep = "."))
  
  if (file.exists(persistent_storage)) {
    persistent_data  <- read_csv(persistent_storage, col_types = cols(.default = "c", value = "d"))
    all_data <- bind_rows(all_data, persistent_data)
    #all_data <- compact_log(all_data)
    
  }
  write_csv(all_data, persistent_storage, append = FALSE)
  print(paste("Wrote table", table_name, "to disk.", 
              length(tickers) - i, "tickers left.", sep=" "))
}


print("################################")
print("")
end <- Sys.time()
print("############################")
print("Total Script Run Time")
print(end - begin)
print("############################")
print("Script completed")