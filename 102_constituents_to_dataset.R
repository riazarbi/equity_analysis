source('011_utils.R')
source("011_ticker_datalog_to_dataset.R")

begin <- Sys.time()

# Create a dataframe of data log files
data_log <- convert_datalog_to_dataframe()

# Prefilter the datalog and delete logfiles
#data_log <- clean_data_log(data_log)
# TO DO: NEED TO DEVELOP AUTOMATIC FILTERING, WHERE YOU OMIT LOGFILES 
# WHOSE TEXT STRINGS ARE NOT WELL FORMED

# Show avaialable datasets
unique(data_log$data_type)

dataset <- "constituent_list"

# Start function here
# Create dataset save location if it does not exist
dataset_folder <- file.path(dataset_directory, dataset)
dir.create(dataset_folder, showWarnings = FALSE)

# Read in new log data
# Filter the log to show just filtered data files
filtered_data_log <- dplyr::filter(data_log, grepl(dataset, data_type))
nrow(filtered_data_log)
# Process the first metadata file
if(nrow(filtered_data_log >= 1)) {
  index <- str_split_fixed(filtered_data_log[1,]$data_label, "_", 2)[2]
  date_of_fact <- str_split_fixed(filtered_data_log[1,]$data_label, "_", 2)[1]
  
  constituents <- read_csv(paste("datalog", filtered_data_log[1,]$filename, sep = "/"), 
                       col_types = cols(.default = "c"))
  
  constituents <- constituents[,-2]
  colnames(constituents)[1] <- "ticker"
  
  if (nrow(constituents) >= 1) {
  # add timestamp ID and source
  constituents$timestamp <- filtered_data_log$timestamp[1]
  constituents$source <- filtered_data_log$source[1]
  constituents$date <- date_of_fact
  constituents$index <- index
}
}

#View(constituents)

if(nrow(filtered_data_log > 1)) {
  for (i in 1:nrow(filtered_data_log)) {
    new_constituents <- read_csv(paste("datalog", filtered_data_log[i,]$filename, sep = "/"), 
                         col_types = cols(.default = "c"))
    index <- str_split_fixed(filtered_data_log[i,]$data_label, "_", 2)[2]
    date_of_fact <- str_split_fixed(filtered_data_log[i,]$data_label, "_", 2)[1]
    new_constituents <- new_constituents[,-2]
    colnames(new_constituents)[1] <- "ticker"
    if (nrow(new_constituents) >= 1) {
      # add timestamp ID and source
      new_constituents$timestamp <- filtered_data_log$timestamp[i]
      new_constituents$source <- filtered_data_log$source[i]
      new_constituents$date <- date_of_fact
      new_constituents$index <- index
      }
    # Bind the data
    constituents <- bind_rows(constituents, new_constituents)
    }
    # Drop NA values
    constituents <- constituents %>% drop_na()
    
  }


# Check if ticker has an existing dataset
persistent_storage <- file.path(dataset_folder, paste(dataset, "csv", sep = "."))
# If ticker does exist, merge new data with esixting dataset
if (file.exists(persistent_storage)) {
  #message <- paste(i, "/", number_of_tickers, table_name, "Merging data               ", sep=" ")
  #cat("\r", message)
  # Read in existing dataset
  persistent_data  <- read_csv(persistent_storage, col_types = cols(.default = "c"))
  # Bind the two datasets
  constituents <- bind_rows(constituents, persistent_data)
  # Compact the merged dataframe to exlude repetitive data
  # Note: this for any data that has not changed, this has the effect of
  # Updating the timestamp to reflect when last updated
}

# Keep only the most recent timestamp
# Of identical entries
filtered <- constituents %>% 
  mutate(key=paste(ticker, source, date, index, sep = "|")) %>%
  arrange(desc(key)) %>%
  filter(key != lag(key, default="0")) %>% 
  select(-key)

str(constituents)
str(filtered)
# Finally, write dataframe to disk
write_csv(filtered, persistent_storage, append = FALSE)

end <- Sys.time()
print(end - begin)