# Clear environment
rm(list=ls())

source('shared_functions.R')

begin <- Sys.time()

# Create a dataframe of data log files
data_log <- convert_datalog_to_dataframe()

# Show avaialable datasets
unique(data_log$data_type)

dataset <- "constituent_list"

# create a folder for the dataset
dataset_folder <- file.path(dataset_directory, dataset)
dir.create(dataset_folder, showWarnings = FALSE)

# Read in new log data
# Filter the log to show just filtered data files
filtered_data_log <- data_log %>% 
  dplyr::filter(ext == "feather") %>% 
  dplyr::filter(grepl(dataset, data_type))
nrow(filtered_data_log)
# Process the first metadata file
if(nrow(filtered_data_log >= 1)) {
  index <- str_split_fixed(filtered_data_log[1,]$data_label, "_", 2)[2]
  date_of_fact <- str_split_fixed(filtered_data_log[1,]$data_label, "_", 2)[1]
  
  constituents <- read_feather(paste("datalog", filtered_data_log[1,]$filename, sep = "/"))
  
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
    new_constituents <- read_feather(paste("datalog", filtered_data_log[i,]$filename, sep = "/"))
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

# Convert date to date type
constituents$date <- ymd(constituents$date)

# Check if ticker has an existing dataset
persistent_storage <- file.path(dataset_folder, paste(dataset, "feather", sep = "."))
# If ticker does exist, merge new data with esixting dataset
if (file.exists(persistent_storage)) {
  #message <- paste(i, "/", number_of_tickers, table_name, "Merging data               ", sep=" ")
  #cat("\r", message)
  # Read in existing dataset
  persistent_data  <- read_feather(persistent_storage)
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


glimpse(constituents)
max(constituents$date)
min(constituents$date)

glimpse(filtered)
max(filtered$date)
min(filtered$date)

# Finally, write dataframe to disk
write_feather(filtered, persistent_storage)

end <- Sys.time()
print(end - begin)
