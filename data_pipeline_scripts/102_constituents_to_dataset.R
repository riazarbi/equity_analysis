print("")
print("NEXT: Creating index constituents dataset...")
# Clear environment
rm(list=ls())
# Read in data_pipeline functions
source('data_pipeline_functions.R')
# Time the script
begin <- Sys.time()

dataset <- "constituent_list"

# Create a dataframe of data log files
print("Scanning datalog...")
data_log <- convert_datalog_to_dataframe()

print("Reading datalog files...")
# create a folder for the dataset
dataset_folder <- file.path(dataset_directory, dataset)
dir.create(dataset_folder, showWarnings = FALSE)
# Read in new log data
# Filter the log to show just filtered data files
filtered_data_log <- data_log %>% 
  dplyr::filter(ext == "feather") %>% 
  dplyr::filter(grepl(dataset, data_type))
nrow(filtered_data_log)

print("Processing constituent list files...")
# Process the first constituent list file
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
  # Read in existing dataset
  persistent_data  <- read_feather(persistent_storage)
  # Bind the two datasets
  constituents <- bind_rows(constituents, persistent_data)
}

# Keep only the most recent timestamp
# Of identical entries
filtered <- constituents %>% 
  mutate(key=paste(ticker, source, date, index, sep = "|")) %>%
  arrange(desc(key)) %>%
  filter(key != lag(key, default="0")) %>% 
  select(-key)

# Finally, write dataframe to disk
write_feather(filtered, persistent_storage)

end <- Sys.time()
print(end - begin)
