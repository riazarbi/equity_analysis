source('011_utils.R')
source("011_ticker_datalog_to_dataset.R")

begin <- Sys.time()

# Create a dataframe of data log files
data_log <- convert_datalog_to_dataframe()

# Prefilter the datalog and delete logfiles
data_log <- clean_data_log(data_log)
# TO DO: NEED TO DEVELOP AUTOMATIC FILTERING, WHERE YOU OMIT LOGFILES 
# WHOSE TEXT STRINGS ARE NOT WELL FORMED

# Show avaialable datasets
unique(data_log$data_type)

dataset <- "metadata_array"

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
  metadata <- read_csv(paste("datalog", filtered_data_log[1,]$filename, sep = "/"), 
                       col_types = cols(.default = "c"))
  if (nrow(metadata) >= 1) {
    # add timestamp ID and source
    metadata$timestamp <- filtered_data_log$timestamp[1]
    metadata$source <- filtered_data_log$source[1]
    # Melt the data into the correct format
    all_data <- metadata %>% gather(metric, value, -timestamp, -source, -TICKER_AND_EXCH_CODE)}
}

if(nrow(filtered_data_log > 1)) {
  for (i in 1:nrow(filtered_data_log)) {
    metadata <- read_csv(paste("datalog", filtered_data_log[i,]$filename, sep = "/"), 
                         col_types = cols(.default = "c"))
    if (nrow(metadata) >= 1) {
      # add timestamp ID and source
      metadata$timestamp <- filtered_data_log$timestamp[i]
      metadata$source <- filtered_data_log$source[i]
      # Melt the data into the correct format
      new_data <- metadata %>% gather(metric, value, -timestamp, -source, -TICKER_AND_EXCH_CODE)}
      # Bind the data
      all_data <- bind_rows(all_data, new_data)
      # Drop NA values
      all_data <- all_data %>% drop_na()

  }}

# Check if ticker has an existing dataset
persistent_storage <- file.path(dataset_folder, paste(dataset, "csv", sep = "."))
  # If ticker does exist, merge new data with esixting dataset
if (file.exists(persistent_storage)) {
  # Read in existing dataset
  persistent_data  <- read_csv(persistent_storage, col_types = cols(.default = "c"))
  # Bind the two datasets
  all_data <- bind_rows(all_data, persistent_data)
  }
    
# Keep only the most recent timestamp
# Of identical entries
filtered <- all_data %>% 
  mutate(key=paste(source, TICKER_AND_EXCH_CODE, metric, value, sep = "|")) %>%
  arrange(desc(key)) %>%
  filter(key != lag(key, default="0")) %>% 
  select(-key)
str(filtered)

# Finally, write dataframe to disk
write_csv(filtered, persistent_storage, append = FALSE)

end <- Sys.time()
print(end - begin)
