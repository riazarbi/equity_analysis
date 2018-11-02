print("")
print("NEXT: Creating metadata dataset...")
# Clear environment
rm(list=ls())
# Read in data_pipeline functions
source("set_paths.R")
source("data_pipeline_scripts/data_pipeline_functions.R")
# Time the script
begin <- Sys.time()

dataset <- "metadata_array"

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

print("Processing ticker metadata files...")
# Process the first metadata file
if(nrow(filtered_data_log >= 1)) {
  metadata <- read_feather(paste("datalog", filtered_data_log[1,]$filename, sep = "/"))
  if (nrow(metadata) >= 1) {
    # add timestamp ID and source
    metadata$timestamp <- filtered_data_log$timestamp[1]
    metadata$source <- filtered_data_log$source[1]
    # Melt the data into the correct format
    all_data <- metadata %>% gather(metric, value, -timestamp, -source, -market_identifier)
    }
}

if(nrow(filtered_data_log > 1)) {
  for (i in 1:nrow(filtered_data_log)) {
    metadata <- read_feather(paste("datalog", filtered_data_log[i,]$filename, sep = "/"))
    if (nrow(metadata) >= 1) {
      # add timestamp ID and source
      metadata$timestamp <- filtered_data_log$timestamp[i]
      metadata$source <- filtered_data_log$source[i]
      # Melt the data into the correct format
      new_data <- metadata %>% gather(metric, value, -timestamp, -source, -market_identifier)
      }
      # Bind the data
      all_data <- bind_rows(all_data, new_data)
      # Drop NA values
      all_data <- all_data %>% drop_na()

  }}

# Check if ticker has an existing dataset
persistent_storage <- file.path(dataset_folder, paste(dataset, "feather", sep = "."))
  # If ticker does exist, merge new data with esixting dataset
if (file.exists(persistent_storage)) {
  # Read in existing dataset
  persistent_data  <- read_feather(persistent_storage)
  # Bind the two datasets
  all_data <- bind_rows(all_data, persistent_data)
  }
    
# Keep only the most recent timestamp
# Of identical entries
filtered <- all_data %>% 
  mutate(key=paste(source, market_identifier, metric, value, sep = "|")) %>%
  arrange(desc(key)) %>%
  filter(key != lag(key, default="0")) %>% 
  select(-key)

# Finally, write dataframe to disk
write_feather(filtered, persistent_storage)
# Confirm it's there
glimpse(feather(persistent_storage))
# Benchmark script timing
end <- Sys.time()
print(end - begin)
