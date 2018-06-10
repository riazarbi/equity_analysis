begin <- Sys.time()

source('011_utils.R')
source('011_compact_log.R')
data_log <- convert_datalog_to_dataframe()
View(data_log)

# TO DO: NEED TO DEVELOP AUTOMATIC FILTERING, WHERE YOU OMIT LOGFILES 
# WHOSE TEXT STRINGS ARE NOT WELL FORMED.

unique(data_log$source)
unique(data_log$data_type)
unique(data_log$data_label)

###########################################################################################################################################
# UPDATING TICKER MARKET DATA
# Define the dataset
unique(data_log$data_type)
dataset <- "ticker_market_data"

# Create dataset save location if it does not exist
dataset_folder <- file.path(dataset_folder_root, dataset)
dir.create(dataset_folder, showWarnings = FALSE)

# Read in new log data
market_data_log <- dplyr::filter(data_log, grepl(dataset, data_type))
market_data_log$ticker <- str_split_fixed(market_data_log$data_label, "__", 4)[,2]
market_data_log$ticker <- tools::file_path_sans_ext(market_data_log$ticker)
market_data_log$ticker <- paste(str_split_fixed(market_data_log$ticker, " ", 3)[,1], str_split_fixed(market_data_log$ticker, " ", 3)[,2])
market_data_log$latest_date <- str_split_fixed(market_data_log$data_label, "__", 4)[,1]
head(market_data_log)

# Break the logs down into separate tickers
length((unique(market_data_log$ticker)))
tickers <- sort(unique(market_data_log$ticker))
for (i in 1:length(tickers)) {
  # Define table name for the ticker
  table_name <- str_replace_all(tickers[i],"[[:punct:]\\s]+","_")
  # Get list of files relating to the ticker
  data_files <- dplyr::filter(market_data_log, grepl(tickers[i],ticker))
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
      table_data$timestamp <- data_files$timestamp[j]
      table_data$source <- data_files$source[j]
      table_data <- table_data %>% gather(metric, value, -timestamp, -source, -date)
      all_data <- bind_rows(all_data, table_data)
      all_data <- all_data %>% drop_na()
      
    }
    
  }
  all_data <- compact_log(all_data)
  persistent_storage <- file.path(dataset_folder, paste(table_name, "csv", sep = "."))
  
  if (file.exists(persistent_storage)) {
    persistent_data  <- read_csv(persistent_storage, col_types = cols(.default = "c", value = "d"))
    all_data <- bind_rows(all_data, persistent_data)
    all_data <- compact_log(all_data)
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




######################################################################
# BUILDING FUNDAMENTAL DATASET

# Define the dataset
unique(data_log$data_type)
dataset <- "ticker_fundamental_data"

# Create dataset save location if it does not exist
dataset_folder <- file.path(dataset_folder_root, dataset)
dir.create(dataset_folder, showWarnings = FALSE)

fundamental_data_log <- dplyr::filter(data_log, grepl(dataset, data_type))
fundamental_data_log$ISIN <- str_split_fixed(fundamental_data_log$data_label, "__", 4)[,2]
fundamental_data_log$ISIN <- tools::file_path_sans_ext(fundamental_data_log$ISIN)
fundamental_data_log$ISIN <- str_split_fixed(fundamental_data_log$ISIN, " ", 2)[,1]
fundamental_data_log$latest_date <- str_split_fixed(fundamental_data_log$data_label, "__", 4)[,1]
head(fundamental_data_log)
unique(fundamental_data_log$ISIN)

ISINs <- unique(fundamental_data_log$ISIN)
for (i in 1:length(ISINs)) {
  if(nchar(ISINs[i]) == 0){
    i <- i+1
  }
  # Define table name
  table_name <- str_replace_all(ISINs[i],"[[:punct:]\\s]+","_")
  # Get list of files relating to the ticker
  data_files <- dplyr::filter(fundamental_data_log, grepl(ISINs[i],ISIN))
  # build a dat aframe to commit
  # read in the first data file
  print("    processing logfile 1")
  all_data <- read_csv(paste("datalog", data_files$filename[1], sep = "/"), col_types = cols(.default = "d", date = "c"))
  # add timestamp ID and source
  if( nrow(all_data) >= 1) {
      all_data$timestamp <- data_files$timestamp[1]
      all_data$source <- data_files$source[1]
      all_data <- all_data %>% gather(metric, value, -timestamp, -source, -date)
  }
  else{
    rm(all_data)
    all_data <- setNames(data.frame(matrix(ncol = 4, nrow = 0)), 
                         c("timestamp", "source", "date", "value"))
  }
  
  # Append other files data if they exist
  if (nrow(data_files) >=2) {
    for (j in 2:nrow(data_files)-1) {
      cat("\r", paste("    processing logfile", j))
      table_data <- read_csv(paste("datalog", data_files$filename[j], sep = "/"), col_types = cols(.default = "d", date = "c"))
      # sometimes the files exist but are empty, so check they have records
      if(nrow(table_data) >= 1) {
        table_data$timestamp <- data_files$timestamp[j]
        table_data$source <- data_files$source[j]
        table_data <- table_data %>% gather(metric, value, -timestamp, -source, -date)
        all_data <- bind_rows(all_data, table_data)
        all_data <- all_data %>% drop_na()}
    }
    
  }

  all_data <- compact_log(all_data)
  persistent_storage <- file.path(dataset_folder, paste(table_name, "csv", sep = "."))
  
  if (file.exists(persistent_storage)) {
    persistent_data  <- read_csv(persistent_storage, col_types = cols(.default = "c", value = "d"))
    all_data <- bind_rows(all_data, persistent_data)
    all_data <- compact_log(all_data)
  }
  
  write_csv(all_data, persistent_storage, append = FALSE)
  print(paste("Wrote table", table_name, "to disk.", 
              length(tickers) - i, "tickers left.", sep=" "))
  
  }






#########################################################################
# Building metadata dataset
metadata_log <- dplyr::filter(data_log, grepl("metadata_array", data_type))
metadata_log$latest_date <- tools::file_path_sans_ext(metadata_log$data_label)
head(metadata_log)


# Connec to database
db <- dbConnect(SQLite(), "datasets/metadata.sqlite")
table_name <- "metadata"
# Get list of files relating to the ticker
data_files <- metadata_log
# build a dat aframe to commit
# read in the first data file
all_data <- read_csv(paste("datalog", data_files$filename[1], sep = "/"), col_types = cols(.default = "d", date = "c"))
# add timestamp ID and source
all_data$timestamp <- data_files$timestamp[1]
all_data$source <- data_files$source[1]
all_data <- all_data %>% gather(metric, value, -TIMESTAMP, -source, -date)
# Append other files data if they exist
if (nrow(data_files) >=2) {
  for (j in 2:nrow(data_files)-1) {
    table_data <- read_csv(paste("datalog", data_files$filename[j], sep = "/"), col_types = cols(.default = "d", date = "c"))
    # sometimes the files exist but are empty, so check they have records
    if(nrow(table_data) >= 1) {
      table_data$TIMESTAMP <- data_files$timestamp[j]
      table_data$source <- data_files$source[j]
      table_data <- table_data %>% gather(metric, value, -TIMESTAMP, -source, -date)
      all_data <- bind_rows(all_data, table_data)
      all_data <- all_data %>% drop_na()}
  }
  
}
# Append the dataframe to the SQLite table
dbWriteTable(db, table_name, all_data, append = TRUE)
# Delete any duplicates, where a duplicate is a specific day from the same timestamp
clear_duplicates <- dbSendQuery(db, paste("DELETE FROM '", table_name,
                                          "' WHERE rowid NOT IN (
                                          SELECT MIN(rowid) 
                                          FROM '", table_name, 
                                          "' GROUP BY timestamp, date, metric)", sep=""))
dbClearResult(clear_duplicates)


# Testing a SQL table
length(dbListTables(db))
dbListTables(db)
dbListFields(db, table_name)
sample <- dbGetQuery(db, paste('SELECT * FROM',table_name))
nrow(sample)
#dbGetQuery(db, paste("PRAGMA table_info(", "SNH_SJ",");"))
View(dbGetQuery(db, paste("SELECT * FROM",table_name,"WHERE date BETWEEN '2011-01-11' AND '2011-08-11'")))

