# Log the time taken for the script
begin <- Sys.time()

#######################################################################################
print("################################")
print("Loading libraries")

# Clear environment
rm(list=ls())

# Load libraries
library(tidyverse)
library(Rblpapi)
# connect to Bloomberg API
con <- blpConnect()

#######################################################################################
print("################################")
print("Defining functions")

### Filter log directory files
get_file_list <- function(timestamp_days_age_limit, 
                          source_substring, 
                          data_type_substring, 
                          data_label_substring) 
{  
  file_list <- list.files(data_directory)
  data_log <- as.data.frame(str_split_fixed(file_list, "__", 4), stringsAsFactors=FALSE)
  colnames(data_log) <- c("timestamp", "source", "data_type", "data_label")
  data_log$filename <- file_list
  
  timestamp_days_age_limit <- timestamp_days_age_limit * 8640000000
  timestamp_days_age_limit <- as.numeric(as.POSIXct(Sys.time()))*10^5 - timestamp_days_age_limit

  data_log <- dplyr::filter(data_log, timestamp >= timestamp_days_age_limit)
  data_log <- dplyr::filter(data_log, grepl(source_substring,source))
  data_log <- dplyr::filter(data_log, grepl(data_type_substring, data_type))
  data_log <- dplyr::filter(data_log, grepl(data_label_substring, data_label))
  return(data_log$filename)
}

#######################################################################################
print("################################")
print("Setting directory path variables and loading lists")
smallbegin <- Sys.time()

# Get working directory
working_directory <- getwd()

# Define directory paths
dimensions_directory <- file.path(working_directory, "dimensions")
data_directory <- file.path(working_directory, "data")
# Make sure data_directory exists, create it if it does not
dir.create(data_directory, showWarnings = FALSE)

# Define dates and create a date dimension
# Periodicity is monthly
today <- Sys.Date()
start_date <- as.Date("1960-01-31")

# Create a date dimension, save to file
dates <- seq.Date(start_date, today, by = "month")
dates_file <- file.path(dimensions_directory, "dates.csv")
write.table(dates, dates_file, col.names = FALSE, row.names = FALSE)
rm(dates_file)

# Read in the indexes dimension
# These are stock indexs (eg. SPX Index)
indexes_file <- file.path(dimensions_directory, "indexes.csv")
indexes <- read.csv(indexes_file, header = FALSE, colClasses = "character")
indexes <- indexes[,1]
rm(indexes_file)

# Read in the fundamental data fields dimension
fundamental_fields_file <- file.path(dimensions_directory, "fundamental_fields.csv")
fundamental_fields <- read.csv(fundamental_fields_file, header = FALSE, colClasses = "character")
fundamental_fields <- fundamental_fields[,1]
rm(fundamental_fields_file)

# Read in the marketdata dimension
market_fields_file <- file.path(dimensions_directory, "market_fields.csv")
market_fields <- read.csv(market_fields_file, header = FALSE, colClasses = "character")
market_fields <- market_fields[,1]
rm(market_fields_file)

# Read in the metadata dimension
metadata_fields_file <- file.path(dimensions_directory, "metadata_fields.csv")
metadata_fields <- read.csv(metadata_fields_file, header = FALSE, colClasses = "character")
metadata_fields <- metadata_fields[,1]
rm(metadata_fields_file)

# Read in the tickers field dimension
tickers_file <- file.path(dimensions_directory, "tickers.csv")
tickers <- read.csv(tickers_file, header = FALSE, colClasses = "character")
tickers

# Print time elapsed
end <- Sys.time()
print("Time Elapsed for this section:")
print(end - smallbegin)
print("")

#######################################################################################
print("################################")
print("Querying constituents for 20 years since present")
smallbegin <- Sys.time()

# This section queries each index in the indexes list for each date in the 
# dates list and saves the result to a data log file

# Index loop starts here
index_iter <- 1
while (index_iter <= length(indexes)) {
  the_index_ticker <- paste(indexes[index_iter], "Index", sep = " ")
  the_index <- indexes[index_iter]
  index_iter <- index_iter + 1
  print(the_index_ticker)
  # Date loop starts here
  date_iter <- length(dates) - 60
  while(date_iter <= length(dates)) {
    the_date <- (gsub("-", "", dates[date_iter]))  
    date_iter <- date_iter + 1
    # query options
    overrd <- c("END_DT"= the_date)
    # the actual bloomberg query
    query <- bds(the_index_ticker,
          "INDX_MWEIGHT_HIST", overrides = overrd)
    # Save the query to log
    # Defining the filename parameters for logging
    data_type <- "constituent_list" 
    data_source <- "bloomberg"
    timestamp <- as.numeric(as.POSIXct(Sys.time()))*10^5
    data_identifier <- paste(the_date, the_index, sep = "_") 
    file_string <- paste(timestamp, data_source, data_type, data_identifier, sep = "__")
    file_string <- paste(file_string, ".csv", sep = "")
    save_file <- file.path(data_directory, file_string)
    #saving file
    write.table(query, save_file, row.names = FALSE, sep = ",")
  }
}

# Print time elapsed
end <- Sys.time()
print("Time Elapsed for this section:")
print(end - smallbegin)
print("")

#######################################################################################
print("################################")
print("Creating a list of unique tickers from the constituent lists")
smallbegin <- Sys.time()

# This section extracts all the constituent list files from
# the data log and compiles a unique ticker list from
# their contents

# Filter the data log to just look at bloomberg constituent lists
file_list <- get_file_list(1, "bloomberg", "constituent_list", "TOP40")
file_list
# Open each file and concatenate
for (file in file_list) {
  file <- file.path(data_directory, file)
  if(!exists("constituents_merge")) {
    constituents_merge <- suppressMessages(read_csv(file))
  }
  
  if(exists("constituents_merge")) {
    temp_constituents_merge <- suppressMessages(read_csv(file))
    constituents_merge <- suppressMessages(bind_rows(constituents_merge, temp_constituents_merge))
    rm(temp_constituents_merge)
  }
}

# cleaning up
rm(file_list)

# extract only nique values
tickers <- unique(pull(constituents_merge[,1]))
class(tickers)
tickers <- paste(tickers, " Equity", sep = "")

rm(constituents_merge)
tickers_file <- file.path(dimensions_directory, "tickers.csv")
write.table(tickers, tickers_file, col.names = FALSE, row.names = FALSE)

# Print time elapsed
end <- Sys.time()
print("Time Elapsed for this section:")
print(end - smallbegin)
print("")

#######################################################################################
print("################################")
print("Querying metadata for unique tickers")
smallbegin <- Sys.time()

# This section takes the unique ticker list passed from the last section and 
# Queries the metadata for all tickers.
# It returns a flat data frame which is saved to file

# the actual query
metadata <- bdp(tickers, metadata_fields)

# Defining the filename parameters for logging
data_source <- "bloomberg"
query <- metadata
data_type <- "metadata_array" 
timestamp <- as.numeric(as.POSIXct(Sys.time()))*10^5
data_identifier <- paste(the_date)
file_string <- paste(timestamp, data_source, data_type, data_identifier, sep = "__")
file_string <- paste(file_string, ".csv", sep = "")
save_file <- file.path(data_directory, file_string)
# Saving the file
write.table(query, save_file, row.names = FALSE, sep = ",")
rm(query)

# Print time elapsed
end <- Sys.time()
print("Time Elapsed for this section:")
print(end - smallbegin)
print("")

#######################################################################################
print("################################")
print("Querying market data for unique tickers")
smallbegin <- Sys.time()

# This section takes the unique ticker list, the marketdata 
# field list, a start and end date and returns a list of 
# flat data frames.
# Then it saves each flat data frame to the data log.

# Need to chunk the tickers to keep the queries manageable
ticker_iter <- 1
while (ticker_iter <= length(tickers)) {
  startticker <- ticker_iter
  endticker <- min(ticker_iter + 100, length(tickers))
  ticker_iter <- endticker + 1

  # the actual query
  opt <- c("currency"="ZAR")
  marketdata <- bdh(securities = tickers[startticker:endticker],
           fields = market_fields,
           start.date = start_date,
           end.date = as.Date(today), options=opt)

  # Save each flat dataframe in the object to a separate
  # log file
  marketdata_iter <- 1
  while (marketdata_iter <= length(marketdata)) {
    # Defining the filename parameters for logging
    timestamp <- as.numeric(as.POSIXct(Sys.time()))*10^5
    data_source <- "bloomberg"
    query <- marketdata[[marketdata_iter]]
    name <- names(marketdata[marketdata_iter])
    data_type <- "ticker_market_data" 
    data_identifier <- paste(the_date, name, sep = "__")
    file_string <- paste(timestamp, data_source, data_type, data_identifier, sep = "__")
    file_string <- paste(file_string, ".csv", sep = "")
    save_file <- file.path(data_directory, file_string)
    # saving the file
    write.table(query, save_file, row.names = FALSE, sep = ",")
    rm(query)
    marketdata_iter <- marketdata_iter + 1
  }
  rm(marketdata_iter)
}
rm(ticker_iter)
rm(startticker)
rm(endticker)

# Print time elapsed
end <- Sys.time()
print("Time Elapsed for this section:")
print(end - smallbegin)
print("")

#######################################################################################
print("################################")
print("Creating a dimension of unique ISINs for fundamental data queries")
smallbegin <- Sys.time()

# This section obtains a unique list of ISINs 
# We need these because fundamental fields
# Are often blank for market ticker symbols
# It's better to get fundamental data for the ISIN
# BUT rmeember to keep the currency the same as the 
# Market data

# Create ISIN dimension
# Add the Equity suffix otherwise queries don't work
ISINs <- paste(metadata$ID_ISIN , " Equity", sep = "")
ISINs <- unique(ISINs)
ISINs_file <- file.path(dimensions_directory, "ISINs.csv")
# Write to disk
write.table(ISINs, ISINs_file, col.names = FALSE, row.names = FALSE)

# Print time elapsed
end <- Sys.time()
print("Time Elapsed for this section:")
print(end - smallbegin)
print("")

#######################################################################################
print("################################")
print("Querying fundamental data for unique tickers")
smallbegin <- Sys.time()

# This section queries fundamental fields for a list of
# ISIN tickers for a prespecified range of dates.
# Because there appears to be an undocumented limit to number of fields
# queried, we chunk the fields.
# We also chunk the ISINs so that no single query gets too big.

# Need to chunk the ISINs to keep the queries manageable
ISIN_iter <- 1
while (ISIN_iter <= length(ISINs)) {
  startISIN <- ISIN_iter
  endISIN <- min(ISIN_iter + 100, length(ISINs))
  ISIN_iter <- endISIN + 1

  # Need to chunk the fields
  fundamental_fields_iter <- 1
  while (fundamental_fields_iter <= length(fundamental_fields)) {
    startfield <- fundamental_fields_iter
    endfield <- min(fundamental_fields_iter + 10, length(fundamental_fields))
    print(startfield)
    print(endfield)
    fundamental_fields_iter <- endfield + 1
  
    # Now run the query on chunked fields
    opt <- c("periodicitySelection"="MONTHLY", "currency"="ZAR")
    fundamentaldata <- bdh(securities = ISINs[startISIN:endISIN],
                       fields = fundamental_fields[startfield:endfield],
                       start.date = start_date,
                       end.date = as.Date(today),
                       options=opt
                       )

    # Save  chunked fundamental data queries to log
    fundamentaldata_iter <- 1
    while (fundamentaldata_iter <= length(fundamentaldata)) {
      # Defining the filename parameters for logging
      timestamp <- as.numeric(as.POSIXct(Sys.time()))*10^
        5
      data_source <- "bloomberg"
      query <- fundamentaldata[[fundamentaldata_iter]]
      name <- names(fundamentaldata[fundamentaldata_iter])
      data_type <- "ticker_fundamental_data" 
      data_identifier <- paste(the_date, name, sep = "__")
      file_string <- paste(timestamp, data_source, data_type, data_identifier, sep = "__")
      file_string <- paste(file_string, ".csv", sep = "")
      save_file <- file.path(data_directory, file_string)
      # saving the file
      write.table(query, save_file, row.names = FALSE, sep = ",")
      fundamentaldata_iter <- fundamentaldata_iter + 1
    }
  }
}
  
# Print time elapsed
end <- Sys.time()
print("Time Elapsed for this section:")
print(end - smallbegin)
print("")

#######################################################################################
print("################################")
print("")
end <- Sys.time()
print("############################")
print("Total Script Run Time")
print(end - begin)
print("############################")
print("Script completed")

# Time to pull all data for 1 month
# Time difference of 1.72415 mins
# Time to pull 15 years
# Time difference of 12.86575 mins