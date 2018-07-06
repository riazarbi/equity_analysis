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

# Extract data from fundamental data logs and save to ISIN-labeled datasets
ticker_datalog_to_dataset("ticker_fundamental_data")

# Extract data from market data logs and save to ticker-labeled datasets
ticker_datalog_to_dataset("ticker_market_data")

end <- Sys.time()
print(end - begin)