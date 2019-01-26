# Clear environment
rm(list=ls())
print("NEXT: Loading libraries, defining paths and reading in parameters...")
######################################################################################
# DEFINE PARAMETERS

# Specify indexes
# Specify currency
indexes <- c("JALSH", "TOP40")
currency <- "ZAR"
index_query_months <- 3
ticker_query_start_date <- as.Date("1960-01-31")

#######################################################################################
# DEFINE DIMENSIONS

# Fundamental Fields
fundamental_fields <- c("PE_RATIO",
                        "DIVIDEND_INDICATED_YIELD",
                        "EQY_DVD_YLD_12M",
                        "EQY_DVD_YLD_12M_NET",
                        "SHAREHOLDER_YIELD_EX_DEBT",
                        "SHAREHOLDER_YIELD",
                        "PX_TO_BOOK_RATIO",
                        "RETURN_COM_EQY",
                        "PX_TO_CASH_FLOW",
                        "PX_TO_FREE_CASH_FLOW",
                        "PX_TO_SALES_RATIO",
                        "BS_SH_OUT",
                        "SALES_REV_TURN",
                        "IS_COGS_TO_FE_AND_PP_AND_G",
                        "IS_OPERATING_EXPN",
                        "IS_OPER_INC",
                        "IS_INT_EXPENSE",
                        "IS_FOREIGN_EXCH_LOSS",
                        "IS_NET_NON_OPER_LOSS",
                        "IS_INC_TAX_EXP",
                        "IS_INC_BEF_XO_ITEM",
                        "IS_XO_LOSS_BEF_TAX_EFF",
                        "MIN_NONCONTROL_INTEREST_CREDITS",
                        "NET_INCOME",
                        "EBIT",
                        "PRETAX_INC",
                        "IS_TOT_CASH_PFD_DVD",
                        "IS_TOT_CASH_COM_DVD",
                        "REINVEST_EARN",
                        "IS_DEPR_EXP",
                        "IS_RD_EXPEND",
                        "BS_CASH_NEAR_CASH_ITEM",
                        "BS_MKT_SEC_OTHER_ST_INVEST",
                        "BS_ACCT_NOTE_RCV",
                        "BS_INVENTORIES",
                        "BS_OTHER_CUR_ASSET",
                        "BS_CUR_ASSET_REPORT",
                        "BS_GROSS_FIX_ASSET",
                        "BS_ACCUM_DEPR",
                        "BS_NET_FIX_ASSET",
                        "BS_LT_INVEST",
                        "BS_OTHER_ASSETS_DEF_CHRG_OTHER",
                        "BS_TOT_ASSET",
                        "BS_ACCT_PAYABLE",
                        "BS_ST_BORROW",
                        "BS_OTHER_ST_LIAB",
                        "BS_CUR_LIAB",
                        "BS_LT_BORROW",
                        "BS_OTHER_LT_LIABILITIES",
                        "BS_TOT_LIAB2",
                        "BS_PFD_EQTY_&_HYBRID_CPTL",
                        "MINORITY_NONCONTROLLING_INTEREST",
                        "TOT_COMMON_EQY",
                        "TOTAL_EQUITY",
                        "TOT_LIAB_AND_EQY",
                        "CF_NET_INC",
                        "CF_DEPR_AMORT",
                        "CF_OTHER_NON_CASH_ADJUST",
                        "CF_CHNG_NON_CASH_WORK_CAP",
                        "CF_CASH_FROM_OPER",
                        "CF_DISP_FIX_ASSET",
                        "CF_CAP_EXPEND_PRPTY_ADD",
                        "CF_DECR_INVEST",
                        "CF_INCR_INVEST",
                        "CF_OTHER_INV_ACT",
                        "CF_CASH_FROM_INV_ACT",
                        "CF_DVD_PAID",
                        "CF_INCR_ST_BORROW",
                        "CF_INCR_LT_BORROW",
                        "CF_REIMB_LT_BORROW",
                        "CF_INCR_CAP_STOCK",
                        "CF_DECR_CAP_STOCK",
                        "CF_OTHER_FNC_ACT",
                        "CF_CASH_FROM_FNC_ACT",
                        "CF_NET_CHNG_CASH",
                        "CF_CASH_PAID_FOR_TAX",
                        "CF_ACT_CASH_PAID_FOR_INT_DEBT",
                        "CF_FREE_CASH_FLOW",
                        "EBITDA")
  
# Marketdata Fields
market_fields <- c("PX_OPEN",
                   "PX_OFFICIAL_CLOSE",
                   "PX_LAST",
                   "PX_HIGH",
                   "PX_LOW",
                   "VOLUME",
                   "LAST_TRADE",
                   "PX_ASK",
                   "PX_BID",
                   "MID",
                   "NUM_TRADES",
                   "TOT_RETURN_INDEX_GROSS_DVDS",
                   "DAY_TO_DAY_TOT_RETURN_GROSS_DVDS",
                   "CUR_MKT_CAP",
                   "PE_RATIO",
                   "DIVIDEND_INDICATED_YIELD",
                   "EQY_DVD_YLD_12M",
                   "EQY_DVD_YLD_12M_NET",
                   "SHAREHOLDER_YIELD_EX_DEBT",
                   "SHAREHOLDER_YIELD",
                   "PX_TO_BOOK_RATIO",
                   "RETURN_COM_EQY",
                   "PX_TO_CASH_FLOW",
                   "PX_TO_FREE_CASH_FLOW",
                   "PX_TO_SALES_RATIO",
                   "BS_SH_OUT")
  
# metadata Fields
metadata_fields <- c("TICKER",
                     "TICKER_AND_EXCH_CODE",
                     "EQY_FUND_TICKER",
                     "CNTRY_OF_DOMICILE",
                     "GICS_SECTOR_NAME",
                     "GICS_INDUSTRY_GROUP_NAME",
                     "GICS_INDUSTRY_NAME",
                     "GICS_SUB_INDUSTRY_NAME",
                     "GICS_SUB_INDUSTRY",
                     "LONG_COMP_NAME",
                     "ID_BB_COMPANY",
                     "ID_BB_GLOBAL",
                     "ID_ISIN",
                     "CRNCY",
                     "QUOTED_CRNCY")

# Define dates and create a date dimension
# Periodicity is monthly but you can chosse any periodicity you like
today <- Sys.Date()
start_date <- ticker_query_start_date
dates <- seq.Date(start_date, today, by = "month")

#######################################################################################
# MAKE SURE THE DIRECTORY STRUCTURE IS IN PLACE

# Get working directory
working_directory <- getwd()
# Define directory paths
data_root <- file.path(working_directory, "data")
# Make sure data_directory exists, create it if it does not
dir.create(data_root, showWarnings = FALSE)
# Define datalog path
data_directory <- file.path(data_root, "datalog")
# Make sure data_directory exists, create it if it does not
dir.create(data_directory, showWarnings = FALSE)

#######################################################################################
# LOAD LIBRARIES AND DEFINE FUNCTIONS

# Load libraries
library(tidyverse)
library(Rblpapi)

# Filter log directory files
get_file_list <- function( 
  source_substring, 
  data_type_substring, 
  data_label_substring) {  
  file_list <- list.files(data_directory)
  data_log <- as.data.frame(str_split_fixed(file_list, "__", 4), stringsAsFactors=FALSE)
  colnames(data_log) <- c("timestamp", "source", "data_type", "data_label")
  data_log$filename <- file_list
  data_log <- dplyr::filter(data_log, grepl(source_substring,source))
  data_log <- dplyr::filter(data_log, grepl(data_type_substring, data_type))
  data_log <- dplyr::filter(data_log, grepl(data_label_substring, data_label))
  return(data_log$filename)
}

#######################################################################################
print("")
print("NEXT: Connecting to Bloomberg client...")

# Log the time taken for the script
begin <- Sys.time()

# connect to Bloomberg API
conn <- tryCatch(blpConnect(), error = function(e) print("Bloomberg connection failed."))

# This doesn't work it's 
if(class(conn) != "externalptr") {
  print("Connection to Bloomberg failed. Skipping datalog queries.")
} else {


#######################################################################################
# THE ORIGIN DATASET FOR THIS PLATFORM IS THE TICKER INDEX. 
# WE QUERY INDEX MEMBERSHIP FOR A RANGE OF DATES WE WANT TO BACKTEST.
# THEN WE RUN OTHER CODE TO PULL ATTRIBUTES OF THOSE INDEX MEMBERS THAT WE CAN USE IN 
# PREDICTING PERFORMANCE OF THOSE MEMBERS OVER TIME
# FUNDAMENTALLY WE ARE INTERESTED IN ASSESSING WHETHER A PARTICULAR SORTING RULE, 
# BASED ON THOSE ATTRIBUTES, HAS EXPLANATORY VALUE IN PREDICTING PRICE PERFORMANCE

print("################################")
print("Querying index constituents")
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
  date_iter <- length(dates) - index_query_months
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

# BUG: If a constituents file is empty, then the whole thing breaks

# Filter the data log to just look at bloomberg constituent lists
file_list <- get_file_list("bloomberg", "constituent_list", "*")

# Open each file and concatenate
for (file in file_list) {
  file <- file.path(data_directory, file)
  if (nrow(read_csv(file)) <= 1) {
    print("Empty file. Deleting")
    file.remove(file)
  }
  else if (!exists("constituents_merge")) {
    constituents_merge <- suppressMessages(read_csv(file))
  }
  else if (exists("constituents_merge")) {
    temp_constituents_merge <- suppressMessages(read_csv(file))
    constituents_merge <- suppressMessages(bind_rows(constituents_merge, temp_constituents_merge))
    rm(temp_constituents_merge)
  }
}

# cleaning up
rm(file_list)

# extract only unique values
tickers <- unique(pull(constituents_merge[,1]))
class(tickers)
tickers <- paste(tickers, " Equity", sep = "")

rm(constituents_merge)
length(tickers)
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
#View(metadata)
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

#####################################################################################

# NEED TO BUILD IN MULTI-CURRENCY SUPPORT
# BUILD UNIQUE TICKER LIST; WITH A CURRENCY COLUMN
# THEN SPLIT DATAFRAMES INTO CURRENCY GROUPS
# AND RUN BELOW ON EACH CURRECNY IN TURN
# WHICH PROBABLY MEANS WRAP THE BELOW INTO FUNCTIONS

# AT THE MOMENT I'M JUST RUNNING THE SCRIPT TWICE, ONCE FOR US STOCKS AND ONCE FOR ZA 
# STOCKS, BUT THIS IS REALLY BIRTTLE BECAUSE I HAVE TO CHANGE THE CURRENCY MANUALLY AND
# CLEAN OUT THE DATALOG BEFORE RUNNING.

#######################################################################################
print("################################")
print("Querying ticker market data...")
smallbegin <- Sys.time()

# This section queries fundamental fields for a list of
# ISIN tickers for a prespecified range of dates.
# Because there appears to be an undocumented limit to number of fields
# queried, we chunk the fields.
# We also chunk the ISINs so that no single query gets too big.

# Need to chunk the ISINs to keep the queries manageable
# https://github.com/Rblp/Rblpapi/issues/207
ticker_iter <- 1
while (ticker_iter <= length(tickers)) {
  startticker <- ticker_iter
  endticker <- min(ticker_iter + 60, length(tickers))
  ticker_iter <- endticker + 1
  
  # Need to chunk the fields
  market_fields_iter <- 1
  while (market_fields_iter <= length(market_fields)) {
    startfield <- market_fields_iter
    endfield <- min(market_fields_iter + 10, length(market_fields))
    print(startfield)
    print(endfield)
    market_fields_iter <- endfield + 1
    
    # Now run the query on chunked fields
    opt <- c(#"periodicitySelection"="MONTHLY", # removed periodicity because we now have compaction so might as well get the exact date something changes 
      "currency"=currency)
    marketdata <- bdh(securities = tickers[startticker:endticker],
                           fields = market_fields[startfield:endfield],
                           start.date = start_date,
                           end.date = as.Date(today),
                           options=opt
    )
    
    # Save  chunked fundamental data queries to log
    marketdata_iter <- 1
    while (marketdata_iter <= length(marketdata)) {
      # Defining the filename parameters for logging
      timestamp <- as.numeric(as.POSIXct(Sys.time()))*10^5
      data_source <- "bloomberg"
      query <- marketdata[[marketdata_iter]]
      name <- names(marketdata[marketdata_iter])
      data_type <- "ticker_market_data" 
      #data_identifier <- paste(the_date, name, sep = "__")
      data_identifier <- gsub("\\s*\\w*$", "", name)
      file_string <- paste(timestamp, data_source, data_type, data_identifier, sep = "__")
      file_string <- paste(file_string, ".csv", sep = "")
      save_file <- file.path(data_directory, file_string)
      # saving the file
      write.table(query, save_file, row.names = FALSE, sep = ",")
      marketdata_iter <- marketdata_iter + 1
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
    opt <- c(#"periodicitySelection"="MONTHLY", # removed periodicity because we now have compaction so might as well get the exact date something changes 
             "currency"=currency)
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
      timestamp <- as.numeric(as.POSIXct(Sys.time()))*10^5
      data_source <- "bloomberg"
      query <- fundamentaldata[[fundamentaldata_iter]]
      name <- names(fundamentaldata[fundamentaldata_iter])
      data_type <- "ticker_fundamental_data" 
      #data_identifier <- paste(the_date, name, sep = "__")
      data_identifier <- gsub("\\s*\\w*$", "", name)
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

}
####################################################################################
# CONVERT BLOOMBERG FIELDS TO INTERNALLY RECOGNISABLE FIELDS
print("")
print("NEXT: Converting Bloomberg fieldnames to consistent fieldnames...")
# Clear environment
rm(list=ls())
# Read in data_pipeline functions
source("R/set_paths.R")
source("R/data_pipeline_functions.R")
# Time the script
begin <- Sys.time()

# Create a dataframe of data log files
print("Scanning datalog...")
data_log <- convert_datalog_to_dataframe()

# METADATA
print("NEXT: Converting metadata fieldnames...")
dataset <- "metadata_array"

print("Reading datalog files...")
# Read in log data
# Filter the log to show just filtered data files
filtered_data_log <- data_log %>% 
  dplyr::filter(ext == "csv") %>% 
  dplyr::filter(grepl(dataset, data_type))
nrow(filtered_data_log)

print("Adding necessary fieldnames...")
if(nrow(filtered_data_log >= 1)) {
  for (i in 1:nrow(filtered_data_log)) {
    metadata <- read_csv(paste(datalog_directory, filtered_data_log[i,]$filename, sep = "/"))
    if (nrow(metadata) >= 1) {
      # Add necessary fieldnames
      metadata <- metadata %>% mutate(market_identifier = TICKER_AND_EXCH_CODE,
                                      fundamental_identifier = ID_ISIN)
      write.table(metadata, 
                  paste(datalog_directory, filtered_data_log[i,]$filename, sep = "/"), 
                  row.names = FALSE, sep = ",")
    }
  }}


#######################################################################################

print("################################")
print("")
end <- Sys.time()
print("############################")
print("Total Script Run Time")
print(end - begin)
print("############################")
print("Script completed") 

