##################################
# CONTEXT
# By the time this script runs you have scraped Bloomberg
# and saved the data in the correct format in the datalog folder. 
# The datalog data has been used to update the per-ticker datasets
# the constituent lists and the metadata arrays.
# So the purpose of this script is to use the datasets to build
# an in-memory list of dataframes that can be passed to a backtester.

# This list of dataframes will have the following structure:
# Each dataframe will have a row for each date, and a column for each metric.
# Each NA value will be filled with the last known value
# There should be two more dataframes - 
# The constituent list dataframe has a list of all constituents for each date
# The metadata array should have metadata for each constituent
##################################

## ENVIRONMENT SET UP
print("NEXT: Loading slow-moving data into memory...")
# Clear environment and load common functions
begin <- Sys.time()

## BUILD DATASETS
# Load constituent list
# Filter constituent_list source to only include the parameter specified source
# Filter constituent_list index to only include the parameter specified index
constituent_list <- read_feather(file.path(dataset_directory, 
                                           "constituent_list", 
                                           "constituent_list.feather")) %>% 
  dplyr::filter(grepl(data_source, source))  %>%
  dplyr::filter(grepl(constituent_index, index))

# Issue with naming: we want to use the ticker names as dataframe names. 
# But the tickers all have whitespace.
# So we replace ticker whitepace with _ so they can be legal variable names
constituent_list$ticker <- str_replace_all(constituent_list$ticker," ","_")

# Get a list of all the tickers in the constituent list
tickers <- constituent_list$ticker
tickers <- unique(tickers)

# Load metadata array
# Rename metadata TICKER_AND_EXCH_CODE column to ticker
metadata <- read_feather(file.path(dataset_directory, "metadata_array", "metadata_array.feather")) %>%
  rename(ticker = market_identifier)

# Replace ticker whitepace with _
# so they can be legal variable names (as above)
metadata$ticker <- str_replace_all(metadata$ticker," ","_")
# Filter metadata to just tickers that exist in backtest universe
metadata <- dplyr::filter(metadata,  ticker %in% tickers)
# Cast metadata into flatfile form (col per variable, row per ticker)
metadata <- reshape2::dcast(metadata, ticker + timestamp + source ~ metric)
# Check what tickers didn't make it because they aren't in the metadata file
length(tickers)
length(metadata$ticker)
dropped_tickers <- setdiff(tickers, metadata$ticker)
if(length(dropped_tickers) == 0) {dropped_tickers <- 0}
# Move from constituent-based list to metadata-based list
print(paste("Dropping", 
            dropped_tickers,
            "tickers from constituent list because of insufficient metadata"))
# Remove tickers list
# No longer needed since we are using the metadata array
rm(tickers)

############################################################################
# At this point we have a metadata dataframe of every ticker in
# the dataset. We also have a long constituent membership dataframe
# that tells us which tickers belong to the index.
###########################################################################

# Build list of ticker dataframes
# Create a filename column in the metadata array for marketdata lookups.
metadata$marketdata_filename <- str_replace_all(metadata$ticker," ","_") %>% 
                                   paste(".feather", sep="")
# Infer fundamental filename for each ticker
metadata$fundamental_filename <- str_replace_all(metadata$fundamental_identifier," ","_") %>%
                                   paste(".feather", sep="")

##########################################################################
# NBNBNBNB: Metadata dataset will need to be timestamp filtered
# But I haven't done that here #
##########################################################################

# Load each ticker into a dataframe
ticker_data <- list()
for (i in 1:length(metadata$ticker)){
  ticker <- metadata$ticker[i]
  cat("\r", paste("Loading", ticker, "              "))
  # Define the file paths of the datasets
  marketdata_filename <- metadata$marketdata_filename[i]
  fundamental_filename <- metadata$fundamental_filename[i]
  market_data_filepath <- file.path(dataset_directory, 
                                    "ticker_market_data", marketdata_filename)
  fundamental_filepath <- file.path(dataset_directory, 
                                    "ticker_fundamental_data", fundamental_filename)
  # Load ticker data
  if (file.exists(market_data_filepath) & 
      file.exists(fundamental_filepath)) {
    market_data <- read_feather(market_data_filepath)
    fundamental_data <- read_feather(fundamental_filepath)
    single_ticker_data <- bind_rows(market_data, fundamental_data)
    rm(market_data)
    rm(fundamental_data)
  } else if (file.exists(market_data_filepath)) {
    single_ticker_data <- read_feather(market_data_filepath)
  } else if (file.exists(fundamental_filepath)) {
    single_ticker_data <- read_feather(fundamental_filepath)
  } else {
    print(paste("No data for", ticker))
    next}
  # use only the most recent timestamped value for each
  # metric for each date
  single_ticker_data <- single_ticker_data %>%
    mutate(key=paste(date, metric, source, sep = "|")) %>%
    arrange(desc(key)) %>%
    filter(key != lag(key, default="0")) %>% 
    select(-key, -timestamp)
  # append ticker datasets to list
  ticker_data[[ticker]] <- single_ticker_data
  rm(fundamental_filename)
  rm(fundamental_filepath)
  rm(single_ticker_data)
  rm(market_data_filepath)
  rm(marketdata_filename)
  rm(ticker)
}
rm(i)
# CLEANUP: Drop empty dataframes
ticker_data <- ticker_data[sapply(ticker_data, 
                              function(x) (dim(x)[1]) > 0)]
# TRANSFORM: into FLATFILE form
print("Casting ticker_data into flatfile form.")
ticker_data <- lapply(ticker_data, 
                    function(x)
                      x %>% spread(metric, value))
# Get object size of test data
print(paste("Slow Moving Data object size:", 
            format(object.size(ticker_data), units="auto", standard = "IEC")))

#############################################################################################
# Create a market dataset
print("NEXT: Loading price and volume data into memory...")
price_related_data <- c("date", 
                        "open",
                        "high",
                        "low",
                        "close",
                        "last")

volume_data <- c("date",
                 "volume")

price_data <- lapply(ticker_data, 
       function(x) {
         my.max <- function(x) ifelse( !all(is.na(x)), max(x, na.rm=T), NA)
         my.min <- function(x) ifelse( !all(is.na(x)), min(x, na.rm=T), NA)
         y <- x %>% 
           dplyr::select(one_of(price_related_data))
         date_stash <- y$date
         y <- y %>% 
           select(-date)  %>% 
           mutate(max_price = apply(., 1, my.max)) %>% 
           mutate(min_price = apply(., 1, my.min)) %>%
           #mutate(spread = standard_spread*max_price) %>%
           add_column(date=date_stash)
         z <- x %>% 
           dplyr::select(one_of(volume_data))
         # NOTE: Spread is arbitrary!
         # https://www.bauer.uh.edu/rsusmel/phd/roll1984.pdf
         # Try create a more realistic estimate of spread
         x <- full_join(z, y, by = "date")
})

# Save the time this script completed so that we know it has run. 
ticker_data_load_date <- Sys.time()
# Print how long the script took to run
end <- Sys.time()
print(end - begin)
