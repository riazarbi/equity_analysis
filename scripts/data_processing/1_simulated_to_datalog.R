# Define paths and load parameters
rm(list=ls())
source("R/set_paths.R")
source("scripts/parameters.R")
source("R/data_pipeline_functions.R")

library(lubridate)
library(feather)
library(magrittr)
library(dplyr)
library(stringr)

# Define directory paths
data_directory <- file.path(working_directory, "data/datalog")
# Make sure data_directory exists, create it if it does not
dir.create(data_directory, showWarnings = FALSE)

# Daily date sequence
simulation_date_sequence <- seq(start_simulation, end_simulation, by="day")
simulation_quarterly_date_sequence <- seq(start_simulation, end_simulation, by="quarter")

#################################################################################################
# CREATE TICKER INDEX 
#################################################################################################
# For survivorship bias, we compute the n larged cap stocks each quarter and rebalance.
# Index is named in parameters file

print("CREATING INDEX ===================================>")
number_constituents <- universe_size * index_coverage

# Create stock universe
stock_universe <- do.call(paste0, replicate(5, sample(LETTERS, universe_size, TRUE), FALSE))

# Create metadata file
# Parameters: just market identifier and fundamental identifier fields, which are identical
metadata <- (data.frame(market_identifier = stock_universe, 
                        fundamental_identifier = stock_universe,
                        sector = "generic"))
# Filename
print(paste("Saving", index, "ticker metadata to datalog."))
timestamp <- as.numeric(as.POSIXct(Sys.time()))*10^5
data_source <- "simulated"
data_type <- "metadata_array"
data_identifier <- index
file_string <- paste(timestamp, data_source, data_type, data_identifier, sep = "__")
file_string <- paste(file_string, ".csv", sep = "")
save_file <- file.path(datalog_directory, file_string)
print(save_file)
# save file
write.table(metadata, save_file, row.names = FALSE, sep = ",")

# Load each ticker into a dataframe
market_caps <- list()

for (i in 1:nrow(metadata)) {
# Create ticker market data for all the stocks in the metadata file
# Parameters: same prices and volumes for every date
  stock <- metadata$market_identifier[i]
  #random price series 
  return_seq <- sample(rnorm(length(simulation_date_sequence),1+stock_growth_rate, price_jump_stddev))
  price_series <- (cumprod(return_seq))
  #plot(price_series)
  # Create sample dataframe
  market_data <- data.frame(date=simulation_date_sequence) %>%
    mutate(open = price_series,
           high = price_series,
           low = price_series,
           close = price_series,
           last = price_series,
           volume = 1000,
           shares_in_issue = 100000,
           market_cap = last*shares_in_issue)

  print(knitr::kable(tail(market_data)))
  # Filename
  print(paste("Saving", stock, "market data to datalog."))
  timestamp <- as.numeric(as.POSIXct(Sys.time()))*10^5
  data_source <- "simulated"
  data_type <- "ticker_market_data" 
  data_identifier <- stock
  file_string <- paste(timestamp, data_source, data_type, data_identifier, sep = "__")
  file_string <- paste(file_string, ".csv", sep = "")
  save_file <- file.path(datalog_directory, file_string)
  print(save_file)
  # Save file
  write.table(market_data, save_file, row.names = FALSE, sep = ",")
  
  # Add market cap vector to list for calcluating index constituents later
  market_caps[[as.character(stock)]] <- (market_data %>% 
    select(date, market_cap) %>% dplyr::rename(!!as.character(stock) := market_cap))
  

# Fundamental logs
# Create ticker fundamental data for all the stocks
  stock <- metadata$fundamental_identifier[i]
  # assign a stable multiple
  stable_earnings_yield <- 1/runif(1, min = 1, max = 30)
  stable_payout_ratio <- runif(1, min = 0, max = 1)
  # Create an eanrings yield series
  earnings_seq <- sample(rnorm(length(simulation_quarterly_date_sequence),1,earnings_yield_stddev))
  earnings_seq <- earnings_seq*stable_earnings_yield
  payout_seq <- sample(rnorm(length(simulation_quarterly_date_sequence),1,payout_ratio_stddev))
  payout_seq <- payout_seq*stable_payout_ratio
  
  # Create sample dataframe
  fundamental_data <- data.frame(date=simulation_quarterly_date_sequence) %>%
    mutate(earnings_yield = earnings_seq,
           payout_ratio = payout_seq)  
  fundamental_data <- left_join(fundamental_data, market_data, by = "date") %>%
    mutate(earnings = last*earnings_yield,
           dividend = earnings*payout_ratio) %>%
    select(date, earnings, dividend)
  print(knitr::kable(tail(fundamental_data)))
  # Filename
  print(paste("Saving", stock, "fundamental data to datalog."))
  timestamp <- as.numeric(as.POSIXct(Sys.time()))*10^5
  data_source <- "simulated"
  data_type <- "ticker_fundamental_data" 
  data_identifier <- stock
  file_string <- paste(timestamp, data_source, data_type, data_identifier, sep = "__")
  file_string <- paste(file_string, ".csv", sep = "")
  save_file <- file.path(datalog_directory, file_string)
  print(save_file)
  # Save file
  write.table(fundamental_data, save_file, row.names = FALSE, sep = ",")
}

market_caps <- (Reduce(function(x,y)merge(x,y,by="date"), market_caps))
# Create constituent lists for all the dates
# Parameters: same tickers in every date
constituent_list_dates <- seq(start_simulation, end_simulation, by="month")

for (i in 1:length(constituent_list_dates)){
  # Get the date
  constituent_list_date <- constituent_list_dates[i]
  print(constituent_list_date)
  # Get the relevant row from market_caps
  # And transform into a datafram
  ranks <- (t(market_caps %>% 
                         dplyr::filter(date == constituent_list_date)))[-1,]
  ranks <- as.data.frame(ranks)
  colnames(ranks) <- c("market_cap")
  ranks[,1] <- as.numeric(levels(ranks[,1]))[ranks[,1]]
  # Rank the market caps
  ranks <- ranks %>% 
    mutate(rank = rank(-market_cap)) %>%
    mutate(constituents = rownames(ranks)) %>%
    dplyr::filter(rank <= number_constituents) %>%
    arrange(rank)
  
  # Select the largest cap stocks in the universe
  constituents <- ranks %>% 
  dplyr::filter(rank <= number_constituents) %>%
  select(constituents)
  print(knitr::kable(head(constituents)))
  
    # Filename
  constituent_list_date <- as.character(constituent_list_date) %>% 
      str_replace_all("[^[:alnum:]]", "")
  print(paste("Saving", index, constituent_list_date, "constituent data to datalog."))
  timestamp <- as.numeric(as.POSIXct(Sys.time()))*10^5
  data_source <- "simulated"
  data_type <- "constituent_list"
  data_identifier <- paste(constituent_list_date, index, sep = "_")
  file_string <- paste(timestamp, data_source, data_type, data_identifier, sep = "__")
  file_string <- paste(file_string, ".csv", sep = "")
  save_file <- file.path(datalog_directory, file_string)
  print(save_file)
  # save file
  write.table(constituents, save_file, row.names = FALSE, sep = ",")
}