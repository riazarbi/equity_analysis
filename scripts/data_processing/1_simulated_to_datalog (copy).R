# Define paths and load parameters
rm(list=ls())
source("R/set_paths.R")
source("parameters.R")
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

# Timeframe
end_simulation <- Sys.Date()
# 10 year backtest
start_simulation <- end_simulation - 365*10
# Stoock market has been open for 50 years
early_start_simulation <- end_simulation - 365*50
# Daily date sequence
simulation_date_sequence <- seq(early_start_simulation, end_simulation, by="day")
simulation_monthly_date_sequence <- seq(early_start_simulation, end_simulation, by="month")

#################################################################################################
# CREATE SIMPLE 1 TICKER INDEX ###################################################################
#################################################################################################
print("CREATING SIMPLE INDEX ===================================>")
# Create index
index <- "SINGLE_STOCK"
number_constituents <- 1

# Create stock universe
stock_universe <- do.call(paste0, replicate(4, sample(LETTERS, number_constituents, TRUE), FALSE))

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


# Create constituent lists for all the dates
# Parameters: same tickers in every date
constituent_list_dates <- as.character(seq(start_simulation, end_simulation, by="month")) %>% 
  str_replace_all("[^[:alnum:]]", "")
for (constituent_list_date in as.character(constituent_list_dates)){
  constituents <- (data.frame(constituent = stock_universe)) 
  print(knitr::kable(head(constituents)))
  # Filename
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

# Create ticker market data for all the stocks in the metadata file
# Parameters: same prices and volumes for every date
for (stock in metadata$market_identifier) {
  # Create sample dataframe
  market_data <- data.frame(date=simulation_date_sequence) %>%
    mutate(open = 1,
           high = 1,
           low = 1,
           close = 1,
           last = 1,
           volume = 1000,
           shares_in_issue = 100000,
           market_cap = last*shares_in_issue)
  print(knitr::kable(head(market_data)))
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
  
}

# Create ticker fundamental data for all the stocks
# Parameters: same prices and volumes for every date
for (stock in metadata$fundamental_identifier) {
  # Create sample dataframe
  fundamental_data <- data.frame(date=simulation_monthly_date_sequence) %>%
    mutate(earnings = 0.1,
           dividend = 0.05)  
  print(knitr::kable(head(fundamental_data)))
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


#################################################################################################
# CREATE SIMPLE 50 TICKER INDEX ###################################################################
#################################################################################################
print("CREATING SIMPLE INDEX ===================================>")
# Create index
index <- "FIFTY_STOCKS"
number_constituents <- 50

# Create stock universe
stock_universe <- do.call(paste0, replicate(4, sample(LETTERS, number_constituents, TRUE), FALSE))

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


# Create constituent lists for all the dates
# Parameters: same tickers in every date
constituent_list_dates <- as.character(seq(start_simulation, end_simulation, by="month")) %>% 
  str_replace_all("[^[:alnum:]]", "")
for (constituent_list_date in as.character(constituent_list_dates)){
  constituents <- (data.frame(constituent = stock_universe)) 
  print(knitr::kable(head(constituents)))
  # Filename
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

# Create ticker market data for all the stocks in the metadata file
# Parameters: same prices and volumes for every date
for (stock in metadata$market_identifier) {
  # Create sample dataframe
  market_data <- data.frame(date=simulation_date_sequence) %>%
    mutate(open = 1,
           high = 1,
           low = 1,
           close = 1,
           last = 1,
           volume = 1000,
           shares_in_issue = 100000,
           market_cap = last*shares_in_issue)
  print(knitr::kable(head(market_data)))
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
  
}

# Create ticker fundamental data for all the stocks
# Parameters: same prices and volumes for every date
for (stock in metadata$fundamental_identifier) {
  # Create sample dataframe
  fundamental_data <- data.frame(date=simulation_monthly_date_sequence) %>%
    mutate(earnings = 0.1,
           dividend = 0.05)  
  print(knitr::kable(head(fundamental_data)))
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

#################################################################################################
# CREATE 50 TICKER INDEX THAT COMPOUNDS AT THE RISK FREE RATE ###################################
#################################################################################################
print("CREATING SIMPLE INDEX ===================================>")
# Create index
index <- "FIFTY_GROWING"
number_constituents <- 50

# Create stock universe
stock_universe <- do.call(paste0, replicate(4, sample(LETTERS, number_constituents, TRUE), FALSE))

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

# Create constituent lists for all the dates
# Parameters: same tickers in every date
constituent_list_dates <- as.character(seq(start_simulation, end_simulation, by="month")) %>% 
  str_replace_all("[^[:alnum:]]", "")
for (constituent_list_date in as.character(constituent_list_dates)){
  constituents <- (data.frame(constituent = stock_universe)) 
  print(knitr::kable(head(constituents)))
  # Filename
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


compound_period <- seq(from = 1, to=length(simulation_date_sequence))
compound_rate <- stock_growth_rate + 1
price_series <- 1*(compound_rate)^compound_period

# Create ticker market data for all the stocks in the metadata file
# Parameters: same prices and volumes for every date
for (stock in metadata$market_identifier) {
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
  
  print(knitr::kable(head(market_data)))
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
  
}

compound_period <- seq(from = 1, to=length(simulation_monthly_date_sequence))
compound_rate <- (stock_growth_rate+1)**(365.25/4) 
fund_series <- 1*(compound_rate)^compound_period

# Create ticker fundamental data for all the stocks
# Parameters: same prices and volumes for every date
for (stock in metadata$fundamental_identifier) {
  # Create sample dataframe
  fundamental_data <- data.frame(date=simulation_monthly_date_sequence) %>%
    mutate(earnings = fund_series/10,
           dividend = fund_series/20)  
  print(knitr::kable(head(fundamental_data)))
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

#################################################################################################
# CREATE TICKER INDEX WITH SURVIVORSHIP BIAS#####################################################
#################################################################################################
# For survivorship bias, we keep everything the same but make 1.2x more constituents and randomly select each month.

print("CREATING SIMPLE INDEX ===================================>")
# Create index
index <- "FIFTY_SURVIVORS"
number_constituents <- 50*1.2

# Create stock universe
stock_universe <- do.call(paste0, replicate(4, sample(LETTERS, number_constituents, TRUE), FALSE))

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

# Create constituent lists for all the dates
# Parameters: same tickers in every date
constituent_list_dates <- as.character(seq(start_simulation, end_simulation, by="month")) %>% 
  str_replace_all("[^[:alnum:]]", "")
for (constituent_list_date in as.character(constituent_list_dates)){
  constituents <- (data.frame(constituent = sample(stock_universe, 50))) 
  print(knitr::kable(head(constituents)))
  # Filename
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

# Compounding
compound_period <- seq(from = 1, to=length(simulation_date_sequence))
compound_rate <- 1.1^(1/365)
price_series <- 1*(compound_rate)^compound_period

# Create ticker market data for all the stocks in the metadata file
# Parameters: same prices and volumes for every date
for (stock in metadata$market_identifier) {
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
  
  print(knitr::kable(head(market_data)))
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
  
}

compound_period <- seq(from = 1, to=length(simulation_monthly_date_sequence))
compound_rate <- (stock_growth_rate+1)**(365.25/4) 
fund_series <- 1*(compound_rate)^compound_period

# Create ticker fundamental data for all the stocks
# Parameters: same prices and volumes for every date
for (stock in metadata$fundamental_identifier) {
  # Create sample dataframe
  fundamental_data <- data.frame(date=simulation_monthly_date_sequence) %>%
    mutate(earnings = fund_series/10,
           dividend = fund_series/20)  
  print(knitr::kable(head(fundamental_data)))
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
#################################################################################################
# CREATE SHRINKING TICKER INDEX WITH SURVIVORSHIP BIAS#####################################################
#################################################################################################
# For survivorship bias, we keep everything the same but make 1.2x more constituents and randomly select each month.

print("CREATING SIMPLE INDEX ===================================>")
# Create index
index <- "FIFTY_SHRINKING_SURVIVORS"
number_constituents <- 50*1.2

# Create stock universe
stock_universe <- do.call(paste0, replicate(4, sample(LETTERS, number_constituents, TRUE), FALSE))

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

# Create constituent lists for all the dates
# Parameters: same tickers in every date
constituent_list_dates <- as.character(seq(start_simulation, end_simulation, by="month")) %>% 
  str_replace_all("[^[:alnum:]]", "")
for (constituent_list_date in as.character(constituent_list_dates)){
  constituents <- (data.frame(constituent = sample(stock_universe, 50))) 
  print(knitr::kable(head(constituents)))
  # Filename
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

# Compounding
compound_period <- seq(from = 1, to=length(simulation_date_sequence))
compound_rate <- 0.9^(1/365)
price_series <- 1*(compound_rate)^compound_period

price_seq <- seq(from = 1, to = 100, length.out = length(simulation_date_sequence))
# Create ticker market data for all the stocks in the metadata file
# Parameters: same prices and volumes for every date
for (stock in metadata$market_identifier) {
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
  
  
  
  print(knitr::kable(head(market_data)))
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
  
}

compound_period <- seq(from = 1, to=length(simulation_monthly_date_sequence))
compound_rate <- 1.1^(1/365)
fund_series <- 1*(compound_rate)^compound_period

# Create ticker fundamental data for all the stocks
# Parameters: same prices and volumes for every date
for (stock in metadata$fundamental_identifier) {
  # Create sample dataframe
  fundamental_data <- data.frame(date=simulation_monthly_date_sequence) %>%
    mutate(earnings = fund_series/10,
           dividend = fund_series/20)  
  print(knitr::kable(head(fundamental_data)))
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

#################################################################################################
# CREATE TICKER INDEX WITH SURVIVORSHIP BIAS AND RANDOM PRICES###################################
#################################################################################################
# For survivorship bias, we keep everything the same but make 1.2x more constituents and randomly select each month.

print("CREATING SIMPLE INDEX ===================================>")
# Create index
index <- "FIFTY_SURVIVORS_RANDOM_PRICES"

number_constituents <- universe_size * index_coverage

# Create stock universe
stock_universe <- do.call(paste0, replicate(4, sample(LETTERS, universe_size, TRUE), FALSE))

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

# Create constituent lists for all the dates
# Parameters: same tickers in every date
constituent_list_dates <- as.character(seq(start_simulation, end_simulation, by="month")) %>% 
  str_replace_all("[^[:alnum:]]", "")
for (constituent_list_date in as.character(constituent_list_dates)){
  constituents <- (data.frame(constituent = sample(stock_universe, 50))) 
  print(knitr::kable(head(constituents)))
  # Filename
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


for (i in 1:nrow(metadata)) {
# Create ticker market data for all the stocks in the metadata file
# Parameters: same prices and volumes for every date
  stock <- metadata$market_identifier[i]
  #random price series 
  return_seq <- sample(rnorm(length(simulation_date_sequence),1+stock_growth_rate,0.01))
  price_series <- (cumprod(return_seq))
  plot(price_series)
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
  
# Fundamental logs
# Create ticker fundamental data for all the stocks
  stock <- metadata$fundamental_identifier[i]
  # assign a stable multiple
  stable_earnings_yield <- 1/runif(1, min = 1, max = 30)
  stable_payout_ratio <- runif(1, min = 0, max = 1)
  # Create an eanrings yield series
  earnings_seq <- sample(rnorm(length(simulation_monthly_date_sequence),1,0.2))
  earnings_seq <- earnings_seq*stable_earnings_yield
  payout_seq <- sample(rnorm(length(simulation_monthly_date_sequence),1,0.1))
  payout_seq <- payout_seq*stable_payout_ratio
  
  # Create sample dataframe
  fundamental_data <- data.frame(date=simulation_monthly_date_sequence) %>%
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

