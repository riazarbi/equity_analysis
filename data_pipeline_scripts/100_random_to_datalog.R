set.seed(42)
library(lubridate)
library(feather)
library(magrittr)
library(dplyr)
# Timeframe
start_simulation <- "2000-01-01"
end_simulation <- Sys.Date()
simulation_date_sequence <- seq(start_simulation, end_simulation, by="day")
long_duration_date_sequence <- seq(ymd("1920-01-01"), ymd("2070-01-01"), by="day")
start_simulation <- ymd(start_simulation)
end_simulation <- ymd(end_simulation)

# Create index
index <- paste("RANDOM", do.call(paste0, replicate(5, sample(LETTERS, 1, TRUE), FALSE)), sep="_")
number_constituents <- 50000

# Specify fields of interest
fields <-c("open", "high", "low", "close", "last", "volume", "shares_in_issue")

# Simulate the life of a stock universe
stock_universe <- do.call(paste0, replicate(4, sample(LETTERS, number_constituents, TRUE), FALSE))
stock_listing_date <- sample(long_duration_date_sequence, number_constituents)
stock_lifetimes <- data.frame(stock_universe, stock_listing_date) 
stock_lifetimes$stock_universe <- as.character(stock_lifetimes$stock_universe)

row(stock_lifetimes)
for (i in seq_along(stock_lifetimes$stock_universe)){
  listing_date <- stock_lifetimes[i,2]
  stock_ticker <- stock_lifetimes[i,1]
  # here we simulate the meandering of the fields
  # I'm thinking given open, all others are random jumps.
  # Except of course vol and shares. 
  # Shares never change.
  # Vol is poisson, with lambda as some small percentage of Shares in issue.
  }


constituent_list_simulation <- seq(start_simulation, end_simulation, by="month")
for (i in seq_along(constituent_list_simulation)){
  # Define the date
  the_date <- constituent_list_simulation[i]
  # Obtain a constituent list
  constituent_list <- index_constituents %>%
    mutate(in_index = 
             (constituent_list_simulation[i] >= index_entry_date & 
                constituent_list_simulation[i] <= index_exit_date)) %>%
    dplyr::filter(in_index == TRUE) %>%
    select(constituents, index_entry_date, index_exit_date)
  nrow(constituent_list)
}











# Filename
timestamp <- as.numeric(as.POSIXct(Sys.time()))*10^5
data_source <- "random_generator"
data_type <- "ticker_market_data" 
data_identifier <- name
file_string <- paste(timestamp, data_source, data_type, data_identifier, sep = "__")
file_string <- paste(file_string, ".csv", sep = "")
save_file <- file.path(data_directory, file_string)
# saving the file
write.table(simulation, save_file, row.names = FALSE, sep = ",")


#
# Simulate stock price data with random walk
#
n = 1000 # Walk n steps
p = .5 # Probability of moving left
trials = 100 # Num times to repeate sim
# Run simulation
rand_walk = replicate(trials, cumsum(sample(c(-1,1), size=n, replace=TRUE, prob=c(p,1-p))))

#
# Prepare data for plotting
#
all_walks = melt(rand_walk)
avg_walk = cbind.data.frame(
  'x' = seq(from=1, to=n, by=1),
  'y' = apply(rand_walk, 1, mean)
)