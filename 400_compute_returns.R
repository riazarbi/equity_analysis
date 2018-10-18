source("shared_functions.R")
options(readr.num_columns = 0)
# Set parameters
data_source <- "self"
datatype <- "backtest" 
algorithm_name <- "cap_weighted"

data_log <- convert_datalog_to_dataframe()

data_log <- dplyr::filter(data_log, grepl(data_source,source))
data_log <- dplyr::filter(data_log, grepl(datatype, data_type))
data_log <- dplyr::filter(data_log, grepl(algorithm_name, data_label))

# These are the portfolio weight instances. 
# You'll have to select one to score.
unique(data_log$timestamp)               
backtest_instance <- max(data_log$timestamp)
data_log <- dplyr::filter(data_log, grepl(backtest_instance, timestamp))

# Load the algorithm file
algo_file <- dplyr::filter(data_log, grepl("algorithm", data_label))$filename
source(file.path(datalog_directory, algo_file))

# Load the weights
weights_file <- dplyr::filter(data_log, grepl("weights", data_label))$filename
weights <- read_feather(file.path(datalog_directory, weights_file))

# Not quite sure if I should score train or backtest, but will stick with
# train because that's what I've computed weights for.
dates <- seq(as.Date(min(train)), 
             as.Date(max(train)), by="day")


# Build a daily total return array. All stocks.
all_market_data_files <- list.files(file.path(dataset_directory,"ticker_market_data"))
rm(all_returns)
for (f in all_market_data_files) {
  print(f)
  data <- read_csv(file.path(dataset_directory,"ticker_market_data", f))
  # Select the metrics to be used 
  returns <- data %>% dplyr::filter( 
    metric == "PX_LAST")
  
  # use only the most recent timestamped value for each
  # metric for each date
  returns <- data %>%
    mutate(key=paste(date, source, metric, sep = "|")) %>%
    arrange(desc(key)) %>%
    filter(key != lag(key, default="0")) %>% 
    select(-key, -timestamp)

  ###########################
  # THIS IS WHERE YOU COMPUTE TOTAL RETURNS ####
  # I'M STUCK WITH JUST PX_LAST
  ##########################
  if (nrow(returns) == 0) {
    print(paste("No data for",ticker_string ))
  } else {
    # Insert ticker name
    ticker_string_parts <- str_split_fixed(sub(pattern = "(.*)\\..*$", replacement = "\\1", basename(f)), "_" ,3)
    ticker_string <- paste(ticker_string_parts[1], ticker_string_parts[2], sep="_")
    returns$ticker <- ticker_string
    returns <- returns %>% select(date, ticker, value)
    if (!exists("all_returns")) {
      all_returns <- returns
      rm(returns)
    } else {
      all_returns <- bind_rows(all_returns, returns)
      rm(returns)
   }
}
}

head(all_returns)

library(reshape2)
View(all_returns %>% dcast(ticker ~ date, value.var='value'))

dcast(data,student~test,value.var='score')
