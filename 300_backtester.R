# BACKTESTER
source("011_utils.R")
# This section should procedurally run code that tells us what the universe looks like.
# Maybe this is EDA?
# need the following functions:
#

# This section is where we set parameters for the backtest
constituent_index <- "TOP40"
data_source <- "bloomberg"
market_metrics <- c("PX_LAST")
fundamental_metrics <- c("BS_CUR_LIAB") 

start_backtest <- "2010-01-01"
end_backtest <- "2015-12-31"
periodicity <- "month"
train_test_split <- 0.8

# Create training and testing date ranges
dates <- seq(as.Date(start_backtest), as.Date(end_backtest), by=periodicity)
date_split <- dates[as.integer(length(dates)*train_test_split)]
train <- dates[dates < date_split]
test <- dates[dates >= date_split]
# Check that test and train don't overlap
intersect(test, train)

# Load datasets to memory

# Load consituent list
constituent_list <- read_csv(
  file.path(dataset_directory, "constituent_list", "constituent_list.csv"), 
  col_types = cols(
    .default = "c",
    date = col_date(format = "%Y%m%d")
  )
)

# Filter constituent_list source
constituent_list <- dplyr::filter(constituent_list, grepl(data_source, source))
# Filter constituent_list index
constituent_list <- dplyr::filter(constituent_list, grepl(constituent_index, index))
# Filter constituent_list dates
max(constituent_list$date)
min(constituent_list$date)
constituent_list <- dplyr::filter(constituent_list, date >= start_backtest)
constituent_list <- dplyr::filter(constituent_list, date <= end_backtest)
max(constituent_list$date)
end_backtest
min(constituent_list$date)
start_backtest

# Step through the backtest dates
##############################################
# this is wherew a for loop should start, scoring each date.
backtest_date <- train[24]

# Get most recent ticker list for a particular date
# This code removes all dates from master constituent list
# That are greater than backtest date.
# Then selects only those rows which are the max of the subsetted 
# dataframe. In plain language, it gets the most recent 
# ticker list for a given backtest date.
ticker_list <- (constituent_list %>%
       filter( date <=backtest_date) %>%
       filter( date == max(date)))

head(ticker_list)

ticker <- ticker_list$ticker[1]

# IDEA: SET PARAMETERS AND FUNCTION, AND THEN RUN BACKTEST AND GET A BACKTEST OBJECT BACK. 
# BACKTEST OBJECT SHOULD HAVE A WHOLE LOT OF STUFF, NOT JUST THE 
# RETURN. 
# WHAT?
# RISK STUFF
# RETURN STUFF
