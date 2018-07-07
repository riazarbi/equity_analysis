# BACKTESTER
source("011_utils.R")
# This section should procedurally run code that tells us what the universe looks like.
# Maybe this is EDA?
# need the following functions:
#

# This section is where we set parameters for the backtest
index <- "TOP_40"
source <- "bloomberg"
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