library(lubridate)
## SET PARAMETERS
## Mode - either LIVE or BACKTEST
run_mode <- "BACKTEST"
# Heartbeat duration: how long between heartbeats (seconds)
heartbeat_duration <- 600

# Set seed for reproducibility
set.seed(42)

## Universe
constituent_index <- "JALSH"
data_source <- "bloomberg"
market_metrics <- c("CUR_MKT_CAP", "PX_LAST")
fundamental_metrics <- c() 

# Timeframe
start_backtest <- "2010-01-01"
end_backtest <- "2010-01-05"
start_backtest <- ymd(start_backtest)
end_backtest <- ymd(end_backtest)
#backtest_dates <- seq(as.Date(start_backtest), as.Date(end_backtest), by="day")

# Maybe delete everything after this... depends on eventual architecture
#periodicity <- "month"
# And the splits
#train_test_split <- 0.8
###################################################################

# PROCESS THE PARAMETERS
# Combine market and fundamental metrics
metrics <- c(market_metrics, fundamental_metrics)
# But if no metrics are selected, just take all possible fields
if (length(metrics)==0){
  metrics <- c(fundamental_fields, market_fields)
}

# Create training and testing date ranges
#dates <- seq(as.Date(start_backtest), as.Date(end_backtest), by=periodicity)
#date_split <- dates[as.integer(length(dates)*train_test_split)]
#train <- dates[dates < date_split]
#test <- dates[dates >= date_split]
#rm(dates)
# Check that test and train don't overlap
# We expect this to be 0
#intersect(test, train)