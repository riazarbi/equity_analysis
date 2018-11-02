# Load libraries
library(lubridate)

# Set seed for reproducibility
set.seed(42)

#################################################################
# SET PARAMETERS
# Mode - either LIVE or BACKTEST
run_mode <- "BACKTEST"
# Heartbeat duration: how long between heartbeats (seconds)
heartbeat_duration <- 1200

# Universe
constituent_index <- "SINGLE_STOCK"
data_source <- "simulated"
market_metrics <- c()
fundamental_metrics <- c() 

# Timeframe
start_backtest <- "2017-01-05"
end_backtest <- "2017-01-10"

# Portfolio characteristics
portfolio_starting_configs <- c("CASH", "STOCK")
portfolio_starting_config <- "CASH"
portfolio_starting_value <- 1000
cash_buffer_percentage <- 0

# Trading characteristics
commission_rate <- 0
minimum_commission <- 0
standard_spread <- 0

###################################################################
# DON'T MODIFY THINGS BELOW THIS LINE #############################
###################################################################

# CREATE SAVE DIRECTORY
portfolios_directory <- file.path(working_directory, "portfolios")
dir.create(portfolios_directory, showWarnings = FALSE)

timestamp <- as.numeric(as.POSIXct(Sys.time()))*10^5
data_type <- "backtest_data"
portfolio_directory <- paste(data_source, constituent_index, data_type, sep = "__")
portfolio_directory <- file.path(portfolios_directory, portfolio_directory)
dir.create(portfolio_directory, showWarnings = FALSE)
###################################################################

# PROCESS THE PARAMETERS
allowed_modes <- c("LIVE", "BACKTEST")
# Combine market and fundamental metrics
metrics <- c(market_metrics, fundamental_metrics)
# But if no metrics are selected, just take all possible fields
#if (length(metrics)==0){
#  metrics <- c(fundamental_fields, market_fields)
#}
# Convert date strings to date objects
start_backtest <- ymd(start_backtest)
end_backtest <- ymd(end_backtest)

