# Define parameters
# Set seed for reproducibility
set.seed(42)

###############################################################
# REPORTING PARAMETERS
annual_risk_free_rate <- 0.04
daily_risk_free_rate <- (1+annual_risk_free_rate)**(1/365.25)-1

##############################################################
# SIMULATED DATA PARAMETERS
# Index name
index <- "RISK_FREE_GROWERS"
# Universe size
universe_size <- 200
# Average Daily Stock Growth Rate
annual_stock_growth_rate <- annual_risk_free_rate
stock_growth_rate <- (1+annual_stock_growth_rate)**(1/365.25)-1
# Index Coverage of Universe (eg index of 10 stocks on universe of 100 stocks is 1/10)
index_coverage <- 1/4
# Timeframe
end_simulation <- Sys.Date()
# start backtest
start_simulation <- end_simulation - 365*10
# standard deviation of daily price jumps (percentage)
price_jump_stddev <- 0.01
# standard deviation of quarterly variation in earnings yield from long run mean (percentage)
earnings_yield_stddev <- 0.2
# standard deviation of quarterly variation in payout ratio from long run mean (percentage)
payout_ratio_stddev <- 0.1

#################################################################
# TRADING PARAMETERS

# Mode - either LIVE or BACKTEST
run_mode <- "BACKTEST"
# Heartbeat duration: how long between heartbeats (seconds)
heartbeat_duration <- 60*60*23.5

# Universe
constituent_index <- index
data_source <- "simulated"
market_metrics <- c()
fundamental_metrics <- c() 

# Timeframe
start_backtest <- "2012-01-01" # inclusive
end_backtest <- "2018-01-01" # not inclusive

# Portfolio characteristics
portfolio_starting_configs <- c("CASH", "STOCK")
portfolio_starting_config <- "CASH"
portfolio_starting_value <- 1000000
cash_buffer_percentage <- 0.05 # decimal form
#cash_yearly_compounding_rate <- 0.05 # decimal form not implemented yet

# Trading characteristics
commission_rate <- 0
minimum_commission <- 0
standard_spread <- 0

##############################################################
# Process parameters
# * DON'T MODIFY * #
allowed_modes <- c("LIVE", "BACKTEST")
# Convert to date format
library(lubridate)
start_backtest <- ymd(start_backtest)
end_backtest <- ymd(end_backtest)

# Check parameter health
if(!(run_mode %in% allowed_modes)) {
  stop("Set a correct mode in parameters.R: Either LIVE or BACKTEST.")
} 
print(paste("Running in", run_mode, "mode."))

