# Define parameters
# Set seed for reproducibility
set.seed(42)

###############################################################
# REPORTING PARAMETERS
annual_risk_free_rate <- 0.04
daily_risk_free_rate <- (1+annual_risk_free_rate)**(1/365.25)-1

##############################################################
# SIMULATED DATA PARAMETERS - ONLY IMPORTANT IF CREATING SIMULATED DATASET
# Index name
index <- "RISK_FREE_RANDOMS"
# Universe size
universe_size <- 200
# Average Daily Stock Growth Rate
annual_stock_growth_rate <- annual_risk_free_rate + 0.04
stock_growth_rate <- (1 + annual_stock_growth_rate)**(1/365.25)-1
# Index Coverage of Universe (eg index of 10 stocks on universe of 100 stocks is 1/10)
index_coverage <- 1/4
# Timeframe
end_simulation <- Sys.Date()
# start backtest
start_simulation <- end_simulation - 365*50
# standard deviation of daily price jumps (percentage)
price_jump_stddev <- 0.02
# standard deviation of quarterly variation in earnings yield from long run mean (percentage)
earnings_yield_stddev <- 0.2
# standard deviation of quarterly variation in payout ratio from long run mean (percentage)
payout_ratio_stddev <- 0.1

##################################################################
# INDEX SPECIFIC TRADING PARAMETERS

# Bloomberg
#constituent_index <- "JALSH"
#data_source <- "bloomberg"
#price_related_data <- c("date", "PX_OPEN", "PX_HIGH", "PX_LOW", "PX_LAST")
#last_price_field <- c('date', "TOT_RETURN_INDEX_GROSS_DVDS") 
#volume_data <- c("date", "VOLUME")
#market_metrics <- c("CUR_MKT_CAP")
#fundamental_metrics <- c() 

# Simulated
constituent_index <- "RISK_FREE_RANDOMS"
data_source <- "simulated"
price_related_data <- c("date", "open", "high", "low", "close", "last")
last_price_field <- c('date', 'last')
volume_data <- c("date", "volume")
market_metrics <- c("market_cap")
fundamental_metrics <- c()

# Specify if fundamental data needs lag adjustment
# we use k-means clustering to tag the metrics for adjustment or not.
# if only 1 cluster, all will be lagged. If more then 1 cluster, only the slowest-updated metrics will be lagged.
fundamental_data_lag_adjustment <- 0 # days. How many do you want to shift your fundamental data back? 
# if there is a lag adjustment, then is the fundamental data heterogenous?
fundamental_data_metric_types <- 1 # auto will auto-detect number of clusters. Override with a number if you want.

##################################################################
# GENERAL TRADING PARAMETERS

# Mode - either LIVE or BACKTEST
run_mode <- "BACKTEST"
# Heartbeat duration: how long between heartbeats (seconds)
heartbeat_duration <- 60*60*24*5 # every week
# rebalancing period: how long between portfolio rebalancing (seconds)
rebalancing_periodicity <- 60*60*24*30 # every month

# Timeframe
start_backtest <- "2014-01-01" # inclusive
end_backtest <- "2015-01-01" # not inclusive

# Portfolio characteristics
portfolio_starting_configs <- c("CASH", "STOCK")
portfolio_starting_config <- "CASH"
portfolio_starting_value <- 1000000
cash_buffer_percentage <- 0.02 # decimal form
#cash_yearly_compounding_rate <- 0.05 # decimal form not implemented yet

# Trading characteristics
commission_rate <- 0
minimum_commission <- 0
standard_spread <- 0.002
soft_rebalancing_constraint <- 0.02 # don't trade if this close to perfect balance

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
print(paste("Parameters file specifies", run_mode, "mode."))

# Build a vector of the necessary metrics
metrics <- c(market_metrics, 
             fundamental_metrics, 
             price_related_data, 
             last_price_field, 
             volume_data) %>% 
                          unique(.)
