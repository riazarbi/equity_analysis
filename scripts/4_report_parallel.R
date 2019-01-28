# Define paths and load parameters
rm(list=ls())
source("R/set_paths.R")
# copy across the parameters file
file.copy(from="scripts/parameters.R", to="results/parameters.R", 
          overwrite = TRUE, recursive = FALSE, 
          copy.mode = TRUE)
# source the parameters file
source("results/parameters.R")

# Time the script
allbegin <- Sys.time()

# Get a list of results subdirectories
results_directories <- list.dirs(results_directory, 
                                  full.names = FALSE)
results_directories <- results_directories[-1]
print(paste("Directory Found:" , results_directories))

# Load slow moving data
source("scripts/trading/load_slow_moving_data.R")

####################################################################################################
library(foreach)
library(doParallel)

# Register a parallel processing cluster
n_cores <- detectCores() - 1
n_cores <- 6
cl <- makeCluster(n_cores, outfile="")
registerDoParallel(cl)

foreach(i=1:length(results_directories), .packages = c("magrittr", "dplyr", "stringr", "feather")) %dopar% {
 
 # SET UP ENVIRONMENT
results_subdirectory <- results_directories[i] 
 begin <- Sys.time()
 # Source scripts
 source("R/set_paths.R")
 source("results/parameters.R")
 # Define get price data function
 get_stock_last_price <- function(ticker, query_date) {
   last <- NA
   result = tryCatch({
     last <- price_data[[ticker]] %>% 
       dplyr::filter(date<=as.Date(query_date)) %>% 
       dplyr::filter(date==max(date)) %>% 
       dplyr::select(last)
     last <- last[[1,1]]
   }, error = function(e) {
   })  
   return(last)
 }
 # Parse the directory name
 dirname_data <- str_split_fixed(results_subdirectory, "__", 3)
 new_data_source <- dirname_data[1]
 new_constituent_index <- dirname_data[2]
 trial <- dirname_data[3]
 print("")
 print(paste("INFO: Processing", trial))
 
 # Read in the logs
 runtime_log <- read_csv(
   file.path(results_directory, results_subdirectory, "runtime_log"))
 trade_history <- read_feather(
   file.path(results_directory, results_subdirectory, "trade_history.feather"))
 transaction_log <- read_feather(
   file.path(results_directory, results_subdirectory, "transaction_log.feather"))
 
 # COMPUTE AND LOOK UP STUFF
 # Compute start and end date of backtest
 start_backtest <- as.Date(min(runtime_log$timestamp))
 end_backtest <- as.Date(max(runtime_log$timestamp))
 backtest_date_sequence <- seq(start_backtest, end_backtest, by = "day")
 
 portfolio_valuation <- as_tibble(backtest_date_sequence) %>% 
   rename(date = value) %>%
   mutate(cash_balance = NA, 
          stock_value = NA,
          portfolio_value = NA)
 
 # CONDUCT PORTFOLIO VALUATION FOR EACH DATE
 # Work through each date, computing cash, stock and portfolio values.
 # I wanted to use apply() for this but for some reason it doesn't want to work
 print("Computing daily portfolio valuations...")
 for (i in seq_along(portfolio_valuation$date)) {
   valuation_date <- portfolio_valuation$date[i]
   message <- paste("INFO: Processing returns data for", valuation_date, "                  ", sep=" ")
   cat("\r", message)
   # Get cash balance
   cash_balance <- sum(transaction_log %>% 
                         dplyr::filter(date(timestamp) <= valuation_date) %>%
                         dplyr::select(amount))
   # Get stock positions for each date
   stock_positions <- trade_history %>% 
     dplyr::filter(date(timestamp) <= valuation_date) %>%
     dplyr::select(symbol, quantity) %>% 
     dplyr::group_by(symbol) %>% 
     dplyr::summarize(sum(quantity)) %>%
     dplyr::rename(position = 'sum(quantity)') %>%
     dplyr::mutate(last_price = NA)
   # Add last prices
   # Not sure why this function wouldn't work in a mutate pipe...
   for (j in seq_along(stock_positions$symbol)) {
     ticker <- stock_positions$symbol[j]
     stock_positions$last_price[j] <- get_stock_last_price(ticker, valuation_date)
     rm(ticker)
   }
   # Compute stock value
   stock_positions <- stock_positions %>%
     dplyr::mutate(value = position * last_price)
   stock_value <- sum(stock_positions$value)
   # Update data frame
   portfolio_valuation$cash_balance[i] <- cash_balance
   portfolio_valuation$stock_value[i] <- stock_value
   portfolio_valuation$portfolio_value[i] <- cash_balance + stock_value
 }
 # end in-frame date walk
 
 # COMPUTE PORTFOLIO STATS
 print("Computing daily portfolio statistics...")
  # dailty stats
 portfolio_stats <- portfolio_valuation %>% 
   mutate(daily_nominal_change = portfolio_value - lag(portfolio_value, 1, order_by = date),
          daily_nominal_change = replace_na(daily_nominal_change,0),
          daily_return = portfolio_value/lag(portfolio_value, 1, order_by = date)-1,
          daily_return = replace_na(daily_return,0),
          daily_risk_free_return = 1+daily_risk_free_rate,
          total_risk_free_return =cumprod(daily_risk_free_return), 
          daily_excess_return = daily_return - daily_risk_free_return+1,
          daily_drawdown = 0) %>%
   mutate(rolling_max = 
            zoo::rollapplyr(portfolio_value, width = 50000, function(x) {max(x)}, fill=NA, partial=TRUE),
          drawdown = rolling_max -portfolio_value,
          drawdown_pct = drawdown / portfolio_value) %>%
   # rolling stats
   mutate(rolling_30day_return = portfolio_value/lag(portfolio_value, 30-1, order_by = date)-1) %>%
   mutate(rolling_30day_sd = 
            zoo::rollapplyr(daily_return, width=30, function(x) {sd(x)}, fill=NA)) %>%
   mutate(rolling_30day_ave_excess_return = 
            zoo::rollapplyr(daily_excess_return, width=30, function(x) {mean(x)}, fill=NA)) %>%
   mutate(rolling_30day_sharpe = rolling_30day_ave_excess_return / rolling_30day_sd) %>%
   mutate(rolling_30_day_max = 
            zoo::rollapplyr(portfolio_value, width = 30, function(x) {max(x)}, fill=NA, partial=TRUE),
          rolling_30_day_drawdown = rolling_30_day_max - portfolio_value,
          rolling_30_day_drawdown_pct = rolling_30_day_drawdown / portfolio_value) %>%    # total return stats
   mutate(total_return = portfolio_value/portfolio_value[1])
 
 print("Writing results to disk...")
 # write the dataframe to the subdirectory
 write_feather(portfolio_stats,
               file.path(results_directory, results_subdirectory, "portfolio_stats.feather"))
 # render the tearsheet
 # knit it
 print(paste("Rendering", results_subdirectory, "tearsheet..."))
 # copy in the template tearsheet
 file.copy("scripts/reporting/tearsheet.Rmd", 
           file.path(results_directory, results_subdirectory, "tearsheet.Rmd"), overwrite = T)
 rmarkdown::render(file.path(results_directory, results_subdirectory, "tearsheet.Rmd"),
                   params = list(results_subdirectory = results_subdirectory), 
                   quiet=TRUE)
 end <- Sys.time()
 print(end - begin)  
 
 } ### end foreach

stopCluster(cl)

####################################################################################################
# Rendering tearsheet and sending some stats to the root directory for CSCV stats
daily_returns <- list()
total_returns <- list()
for (i in 1:length(results_directories)) {
  results_subdirectory <- results_directories[i] 
  # load portfolio stats
  portfolio_stats <- read_feather(file.path(results_directory, results_subdirectory, "portfolio_stats.feather"))
  print(paste("Adding", results_subdirectory, "results to cross-sectional results tables..."))
  # Add the dataframe to the portfolio values list
  trial_daily_returns <- portfolio_stats %>% 
    select(date, daily_return) %>%
    dplyr::rename(!!trial := daily_return)
  daily_returns[[trial]] <- trial_daily_returns
  
  trial_total_returns <- portfolio_stats %>% 
    select(date, total_return) %>%
    dplyr::rename(!!trial := total_return)
  total_returns[[trial]] <-trial_total_returns
  
  }  

# Merge all the portfolio value dataframes into a single dataframe 
total_returns <- Reduce(function(x,y)merge(x,y,by="date"), total_returns)
total_returns$risk_free_return <- portfolio_stats$total_risk_free_return
# write to results root directory
write_feather(total_returns,
              file.path(results_directory, "total_returns.feather"))

# Merge all the portfolio value dataframes into a single dataframe 
daily_returns <- Reduce(function(x,y)merge(x,y,by="date"), daily_returns)
# write to results root directory
write_feather(daily_returns,
          file.path(results_directory, "daily_returns.feather"))

###################################################
allend <- Sys.time()
print(allend - allbegin)
###################################################

# What do I want to report on?

# Annualized return
# Annualized excess return
# Risk adjusted return
# Information ratio
# Max drawdown # Weekly daily monthly annually
# Turnover
# Ave number of trades
# Expense ratio
# Attribution: absolute return vs loss to trading
# Beta relative to benchmark

# Can you confirm that your rebalancing strategy correction re-invests based on the PL.

# Do you have a transaction cost model? 
# At least square-root law: cost \approx spread + \sigma_(daily) *\sqrt(Volume/ADV)

# Summary statistics - 
# - backtest start and end date
# - index
# - number constituents

# What do we need?
# A benchmark -  define this in parameters

# Where do we save this?
# - in the results
# - a report per stock