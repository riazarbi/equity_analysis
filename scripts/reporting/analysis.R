library(magrittr)
library(tidyverse)
library(lubridate)
# Get date sequence
start_backtest
end_backtest
backtest_sequence <- seq(start_backtest, end_backtest, by="day")
trades <- feather::read_feather(file.path(results_path, "trade_history.feather"))
tickers <- unique(trades$symbol)
price_matrix <- data.frame(matrix(NA, ncol = 0, nrow = length(tickers)))
price_matrix$symbol <- tickers
holdings_matrix <- price_matrix


for (i in 1:length(backtest_sequence)) {
  selected_date <- backtest_sequence[i]
  #print(as.character(selected_date))
daily_holdings <-   trades %>% 
    filter(as.Date(timestamp) <= selected_date) %>%
    group_by(symbol) %>% 
    summarise(sum(quantity)) %>%
  mutate(`sum(quantity)` = symbol) %>%
  dplyr::rename(!!as.character(selected_date) := `sum(quantity)`)

holdings_matrix <- left_join(holdings_matrix, daily_holdings, by="symbol")
}

symbols <- holdings_matrix[,1]
holdings_matrix <- as.data.frame(t(holdings_matrix[,-1]))
colnames(holdings_matrix) <- symbols

# What do I want to report on?

# Annualized return
# Annualized excess return
# Risk adjusted return
# Information ratio
# Max drawdown # Weekly daily monnthly annually
# Turnover
# Ave number of trades
# Expense ratio
# Attribution: absolute return vs loss to trading
# Beta relative to benchmark

# I assume that you will also put in the PBO using the CSCV method and 
# plot the equity curves (excess wealth) against the index on a log scale 
# i.e. ln((returns-RFR)*initial weight).

# Can you confirm that your rebalancing strategy correction re-invests based on the PL.

# Do you have a transaction cost model? 
# At least square-root law: cost \approx spread + \sigma_(daily) *\sqrt(Volume/ADV)

# Summary statistics - 
# - backtest start and end date
# - index
# - number constituents
# - 

# What do we need?
# A benchmark -  define this in parameters

# Where do we save this?
# - in the results
# - a report per stock

# Over and above, we want a PBO report for the entire set of trials

#View(read_feather(file.path(results_path, "trade_history.feather")))
#View(read_feather(file.path(results_path, "submitted_trades.feather")))
#View(read_csv(file.path(results_path, "runtime_log"), col_names=F))
