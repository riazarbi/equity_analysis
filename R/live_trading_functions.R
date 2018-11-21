# LOAD LIBRARIES ###################################
library(magrittr)

# CONTEXTUALLY DEFINED FUNCTIONS ################################
stop("ERROR: Haven't implemented live trading yet.")

# connect to broker
connect_to_broker <- function(){
}

# get stock price
get_stock_quote <- function(ticker) {
  return(quote)
}

# Get trade history
get_trade_history <- function(con) {
  return(trade_history)
}

# Get transaction log
get_transaction_log <- function(con){
  return(transaction_log)
}

# submit orders
submit_orders <- function(trades) {
    return(trade_success_val)
  }
