# This code chunk
# 1. Connects to a broker (dummy if backtest)
# 2. 
# 3. Defines the following functions contextually,
# depending on what run_mode flag is set.
# get_trade_history
# get_transaction_log
# submit_orders
  
if(run_mode=="BACKTEST") {
  
  # connect to broker
  connect_to_broker <- function(){
  print("INFO: Connecting to dummy broker.")
  con <- "simulated_broker"
  print("CONNECTION SUCCESSFUL.")
  return(con)
  }
  
# get stock price
  get_stock_quote <- function(ticker) {
    quote <- c(NA, NA, NA)
    result = tryCatch({
    quote_parameters <- price_data[[ticker]] %>% 
            filter(date<=as.Date(runtime_date)) %>% 
            filter(date==max(date)) %>% 
            select(max_price, min_price, spread, PX_VOLUME)
    # I take a random number between min and max,
    # And then I compute bid and offer from the spread.
    midpoint <- runif(1, quote_parameters$min_price, quote_parameters$max_price)
    bid <- midpoint - quote_parameters$spread
    offer <- midpoint + quote_parameters$spread
    # How much volume is on offer? 
    # I've simply divided the total volume
    # Into trading windows, which are a function of the heartbeat_duration
    # I assume trading day is 8 hours.
    size <- quote_parameters$PX_VOLUME/(8*60*60/heartbeat_duration)
    quote <- c(bid, offer, size)
    }, error = function(e) {
    })
    names(quote) <- c("bid", "offer", "size")
    return(quote)
  }

  # Get trade history
  get_trade_history <- function(con) {
    if(file.exists("trade_history.feather")){
      trade_history <- read_feather("trade_history.feather")
    } else {
      trade_history <- data.frame(matrix(ncol = 9, nrow = 0))
      x <- c("timestamp", 
             "transaction", 
             "action", 
             "symbol", 
             "quantity", 
             "price", 
             "principal amount",
             "commission",
             "net amount")
      colnames(trade_history) <- x
    }
    return(trade_history)
  }
  
  # Get transaction log
  get_transaction_log <- function(con){
    if(file.exists("transaction_log.feather")) {
    transaction_log <- read_feather("transaction_log.feather")
  } else {
    transaction_log <- data.frame(matrix(ncol = 5, nrow = 0))
    x <- c("timestamp", 
           "transaction", 
           "description", 
           "amount", 
           "balance")
    colnames(transaction_log) <- x
    # Seed the portfolio wih an initial amount
    if(portfolio_starting_config == "CASH"){
      first_row <- c(as.numeric(as.POSIXct(Sys.time()))*10^5,
                     NA,
                     "INITIAL DEPOSIT",
                     portfolio_starting_value,
                     portfolio_starting_value)
      transaction_log[nrow(transaction_log)+1,] <- first_row
    } else if(portfolio_starting_config == "STOCK") {
      stop("SORRY HAVENT IMPLEMENTED STARTING CONFIG=STOCK YET")
    }
  transaction_log$amount <- as.numeric(transaction_log$amount)
  transaction_log$balance <- as.numeric(transaction_log$balance)
  }  
    return(transaction_log)
  }
  
  # submit orders
  submit_orders <- function(trades) {
    # b. get quotes
    quotes <- as.data.frame(
      t(do.call(cbind, lapply(trades$portfolio_members, get_stock_quote)))) %>%
      mutate(portfolio_members=trades$portfolio_members)
    
    # c. join trades and quotes
    trades <- left_join(trades, quotes, on="portfolio_members")
    # b. only allocate the min of amount or available

    # c. create transaction logs and trade history
    # d. append to feather files
    # e. write to feather files
    # read trades
    

    # Append trades to logs
    transaction_log <- get_transaction_log(con)
    trade_history <- get_trade_history(con)
    
    # do some appending here
    
    # save logs
    save_feather(transaction_log, "transaction_log.feather")
    save_feather(trade_history, "trade_history.feather")
    print("SUCCESS")
  }

} else if(run_mode=="LIVE"){
  print("ERROR: Haven't implemented live trading yet.")
} else {
  print("ERROR: Something went wrong - malformed run_mode")
  }