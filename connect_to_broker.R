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
  
  submit_orders <- function(trades) {
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