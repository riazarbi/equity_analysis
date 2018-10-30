# Get or create trade history
if(file.exists("trade_history.feather")){
    read_feather("trade_history.feather")
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

# Get or create transaction log
if(file.exists("transaction_log.feather")){
    read_feather("transaction_log.feather")
} else {
    transaction_log <- data.frame(matrix(ncol = 5, nrow = 0))
    x <- c("timestamp", 
           "transaction", 
           "description", 
           "amount", 
           "balance")
    colnames(transaction_log) <- x
}

rm(x)

# Enter the initial values
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