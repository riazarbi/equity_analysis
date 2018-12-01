# LOAD LIBRARIES ###################################
library(magrittr)

# CONTEXTUALLY DEFINED FUNCTIONS ################################

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
        select(max_price, min_price, volume)
      # I take a random number between min and max,
      # And then I compute bid and offer from the spread.
      midpoint <- runif(1, quote_parameters$min_price, quote_parameters$max_price)
      bid <- midpoint - midpoint*standard_spread#quote_parameters$spread
      offer <- midpoint + midpoint*standard_spread#quote_parameters$spread
      # How much volume is on offer? 
      # I've simply divided the total volume
      # Into trading windows, which are a function of the heartbeat_duration
      # I assume trading day is 8 hours.
      size <- quote_parameters$volume/(8*60*60/heartbeat_duration)
      quote <- c(bid, offer, size)
    }, error = function(e) {
    })
    names(quote) <- c("bid", "offer", "size")
    return(quote)
  }
  
  # Get trade history
  get_trade_history <- function(con) {
    if(file.exists(file.path(results_path, "trade_history.feather"))){
      trade_history <- read_feather(file.path(results_path, "trade_history.feather"))
    } else {
      trade_history <- data.frame(runtime_date - 1,
                                  as.character(as.numeric(as.POSIXct(runtime_date - 1))*10^5),
                                  "dummy",
                                  "dummy",
                                  1,
                                  1,
                                  1,
                                  1,
                                  1)
      x <- c("timestamp", 
             "transaction", 
             "action", 
             "symbol", 
             "quantity", 
             "price", 
             "principal_amount",
             "commission",
             "net_amount")
      colnames(trade_history) <- x
      trade_history <- trade_history[0,]
    }
    return(trade_history)
  }
  
  # Get transaction log
  get_transaction_log <- function(con){
    if(file.exists(file.path(results_path, "transaction_log.feather"))) {
      transaction_log <- read_feather(file.path(results_path, "transaction_log.feather"))
    } else {
      x <- c("timestamp", 
             "transaction", 
             "description", 
             "amount")
      # Seed the portfolio wih an initial amount
      if(portfolio_starting_config == "CASH"){
        transaction_log <- (data.frame(runtime_date - 1,
                                       as.character(as.numeric(as.POSIXct(runtime_date - 1))*10^5),
                                       "INITIAL DEPOSIT",
                                       portfolio_starting_value))
        colnames(transaction_log) <- x
        transaction_log$transaction <- as.character(transaction_log$transaction)
        transaction_log$description <- as.character(transaction_log$description)
        
      } else if(portfolio_starting_config == "STOCK") {
        stop("SORRY HAVENT IMPLEMENTED STARTING CONFIG=STOCK YET")
      }
    }  
    return(transaction_log)
  }
  
  # submit orders
  submit_orders <- function(trades) {
    # b. get quotes from market
    quotes <- as.data.frame(
      t(do.call(cbind, lapply(trades$portfolio_members, get_stock_quote)))) %>%
      mutate(portfolio_members=trades$portfolio_members)
    
    # c. join trades and quotes: match trades
    successful_trades <- left_join(trades, quotes, on="portfolio_members") %>%
      mutate(match = ifelse((order_type=="BUY" & limit >= offer), TRUE, 
                            ifelse(order_type=="SELL" & limit <= bid, TRUE, FALSE ))) %>%
      mutate(price = ifelse((order_type=="BUY"), offer, 
                            ifelse(order_type=="SELL", bid, FALSE ))) %>%
      dplyr::filter(match == TRUE) 
    if(nrow(successful_trades) == 0) {
      print("RESULT: NO TRADES")
      return("0, 0")
    } else {
      
      # create session trade history
      session_trades <- successful_trades %>%
        rename(action = order_type) %>%
        rename(symbol = portfolio_members) %>%
        mutate(quantity = pmin(order_units_int, as.integer(size))) %>%
        mutate(principal_amount = quantity*price) %>%
        mutate(seconds = rep(1:nrow(successful_trades))) %>%
        mutate(timestamp = runtime_date + seconds) %>%
        mutate(transaction = as.character(as.numeric(as.POSIXct(timestamp))*10^5+1)) %>% # I add 1 to the timestamp to stop R autmatically converting to scientific notation
        select(timestamp, transaction, action, symbol, quantity, price, principal_amount)
      
      session_trades <- session_trades %>%
        mutate(commission = abs(principal_amount*commission_rate)) %>%
        mutate(commission = ifelse(commission >= minimum_commission, commission, minimum_commission)) %>%
        mutate(net_amount = principal_amount + commission) 
      
      # create session trade log
      session_trades <- session_trades %>%
        mutate(too_expensive = ifelse((commission < minimum_commission), TRUE, FALSE)) %>%
        dplyr::filter(too_expensive == FALSE)  %>%
        select(-too_expensive)
      # append session trade history to trade history
      trade_history <- bind_rows(trade_history, session_trades) %>%
        dplyr::filter(quantity != 0)
      
      # create session transaction log
      session_transactions <- session_trades %>%
        mutate(description = paste(action, quantity, symbol)) %>%
        mutate(net_amount = -net_amount) %>%
        select(timestamp, transaction, description, net_amount) %>%
        dplyr::rename(amount = net_amount)
      # append session transaction log to transaction log
      transaction_log <- bind_rows(transaction_log, session_transactions) %>%
        dplyr::filter(amount !=0)
      # write to feather files
      write_feather(transaction_log, file.path(results_path, "transaction_log.feather"))
      write_feather(trade_history, file.path(results_path, "trade_history.feather"))
      
      trade_values <- session_trades %>% 
        select(action, net_amount) %>% 
        group_by(action) %>% 
        summarise(sum(net_amount))
      
      # Compute return string
      buyval <- (trade_values %>% dplyr::filter(action == "BUY"))
      if(nrow(buyval) == 1) {
        buyval <- buyval[[1,2]] 
      } else {buyval = 0}
      
      sellval <- (trade_values %>% dplyr::filter(action == "SELL"))
      if(nrow(sellval) == 1) {
        sellval <- sellval[[1,2]] 
      } else {sellval = 0}
      trade_success_val <- paste(buyval, sellval, sep=", ") 
      
      # Print result
      print(paste("RESULT:", nrow(session_trades), "trades were made in this session"))
      # Return result for logging
      return(trade_success_val)
    }
  }
