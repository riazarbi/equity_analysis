# LOAD LIBRARIES ###################################
library(magrittr)


# GENERIC FUNCTIONS ################################
get_runtime_dataset <- function(execution_date, constituent_list, ticker_data) {
  # Expects: 
  #   execution_date
  #   constituent_list
  #   ticker_data
  # Returns:
  #   runtime_ticker_data
  
  # Get most recent constituent members for the execution date
  print(paste("ACTION: Getting", constituent_index, "members at", execution_date, "according to", data_source))
  index_members <- (constituent_list %>%
                      filter(date <= execution_date) %>%
                      filter(date == max(date)))
  
  # SURVIVORSHIP BIAS
  print("ACTION: Filtering for SURVIVORSHIP BIAS...") 
  # FILTER: to only include time-appropriate index members
  runtime_ticker_data <- ticker_data[names(ticker_data) %in% index_members$ticker]
  # CHECK: that constituent members and algo members are identical
  print(paste("CHECKING FOR ISSUES:", (setdiff((index_members$ticker), names(runtime_ticker_data)))))
  print(paste("CHECKING FOR ISSUES:", (setdiff(names(runtime_ticker_data), (index_members$ticker)))))
  # CHECK: what has been dropped from master dataset?
  print(paste("DROPPED:", 
              length((setdiff(names(ticker_data), names(runtime_ticker_data)))),
              "/",
              length(ticker_data),
              "tickers from ticker_data because they are not in the",
              execution_date,
              "constituent list."))
  # LOOKAHEAD BIAS
  print("ACTION: Filtering for LOOKAHEAD BIAS...") 
  # FILTER: any row entries after execution date 
  runtime_ticker_data <- lapply(runtime_ticker_data, 
                                function(x) 
                                  (filter(x, date <= execution_date)))
  # CLEANUP: Drop empty dataframes
  runtime_ticker_data <- runtime_ticker_data[sapply(runtime_ticker_data, 
                                                    function(x) (dim(x)[1]) > 0)]
  # CHECK: verify no look-ahead data
  print(paste("CHECK: execution date is:", execution_date))
  print(paste("CHECK: latest date in runtime_ticker_data is:", 
              zoo::as.Date(max(unlist(lapply(runtime_ticker_data, function(x) max(x$date)))))))
  # CLEANUP: drop any missing columns
  print("ACTION: Dropping ticker columns with no data...")
  not_all_na <- function(x) {!all(is.na(x))}
  runtime_ticker_data <- lapply(runtime_ticker_data, 
                                function(x) x %>% select_if(not_all_na))
  # FILL: fill NA values with last known value
  print("ACTION: Backfilling NA values with last known value")
  runtime_ticker_data <- lapply(runtime_ticker_data, 
                                function(x) x %>% arrange(date)
                                %>% fill(names(x))
                                %>% arrange(desc(date)))
  # CHECK: Count % of NA values
  # there will be some because early values don't have any data to backfill from
  #sort(sapply(runtime_ticker_data, function(x) round(sum(is.na(x)/(dim(x)[1]*dim(x)[2]))*100,0)), decreasing=TRUE)
  return(runtime_ticker_data)
}

compute_positions <- function(transaction_log, trade_history) {
  positions <- data.frame(matrix(ncol = 2, nrow = 0))
  x <- c("portfolio_members", "starting_position")
  colnames(positions) <- x
  cash_row <- c("CASH", sum(transaction_log$amount))
  positions[nrow(positions)+1,] <- cash_row
  positions$starting_position <- as.numeric(positions$starting_position)
  if(nrow(trade_history) != 0) {
    stock_positions <- trade_history %>% group_by(symbol) %>% summarise(sum(quantity)) %>%
      rename(portfolio_members = symbol,
             starting_position = `sum(quantity)`)
    positions <- bind_rows(positions, stock_positions)
  }
  return(positions)
}

compute_trades <- function(target_weights, positions) {
  # Bind target weights and positions
  trades <- full_join(positions, target_weights) 
  # Convert all NA to 0
  trades[is.na(trades)] <- 0
  # Get price of each stock
  quotes <- as.data.frame(
    t(do.call(cbind, lapply(trades$portfolio_members, get_stock_quote)))) %>%
    mutate(portfolio_members=trades$portfolio_members)
  trades <- left_join(trades,quotes, by="portfolio_members")
  # add cash quote
  trades[trades$portfolio_members == 'CASH', "bid"] <- 1
  trades[trades$portfolio_members == 'CASH', "offer"] <- 1
  trades[trades$portfolio_members == 'CASH', "size"] <- portfolio_starting_value
  # Compute portfolio value
  trades <- trades %>% mutate(price = (bid+offer)/2) %>%
    mutate(starting_value = price*starting_position)
  portfolio_value <- sum(trades$starting_value)
  print(paste(trades$starting_value, portfolio_value))
  # Adjust target weights for cash buffer percentage
  trades$target_weight <- trades$target_weight*(1-cash_buffer_percentage)
  # Fill in the cash target weight
  trades[trades$portfolio_members == 'CASH', "target_weight"] <- cash_buffer_percentage
  # Compute target values
  trades$target_value <- trades$target_weight*portfolio_value
  # Compute the value of the equalizing trades
  trades$order_value <- trades$target_value - trades$starting_value
  # Compute how many units need to be trades of each stock
  trades$order_units <- trades$order_value/trades$price
  trades$order_units_int <- round(trades$order_units,0)
  # Assign each order as a BUY or SELL
  trades <- trades %>%
    mutate(order_type = ifelse(order_value < 0, "SELL", ifelse(order_value > 0, "BUY", "NO TRADE" ))) %>%
    mutate(limit = ifelse(order_value < 0, bid, ifelse(order_value > 0, offer, 0 )))
  # Make sure the math works
  if(round(sum(trades$target_value),2) != round(portfolio_value,2)) {
    rm(trades)
    stop("ERROR: Portfolio target values and existing portfolio value are not equal.")
  } else if(round(sum(trades$target_weight),5) != 1) {
    rm(trades)
    stop("ERROR: Portfolio target weights don't sum to 1.")
  } else if(round(sum(trades$order_value),2) != 0) {
    rm(trades)
    stop("ERROR: Order values are not cash neutral.")
  }
  # Select only the most relevant columns
  trades <- trades %>%
    select(portfolio_members, order_type, limit, order_units_int, order_value) %>%
    tidyr::drop_na()
  
  return(trades)
}

portfolio_valuation <- function(positions) {
  # Convert all NA to 0
  positions[is.na(positions)] <- 0
  # Get price of each stock
  quotes <- as.data.frame(
    t(do.call(cbind, lapply(positions$portfolio_members, get_stock_quote)))) %>%
    mutate(portfolio_members=positions$portfolio_members)
  valuation <- left_join(positions,quotes, by="portfolio_members")
  # add cash quote
  valuation[valuation$portfolio_members == 'CASH', "bid"] <- 1
  valuation[valuation$portfolio_members == 'CASH', "offer"] <- 1
  valuation[valuation$portfolio_members == 'CASH', "size"] <- portfolio_starting_value
  # Compute portfolio value
  valuation <- valuation %>% mutate(price = (bid+offer)/2) %>%
    mutate(starting_value = price*starting_position) %>% 
    select(-size, -bid, -offer) %>%
    rename(position = starting_position,
           value = starting_value)
  return(valuation)
}


# CONTEXTUALLY DEFINED FUNCTIONS ################################
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
        select(max_price, min_price, spread, volume)
      # I take a random number between min and max,
      # And then I compute bid and offer from the spread.
      midpoint <- runif(1, quote_parameters$min_price, quote_parameters$max_price)
      bid <- midpoint - quote_parameters$spread
      offer <- midpoint + quote_parameters$spread
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
      
      session_trades <- session_trades %>%
        mutate(too_expensive = ifelse((commission < minimum_commission), TRUE, FALSE)) %>%
        dplyr::filter(too_expensive == FALSE)  %>%
        select(-too_expensive)
      
      # append session trade history to trade history
      trade_history <- bind_rows(trade_history, session_trades)
      # create session transaction log
      session_transactions <- session_trades %>%
        mutate(description = paste(action, quantity, symbol)) %>%
        mutate(net_amount = -net_amount) %>%
        select(timestamp, transaction, description, net_amount) %>%
        dplyr::rename(amount = net_amount)
      # append session transaction log to transaction log
      transaction_log <- bind_rows(transaction_log, session_transactions)
      # write to feather files
      write_feather(transaction_log, "portfolio/transaction_log.feather")
      write_feather(trade_history, "portfolio/trade_history.feather")
      print(paste("RESULT:", nrow(session_trades), "trades were made in this session."))
    }
  }
  
} else if(run_mode=="LIVE"){
  print("ERROR: Haven't implemented live trading yet.")
} else {
  print("ERROR: Something went wrong - malformed run_mode")
}

