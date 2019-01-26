# LOAD LIBRARIES ###################################
library(magrittr)

# CONTEXT-IGNORANT TRADING FUNCTIONS ################################
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
  if (nrow(index_members) == 0) {
    stop(paste("FATAL: Constituent list is empty for date ", 
               execution_date, 
               ". Perhaps you don't have enough date for a backtest of range ", 
               start_backtest,
               "->",
               end_backtest,
               "?",
               sep=""))
  }
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
  trades <- full_join(positions, target_weights, by="portfolio_members") 
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
  # Adjust target weights for cash buffer percentage
  trades$target_weight <- trades$target_weight*(1-cash_buffer_percentage)
  # Fill in the cash target weight
  trades[trades$portfolio_members == 'CASH', "target_weight"] <- cash_buffer_percentage
  # Compute target values
  trades$target_value <- trades$target_weight*portfolio_value
  # compute the closeness to the target
  trades <- trades %>%
    mutate(deviation = abs(target_value/starting_value-1)) %>%
    mutate(soft_balanced = ifelse(deviation <= soft_rebalancing_constraint,
                                 TRUE,
                                 FALSE))
  # Compute the value of the equalizing trades
  trades$order_value <- trades$target_value - trades$starting_value
  # Compute how many units need to be traded of each stock
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
    dplyr::filter(soft_balanced == FALSE) %>%
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