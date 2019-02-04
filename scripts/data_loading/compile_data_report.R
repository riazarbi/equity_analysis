# COMPILE DATA REPORT
# Load data
###############################################################
# Assumed load_slow_moving_data has just been run
# Load datalog
datalog <- convert_datalog_to_dataframe()

# Load libraries
###############################################################
library(tidyverse)
library(lubridate)
library(xts)
library(factoextra)
library(cluster)
library(viridis)
library(ggExtra)

# DATALOG STATS ##############################################
# number of files per extension
number_datalog_files <- as.character(datalog %>% summarise(n()))
number_feather_files <- as.character(datalog %>% filter(ext == "feather") %>% summarise(n()))
number_csv_files <- as.character(datalog %>% filter(ext == "csv") %>% summarise(n()))
# select just feather
datalog <- datalog %>% filter(ext == "feather")

# datalog counts
# number of constituent files in datalog
datalog_constituent_files <- datalog %>% filter(data_type=="constituent_list")
# number of metadata files in datalog
datalog_metadata_files <- datalog %>% filter(data_type=="metadata_array")

# number of tickers in marketdata logs
datalog_unique_market_tickers <- datalog %>% filter(data_type == "ticker_market_data") %>% select(data_label) %>% unique()

# number of tickers in marketdata datasets
dataset_market_tickers <- list.files(file.path(dataset_directory, "ticker_market_data"))

# number of tickers in fundamental data logs
# note - not necessarily equal to number in marketdata; sometimes ISIN covers two tickers.
datalog_unique_fundamental_tickers <- datalog %>% filter(data_type == "ticker_fundamental_data") %>% select(data_label) %>% unique()

# compare to number in datasets
dataset_fundamental_tickers <- list.files(file.path(dataset_directory, "ticker_fundamental_data"))

# COMPLETENESS STATS ##############################################
# number of tickers in constituent lists
constituent_tickers <- read_feather(file.path(dataset_directory, "constituent_list", "constituent_list.feather")) %>% 
  filter(source == data_source) %>%  
  filter(index == constituent_index) %>%
  select(ticker) %>%
  unique() 

# number of tickers in metadata list
metadata_dataset <- read_feather(file.path(dataset_directory, "metadata_array", "metadata_array.feather")) %>% filter(market_identifier %in% constituent_tickers$ticker)
metadata_tickers <- metadata_dataset %>% 
  select(market_identifier) %>%
  unique()

# QUESTION: Does every constituent have market data?
market_datasets <- tools::file_path_sans_ext(dataset_market_tickers) %>% 
  gsub("_", " ", ., fixed = TRUE) %>% 
  enframe()
constituents_without_marketdata <- setdiff(constituent_tickers$ticker, market_datasets$value) # 0 means complete coverage

# QUESTION: Does every constituent have fundamental data?
fundamental_datasets <- tools::file_path_sans_ext(dataset_fundamental_tickers) %>% 
  gsub("_", " ", ., fixed = TRUE) %>% 
  enframe() 

# ISINs of constituents
constituent_ISINs <- metadata_dataset %>% filter(source == data_source,
                                                 market_identifier %in% constituent_tickers$ticker,
                                                 metric == "fundamental_identifier")
constituents_without_fundamentals <- setdiff(constituent_ISINs$value, fundamental_datasets$value)


constituents_without_fundamentals <- metadata_dataset %>% filter(value %in% constituents_without_fundamentals) %>% select(market_identifier) %>% unique()

# # QUESTION: Does every constituent have metadata?
# Count differences between constituent lists and tickers
constituents_without_metadata <- setdiff(constituent_tickers$ticker, metadata_tickers$market_identifier)

# Metadata health check
incomplete_metadata <- metadata_dataset %>% spread(metric, value) %>% filter(!complete.cases(.)) %>% select_if(~sum(is.na(.)) > 0)

percent_missing_metadata <- round(nrow(incomplete_metadata) / nrow(metadata_dataset %>% spread(metric, value)),2)

# METRIC COMPLETENESS ##############################################
# NOTE: WILL BE LIMITED BY METRICS SPECIFIED IN PARAMETERS
# 1. Build a list of months in the datasets
dataset_date_increments <- bind_rows(ticker_data, .id = "ticker") %>% 
  select(date) %>%
  unique() %>% 
  dplyr::mutate(year = lubridate::year(date), 
                month = lubridate::month(date),
                month_days = lubridate::days_in_month(date)) %>%
  mutate(month_start = rollback(date, roll_to_first = TRUE)) %>%
  mutate(month_end = ymd(paste(year, month, month_days, sep=","))) %>% 
  select(month_start, month_end) %>%
  unique() %>%
  arrange(month_start)

# MARKET METRIC COMPLETENESS
# Bind into a gigantinc dataframe

market_metric_nas <- bind_rows(ticker_data, .id = "ticker") %>% 
  select(-ticker) %>% select(one_of(complete_market_field_list), date)

if(exists("monthly_nas")) {
  rm("monthly_nas")
}
# 2. Iterate through ticker_data, month by month, computing NA % per field
for (i in seq_along(1:nrow(dataset_date_increments))) {
  start_date <- dataset_date_increments[i,]$month_start
  end_date <- dataset_date_increments[i,]$month_end 
  column_name = as.character(end_date)
  month_nas <- market_metric_nas  %>%
    filter(date >= start_date,
           date <= end_date) %>%
    select(-date) %>%
    gather(key = "metric", value = "value") %>%
    group_by(metric) %>%
    summarize(missing_share = mean(is.na(value))) %>%
    as_tibble() %>%
    dplyr::rename(!!column_name := missing_share)
  if(!exists("monthly_nas")) {
    monthly_nas <- month_nas
  } else {
    monthly_nas <- full_join(monthly_nas, month_nas, by = "metric")
  }
}
# Free up some memory
rm(market_metric_nas, month_nas)
gc(full=TRUE)

# get into correct format for plotting
monthly_nas_melted <- monthly_nas %>% 
  gather(key="month_end", value="proportion_NA", -metric) %>% 
  arrange(month_end) %>% 
  mutate(month_end = ymd(month_end)) %>%
  mutate(year = year(month_end)) %>%
  mutate(month = month(month_end))

# create plot
monthly_nas_plot <- ggplot(monthly_nas_melted,aes(month_end,metric,fill=proportion_NA))+
  geom_tile(color= "white",size=0.1) + 
  scale_fill_viridis(name="Proportion NA",option ="C") + 
  theme_minimal(base_size = 8) +
  labs(title= paste("Proportion NA per metric"), x="Month", y="Metric") +
  theme(legend.position = "right") +
  theme(plot.title=element_text(size = 14))+
  theme(axis.text.y=element_text(size=6)) +
  theme(strip.background = element_rect(colour="white"))+
  theme(plot.title=element_text(hjust=0))+
  theme(axis.ticks=element_blank())+
  theme(axis.text=element_text(size=7))+
  theme(legend.title=element_text(size=8))+
  theme(legend.text=element_text(size=6)) +
  removeGrid()

monthly_nas_averages <- monthly_nas %>% select(-metric) %>% colMeans()
monthly_nas_averages <- xts(monthly_nas_averages, order.by=(ymd(names(monthly_nas_averages))))


# FUNDAMENTAL METRIC CLUSTERING
# Math has been done in load_slow_moving_data.R
# As long as you load that .Rdata object you should be fine.

# FUNDAMENTAL DATA LAG DETECTION
lag_metrics_date_counts <- fundamental_date_counts_df %>% select(lag_metrics, date) 
lag_metrics_date_counts$sums <- lag_metrics_date_counts %>% select(-date) %>% rowSums(na.rm = T)
most_frequent_lag_dates <- lag_metrics_date_counts %>% select(date, sums) %>% arrange(desc(sums)) %>% head(7)

# Computing 

ticker_data_sample <- sample(ticker_data, length(ticker_data)/5)

ticker_data_sample_df <- bind_rows(ticker_data_sample, .id = "ticker") %>%   
  select(one_of(lag_metrics), date)

if (length(ticker_data_sample_df) != 1) {
  lag_adjusted_date_counts <- ticker_data_sample_df %>% 
    gather(key="metric", value = "value", -date) %>%
    drop_na(value) %>%
    select(date, metric) %>%
    group_by(date) %>%
    summarize(sums = n()) %>%
    as_tibble() %>%
    arrange(desc(sums)) %>%
  head(7)
}
# TICKER METRIC COMPLETENESS ##############################################
ticker_data_filtered <- lapply(ticker_data, 
                               function(x) 
                                 (filter(x, date <= end_backtest)))
ticker_data_filtered <- lapply(ticker_data_filtered, 
                               function(x) 
                                 (filter(x, date >= start_backtest)))

relevant_tickers <- constituent_list %>% dplyr::filter(date <= end_backtest,
                                                       date >= start_backtest) %>% select(ticker) %>% unique()

for (i in seq_along(1:length(ticker_data_filtered))) {
  tick_name <- names(ticker_data_filtered)[i]
  if (!(tick_name %in% relevant_tickers$ticker)) {
    ticker_data_filtered[[tick_name]] <- NULL
      }
}

# drop empty dataframes
ticker_data_filtered <- ticker_data_filtered[sapply(ticker_data_filtered, 
                                  function(x) (dim(x)[1]) > 0)]


overall_marketdata_score <- list()
algo_marketdata_score <- list() 
price_related_data_score <- list()
volume_data_score <- list()
last_price_field_score  <- list()

tickers <- names(ticker_data_filtered)
market_metrics_evaluated <-unique(c(market_metrics, price_related_data, volume_data, last_price_field))

for (i in seq_along(1:length(tickers))) {
  tick_name <- tickers[i]
  tick_data <- ticker_data_filtered[[tick_name]]
  tick_data <- tick_data %>% dplyr::select(one_of(market_metrics_evaluated))
  # compute NA
  na_percentages <- colMeans(is.na(tick_data))
  # saving scores
  overall_marketdata_score[[tick_name]] <- mean(na_percentages)
  algo_marketdata_score[[tick_name]] <- mean(na_percentages[names(na_percentages) %in% market_metrics])
  price_related_data_score[[tick_name]] <- mean(na_percentages[names(na_percentages) %in% price_related_data])
  volume_data_score[[tick_name]] <- mean(na_percentages[names(na_percentages) %in% volume_data])
  last_price_field_score[[tick_name]]  <- mean(na_percentages[names(na_percentages) %in% last_price_field])
}

na_scores <- as_data_frame(cbind(unlist(overall_marketdata_score),
                                 unlist(algo_marketdata_score),
                                 unlist(price_related_data_score),
                                 unlist(volume_data_score),
                                 unlist(last_price_field_score))) %>% 
  replace(is.na(.), 1)

colnames(na_scores) <- c("Overall Market Fields", "Algo Market Fields", "Price Related Fields", "Volume", "Last Price")
na_scores$ticker <- names(overall_marketdata_score)
all_quantiles <- quantile(as.matrix(na_scores %>% select(-ticker)))

na_ranks <- na_scores %>% mutate(overall_percentile_rank = ntile(`Overall Market Fields`,100),
                                 algo_percentile_rank = ntile(`Algo Market Fields`,100),
                                 price_percentile_rank = ntile(`Price Related Fields`,100),
                                 volume_percentile_rank = ntile(`Volume`,100),
                                 last_price_percentile_rank = ntile(`Last Price`,100))

worst_overall <- na_ranks %>% filter(overall_percentile_rank >= 0.9*max(overall_percentile_rank)) %>% select(`Overall Market Fields`) %>% colMeans()
worst_algo <- na_ranks %>% filter(algo_percentile_rank >= 0.9*max(algo_percentile_rank)) %>% select(`Algo Market Fields`) %>% colMeans()
worst_price <- na_ranks %>% filter(price_percentile_rank >= 0.9*max(price_percentile_rank)) %>% select(`Price Related Fields`) %>% colMeans()
worst_volume <- na_ranks %>% filter(volume_percentile_rank >= 0.9*max(volume_percentile_rank)) %>% select(`Volume`) %>% colMeans()
worst_last <- na_ranks %>% filter(last_price_percentile_rank >= 0.9*max(last_price_percentile_rank)) %>% select(`Last Price`) %>% colMeans()

bottom_10_stats <- c(worst_overall, worst_algo, worst_price, worst_volume, worst_last)

# try clear some space

gc(full=TRUE)
save.image(file = "temp/data_report.RData")

# copy reports template to reports directory
file.copy("scripts/reporting/data_quality.Rmd", 
          file.path(results_directory, "data_quality.Rmd"), overwrite = T)

rm(list=ls())
source("R/set_paths.R")
gc(full=TRUE)
# knit the report
rmarkdown::render(file.path(results_directory, "data_quality.Rmd"))
print("Done!")