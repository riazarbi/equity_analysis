##################################
# CONTEXT
# By the time this script runs you have scraped Bloomberg
# and saved the data in the correct format in the datalog folder. 
# The datalog data has been used to update the per-ticker datasets
# the constituent lists and the metadata arrays.
# So the purpose of this script is to use the datasets to build
# an in-memory list of dataframes that can be passed to a backtester.

# This list of dataframes will have the following structure:
# Each dataframe will have a row for each date, and a column for each metric.
# Each NA value will be filled with the last known value
# There should be two more dataframes - 
# The constituent list dataframe has a list of all constituents for each date
# The metadata array should have metadata for each constituent
##################################
# Start timing
##############################################################
print("NEXT: Loading slow-moving data into memory...")
begin <- Sys.time()

library(cluster)
library(factoextra)
library(xts)
##############################################################
## BUILD DATASETS
# Load constituent list
# Filter constituent_list source to only include the parameter specified source
# Filter constituent_list index to only include the parameter specified index
constituent_list <- read_feather(file.path(dataset_directory, 
                                           "constituent_list", 
                                           "constituent_list.feather")) %>% 
  dplyr::filter(grepl(data_source, source))  %>%
  dplyr::filter(grepl(constituent_index, index))

# Issue with naming: we want to use the ticker names as dataframe names. 
# But the tickers all have whitespace.
# So we replace ticker whitepace with _ so they can be legal variable names
constituent_list$ticker <- str_replace_all(constituent_list$ticker," ","_")

# Get a list of all the tickers in the constituent list
tickers <- constituent_list$ticker
tickers <- unique(tickers)

# Load metadata array
# Rename metadata TICKER_AND_EXCH_CODE column to ticker
metadata <- read_feather(file.path(dataset_directory, "metadata_array", "metadata_array.feather")) %>%
  rename(ticker = market_identifier)

# Replace ticker whitepace with _
# so they can be legal variable names (as above)
metadata$ticker <- str_replace_all(metadata$ticker," ","_")
# Filter metadata to just tickers that exist in backtest universe
metadata <- dplyr::filter(metadata,  ticker %in% tickers)
# Cast metadata into flatfile form (col per variable, row per ticker)
metadata <- reshape2::dcast(metadata, ticker + timestamp + source ~ metric)
# Check what tickers didn't make it because they aren't in the metadata file
length(tickers)
length(metadata$ticker)
dropped_tickers <- setdiff(tickers, metadata$ticker)
if(length(dropped_tickers) == 0) {dropped_tickers <- 0}
# Move from constituent-based list to metadata-based list
print(paste("Dropping", 
            dropped_tickers,
            "tickers from constituent list because of insufficient metadata"))
# Remove tickers list
# No longer needed since we are using the metadata array
rm(tickers)

############################################################################
# At this point we have a metadata dataframe of every ticker in
# the dataset. We also have a long constituent membership dataframe
# that tells us which tickers belong to the index.
###########################################################################

# Build list of ticker dataframes
# Create a filename column in the metadata array for marketdata lookups.
metadata$marketdata_filename <- str_replace_all(metadata$ticker," ","_") %>% 
                                   paste(".feather", sep="")
# Infer fundamental filename for each ticker
metadata$fundamental_filename <- str_replace_all(metadata$fundamental_identifier," ","_") %>%
                                   paste(".feather", sep="")

##########################################################################
# NBNBNBNB: Metadata dataset will need to be timestamp filtered
# But I haven't done that here #
##########################################################################

# Load each ticker into a dataframe
ticker_data <- list()
# Start building field lists for donwstream analysis
complete_fundamental_field_list <- character()
complete_market_field_list <- character()

for (i in 1:length(metadata$ticker)){
  ticker <- metadata$ticker[i]
  cat("\r", paste("Loading", ticker, "              "))
  # Define the file paths of the datasets
  marketdata_filename <- metadata$marketdata_filename[i]
  fundamental_filename <- metadata$fundamental_filename[i]
  market_data_filepath <- file.path(dataset_directory, 
                                    "ticker_market_data", marketdata_filename)
  fundamental_filepath <- file.path(dataset_directory, 
                                    "ticker_fundamental_data", fundamental_filename)
  # Load ticker data
  if (file.exists(market_data_filepath) & 
      file.exists(fundamental_filepath)) {
    market_data <- read_feather(market_data_filepath)
    # add colnames ot field list
    complete_market_field_list <- c(complete_market_field_list, unique(market_data$metric))
    fundamental_data <- read_feather(fundamental_filepath)
    complete_fundamental_field_list <- c(complete_fundamental_field_list, unique(fundamental_data$metric))
    # merge the datasets
    single_ticker_data <- bind_rows(market_data, fundamental_data)
    rm(market_data)
    rm(fundamental_data)
  } else if (file.exists(market_data_filepath)) {
    single_ticker_data <- read_feather(market_data_filepath)
  } else if (file.exists(fundamental_filepath)) {
    single_ticker_data <- read_feather(fundamental_filepath)
  } else {
    print(paste("No data for", ticker))
    next}
  # use only the most recent timestamped value for each
  # metric for each date
  single_ticker_data <- single_ticker_data %>%
    mutate(key=paste(date, metric, source, sep = "|")) %>%
    arrange(desc(key)) %>%
    filter(key != lag(key, default="0")) %>% 
    select(-key, -timestamp)
  # append ticker datasets to list
  ticker_data[[ticker]] <- single_ticker_data
  rm(fundamental_filename)
  rm(fundamental_filepath)
  rm(single_ticker_data)
  rm(market_data_filepath)
  rm(marketdata_filename)
  rm(ticker)
}
rm(i)
# Start building field lists for donwstream analysis
complete_fundamental_field_list <- unique(complete_fundamental_field_list)
complete_market_field_list <- unique(complete_market_field_list)
# CLEANUP: Drop empty dataframes
ticker_data <- ticker_data[sapply(ticker_data, 
                              function(x) (dim(x)[1]) > 0)]
# TRANSFORM: into FLATFILE form
print("Casting ticker_data into flatfile form.")
ticker_data <- lapply(ticker_data, 
                    function(x)
                      x %>% spread(metric, value))
# FILTER: keep only necessary metrics
print("Dropping unnecessary fields....")
print("NOTE: If market_metrics and fundamental_metrics have not been specified no fields will be dropped. 
      This is risky, though, because then you could have tickers which don't have the necessary metrics to trade.
      In that case, the backtest will fail.")
if((length(market_metrics) + length(fundamental_metrics))!=0) {
  ticker_data <- lapply(ticker_data, function(x) x <- x %>%
                        select(one_of(metrics)))
  for (tick in names(ticker_data)){
    if (length(market_metrics) != sum(market_metrics  %in% colnames(ticker_data[[tick]]))) {
      print(paste("Dropping", tick, "because it is missing market data required for algorithm computation"))
      ticker_data[[tick]] <- NULL
    }
    if (length(fundamental_metrics) != sum(fundamental_metrics  %in% colnames(ticker_data[[tick]]))) {
      print(paste("Dropping", tick, "because it is missing fundamental data required for algorithm computation"))
      ticker_data[[tick]] <- NULL
    }
  }
}

print("Looking for tickers that must be excluded because they have no data...")
# CLEANUP: drop dataframes that don't have volume or last_price data
for (tick in names(ticker_data)){
  if(length(volume_data) != sum(volume_data  %in% colnames(ticker_data[[tick]]))) {
    print(paste("Dropping", tick, "because it is missing volume data"))
    ticker_data[[tick]] <- NULL
  }
}

for (tick in names(ticker_data)){
  if(length(last_price_field) != sum(last_price_field  %in% colnames(ticker_data[[tick]]))) {
    print(paste("Dropping", tick, "because it is missing last_price data"))
    ticker_data[[tick]] <- NULL
  }
}

##############################################################################################
# LAG METRIC DETECTION
# Read in all the fundamental source data
# this section should not modify any ticker data
print("Detecting which fundamental metrics need lag-correction...")
if (exists("fundamental_dates")) {
  rm(fundamental_dates)
}
print("Reading in fundamental data...")
for (i in seq_along(1:nrow(metadata))) {
  fundamental_filename <- metadata$fundamental_filename[i]
  fundamental_filepath <- file.path(dataset_directory, 
                                    "ticker_fundamental_data", fundamental_filename)
  if (!file.exists(fundamental_filepath)) {
    next 
  }
  single_fundamental_dates <- read_feather(fundamental_filepath) %>% select(-source, -timestamp)
  if(nrow(single_fundamental_dates) == 0) {
    next
  }
  if ((!exists("fundamental_dates"))) {
    fundamental_dates <- single_fundamental_dates
  } else {
    fundamental_dates <- bind_rows(single_fundamental_dates, fundamental_dates)
  }
}

# Count how many occurrences there are across tickers per fundamental metric
print("Counting occurrences per date...")
fundamental_date_counts_df <- fundamental_dates %>% group_by_(.dots=c("date","metric")) %>%
  summarise(n()) %>% 
  as_tibble() %>% 
  rename(count = 'n()') %>%
  spread(metric, count)

# Convert to xts
fundamental_date_counts <- xts(fundamental_date_counts_df %>% select(-date), order.by=fundamental_date_counts_df$date) 

# Take just the last 2 years
print("Filtering to just the last two years...")
fundamental_date_2yr_counts <- fundamental_date_counts %>% last('2 year') %>% colSums(na.rm=T) %>% enframe() %>% arrange(value)

# Try auto-detects how many clusters there are
kmax <- min(nrow(fundamental_date_2yr_counts), 5)
# Plot many k-means.
if(kmax > 2) {
  fundamental_metrics_silhouette_plot <- fviz_nbclust(as.data.frame(fundamental_date_2yr_counts$value), 
                                                    kmeans, 
                                                    method = "silhouette", 
                                                    k.max = kmax)
}
# Only use the estimated number of clusters if "auto" sleected in parameters.
# Figure out the cluster number we should use.
if(fundamental_data_metric_types == "auto") {
  print("Auto-detecting number of clusters in fundamental metrics...")
  number_clusters <- fundamental_metrics_silhouette_plot$data %>% 
    filter(y == max(y)) %>% 
    select(clusters) %>% 
    pull() %>% 
    as.numeric()
} else {
  print("Number of clusters is hard-coded in parameters.R")
  number_clusters <- fundamental_data_metric_types
}

# Classify the metrics 
print("Classifying metrics into clusters...")
kmeans_model <- kmeans(fundamental_date_2yr_counts$value, number_clusters, nstart = 25) 
fundamental_metric_cluster_labels <- kmeans_model$cluster
# Tag the metrics as part of a cluster
print("Labeling metrics..")
fundamental_metric_clusters <- cbind(fundamental_date_2yr_counts, fundamental_metric_cluster_labels)
# Determine the cluster with the lowest score
lag_cluster <- kmeans_model$centers 
lag_cluster <- match(min(lag_cluster),lag_cluster)
# Filter the metrics and get the ones that need to be lagged.
print("Creating list of lag metrics...")
lag_metrics <- fundamental_metric_clusters %>% 
  filter(fundamental_metric_cluster_labels == lag_cluster) %>% 
  select(name) %>% pull()
print(lag_metrics)

# Visualize the metric clusters.
fundamental_metric_clusters_plot <- fviz_cluster(kmeans_model, data = as.data.frame(fundamental_date_2yr_counts), geom = "point",
                                                 stand = FALSE, ellipse.type = "norm") + coord_flip()

##############################################################################################
# APPLYING LAG TO LAG METRICS

if (fundamental_data_lag_adjustment == 0) {
  print("Lag adjustment of 0 specified. Skipping lag adjustment.")
} else {
  print(paste("Applying lag adjustment of", fundamental_data_lag_adjustment, "days to lag metrics..."))
  ticker_data <- lapply(ticker_data, 
                     function(x) {
                    # create one df with no lag metrics
                    split_x <- x %>% select(-one_of(lag_metrics))
                    # create another one with just lag metrics
                    # then lag them
                    lagged_x <- x %>% 
                         select(date, one_of(lag_metrics)) %>%
                         mutate(date = (date + days(fundamental_data_lag_adjustment))) 
                    # join them back together
                      x <- left_join(split_x, lagged_x, by = "date")
                     })

}

# Get object size of test data
print(paste("Slow Moving Data object size:", 
            format(object.size(ticker_data), units="auto", standard = "IEC")))

#############################################################################################
# Create a market dataset
print("NEXT: Loading price and volume data into memory...")
print("NOTE: Will fail if you haven't specified price_related_data and volume_related_data vectors in parameters.R")
print("Performing the following operations:")
print("Taking relevant fields from ticker_data...")
print("Renaming source-specific fields to standard field names...")
print("Imputing missing 'max_price', 'min_price', 'last' and 'volume' values...")
print("Price imputation is just backfilled from last known value. Volume is average over last 3 months...")

price_data <- lapply(ticker_data, 
        function(x) {
         my.max <- function(x) ifelse( !all(is.na(x)), max(x, na.rm=T), NA)
         my.min <- function(x) ifelse( !all(is.na(x)), min(x, na.rm=T), NA)
         y <- x %>% 
           dplyr::select(one_of(price_related_data))
         date_stash <- y$date
         y <- y %>% 
           select(-date)  %>% 
           mutate(max_price = apply(., 1, my.max)) %>% 
           mutate(min_price = apply(., 1, my.min)) %>%
           #mutate(spread = standard_spread*max_price) %>%
           add_column(date=date_stash)
         # NOTE: Spread is arbitrary!
         # https://www.bauer.uh.edu/rsusmel/phd/roll1984.pdf
         # Try create a more realistic estimate of spread
         # rename volume to standard name
         z <- x %>% 
           dplyr::select(one_of(volume_data)) %>% 
           rename(volume = !!names(.[2]))
         # next - define the last price field and join back to price data
         w <- x %>% 
           dplyr::select(one_of(last_price_field)) %>% 
           rename(last = !!names(.[2]))
         x <- full_join(z, y, by = "date")
         x <- full_join(w, x, by = "date") %>%
           rename(last = !!names(.[2])) # in the event that last exists in both dataframes this fixes it.
         # impute: replace NA in max_price, min_price and last with last known value
         # impute: replace NA in volume with zero
         # the alternative is some sort of rolling mean, but this may add more volume than is realistically available.
         x <- x %>% fill(max_price, min_price, last) %>% 
           mutate(volume = replace_na(volume, 0)) 
         })

# Save image to data/datasets/slow_moving_data.Rdata
if(!dir.exists("temp")) {
  dir.create("temp")
}
print("Saving ticker_data to temp/temp/ticker_data.Rds")
saveRDS(ticker_data, file = "temp/ticker_data.Rds")
print("Saving price_data to temp/price_data.Rds")
saveRDS(price_data, file = "temp/price_data.Rds")
print("Saving metadata to temp/metadata.Rds")
saveRDS(metadata, file = "temp/metadata.Rds")
print("Saving constituent_list to temp/constituent_list.Rds")
saveRDS(constituent_list, file = "temp/constituent_list.Rds")
# Save the time this script completed so that we know it has run. 
ticker_data_load_date <- Sys.time()
# Print how long the script took to run
end <- Sys.time()
print(end - begin)
