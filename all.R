all_begin <- Sys.time()

source("101_datalog_csv_to_feather.R")
source("102_constituents_to_dataset.R")
source("102_metadata_to_dataset.R")
source("102_ticker_logs_to_dataset.R")
source("300_compute_weights.R")
#source("400_compute_returns.R")

all_end <- Sys.time()
all_end - all_begin