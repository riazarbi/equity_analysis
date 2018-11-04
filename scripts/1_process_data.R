#####################################################
# DATA PIPELINE SCRIPTS #############################
# Clean out environment so we know this script works in a clean environment
rm(list=ls())
#####################################################
# DATA PIPELINE SCRIPTS - ONCE DAILY
# All these scripts can be run independently - they all actually clear the
# environment before running.
# These scripts are not parametrized, and they don't need to be.
source("scripts/data_pipeline/1_bloombergUSD_to_datalog.R")
source("scripts/data_pipeline/1_bloombergZAR_to_datalog.R")
source("scripts/data_pipeline/1_simulated_to_datalog.R")
source("scripts/data_pipeline/2_datalog_csv_to_feather.R")
source("scripts/data_pipeline/3_constituents_to_dataset.R")
source("scripts/data_pipeline/3_metadata_to_dataset.R")
source("scripts/data_pipeline/3_ticker_logs_to_dataset.R")
#####################################################