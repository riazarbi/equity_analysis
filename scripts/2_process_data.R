#####################################################
# DATA PIPELINE SCRIPTS #############################
# Clean out environment so we know this script works in a clean environment
rm(list=ls())
#####################################################

# DATA PIPELINE SCRIPTS - ONCE DAILY
# All these scripts can be run independently - they all actually clear the
# environment before running.
# These scripts assume that there is data in the datalog
# These scripts are not parametrized, and they don't need to be.
source("scripts/data_processing/2_datalog_csv_to_feather.R")
source("scripts/data_processing/3_constituents_to_dataset.R")
source("scripts/data_processing/3_metadata_to_dataset.R")
source("scripts/data_processing/3_ticker_logs_to_dataset.R")

#####################################################
