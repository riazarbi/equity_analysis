#####################################################
# DATA PIPELINE SCRIPTS #############################
# Clean out environment so we know this script works in a clean environment
rm(list=ls())
#####################################################
# DATA PIPELINE SCRIPTS - ONCE DAILY
# Maybe wrap into their own script
# All these scripts can be run independently - they actually clear the
# environment before running.
# These scripts are not parametrized, and they don't need to be.
source("data_pipeline_scripts/100_bloombergUSD_to_datalog.R")
source("data_pipeline_scripts/100_bloombergZAR_to_datalog.R")
source("data_pipeline_scripts/100_simulated_to_datalog.R")
source("data_pipeline_scripts/101_datalog_csv_to_feather.R")
source("data_pipeline_scripts/102_constituents_to_dataset.R")
source("data_pipeline_scripts/102_metadata_to_dataset.R")
source("data_pipeline_scripts/102_ticker_logs_to_dataset.R")
#####################################################

