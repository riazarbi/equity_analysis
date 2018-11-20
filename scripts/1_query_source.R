#####################################################
# SOURCE QUERY SCRIPTS #############################
# Clean out environment so we know this script works in a clean environment
rm(list=ls())
#####################################################
# All these scripts can be run independently - they all actually clear the
# environment before running.
# These scripts are not parametrized, and they don't need to be.
source("scripts/data_processing/1_bloombergUSD_to_datalog.R")
source("scripts/data_processing/1_bloomberg_to_datalog.R")
source("scripts/data_processing/1_simulated_to_datalog.R")
#####################################################