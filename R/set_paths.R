# Libraries
library(tidyverse)
library(feather)
library(data.table)
library(lubridate)
library(magrittr)
library(dplyr)
library(parallel)

###################################################################
# SET DIRECTORIES

# Get working directory
working_directory <- here::here()

# Define data directory paths
data_directory <- file.path(working_directory, "data")
dimensions_directory <- file.path(working_directory, "data/dimensions")
datalog_directory <- file.path(working_directory, "data/datalog")
dataset_directory <- file.path(working_directory, "data/datasets")
dir.create(data_directory, showWarnings = FALSE)
dir.create(dataset_directory, showWarnings = FALSE)

# Define trial directory
trial_directory <- file.path(working_directory, "trials")
dir.create(trial_directory, showWarnings = FALSE)

# Define results directory
results_directory <- file.path(working_directory, "results")
dir.create(results_directory, showWarnings = FALSE)

##################################################################
