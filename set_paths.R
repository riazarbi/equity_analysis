# Libraries
library(tidyverse)
library(feather)
library(data.table)
library(lubridate)
library(magrittr)
library(dplyr)

###################################################################
# SET DIRECTORIES

# Get working directory
working_directory <- getwd()

# Define directory paths
dimensions_directory <- file.path(working_directory, "dimensions")
datalog_directory <- file.path(working_directory, "datalog")
dataset_directory <- file.path(working_directory, "datasets")
dir.create(dataset_directory, showWarnings = FALSE)

##################################################################