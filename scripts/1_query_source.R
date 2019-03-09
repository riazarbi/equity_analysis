#####################################################
# SOURCE QUERY SCRIPTS #############################
# Clean out environment so we know this script works in a clean environment
rm(list=ls())
#####################################################
# GET PROMPT
scripts <- c("scripts/data_processing/1_bloomberg_to_datalog.R",
             "scripts/data_processing/1_simulated_to_datalog.R")

print("Available scripts:")
for (i in seq_along((1:length(scripts)))) {
  print(paste(i, scripts[i], sep=" : "))
}

script_to_run <- readline(prompt=paste("Select script to run [1-",length(scripts),"]: ", sep=""))
source(scripts[as.integer(script_to_run)])