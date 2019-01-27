sink("console_log", split=TRUE)
# Install required packages
list.of.packages <- c("Rblpapi", 
                      "here", 
                      "doParallel", 
                      "xts",
                      "dygraphs",
                      "pbo")

new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)

# Reset all directories
delete_data <- readline(prompt=paste("Delete all datasets and datalog? [N/y]: ", sep=" "))
if(delete_data == "y") {
  print("Deleting all datasets...")
  unlink("data", recursive=T )
} else {
  print("Not deleting all datasets and datalog...")
}

# Delete trials
delete_trials <- readline(prompt=paste("Delete all files in the trials directory? [N/y]: ", sep=" "))
if(delete_trials == "y") {
  print("Deleting all files in the trials directory...")
  unlink("trials", recursive=T)
  # Copy in the sample trials
  dir.create("trials")
  print("Copying in samples to the trials directory...")
  file.copy(from=file.path("scripts/trading/sample_trials",list.files("scripts/trading/sample_trials")), 
            to="trials", 
            overwrite = TRUE, recursive = FALSE, 
            copy.mode = TRUE)
  
} else {
  print("Not deleting all files in the trials directory...")
}

print("Deleting any tempfiles...")
unlink("temp", recursive=T)
print("Deleting any old results...")
unlink("results", recursive=T)

# Run the entire processing chain
print("Running 1_query_source.R")
source("scripts/1_query_source.R")

print("Running 2_process_data.R")
source("scripts/2_process_data.R")

print("Running 3_run_trials.R")
source("scripts/3_run_trials.R")

print("Running 4_report.R")
source("scripts/4_report.R")

print("Running 5_cross_validate.R")
source("scripts/5_cross_validate.R")

sink(type="message")
sink()
