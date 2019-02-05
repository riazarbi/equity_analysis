# Start logging to a file
#unlink("console_log")
#sink("console_log", split=TRUE)

# Install required packages
list_of_packages <- c("Rblpapi", 
                      "here", 
                      "doParallel", 
                      "xts",
                      "dygraphs",
                      "pbo",
                      "gridExtra",
                      "viridis")

new_packages <- list_of_packages[!(list_of_packages %in% installed.packages()[,"Package"])]
if(length(new_packages)) install.packages(new_packages)
rm(list_of_packages, new_packages)

# Reset all directories
delete_data <- readline(prompt=paste("Delete all datasets? Datalogs will not be removed. [N/y]: ", sep=" "))
if(delete_data == "y") {
  print("Deleting all datasets...")
  unlink("data/datasets", recursive=T )
  rm(delete_data)
} else {
  print("Not deleting all datasets...")
}

# Reset all directories
run_data_report <- readline(prompt=paste("Run data report? (tends to fail on simulated data). [N/y]: ", sep=" "))
if(run_data_report=="y" | run_data_report == "Y") {
  print("Data report will be run after loading data.")
} else print("Data report will NOT be run after loading data.")

if(run_data_report=="y" | run_data_report=="Y") {
  write_report <- file("write_report")
  writeLines(c("YES"), write_report)
  close(write_report)
}
  
# Delete trials
delete_trials <- readline(prompt=paste("Delete all files in the trials directory? [N/y]: ", sep=" "))
if(delete_trials == "y") {
  print("Deleting all files in the trials directory...")
  unlink("trials", recursive=T)
  rm(delete_trials)
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

print("Running 3_load_data.R")
source("scripts/3_load_data.R")

if(file.exists("write_report")) {
  unlink("write_report")
  rmarkdown::render(file.path(results_directory, "data_quality.Rmd"))
}
  
print("Running 4_run_trials.R")
source("scripts/4_run_trials.R")

print("Running 5_report_parallel.R")
source("scripts/5_report_parallel.R")

print("Running 6_cross_validate.R")
source("scripts/6_cross_validate.R")

# Stop logging to a file
#sink(type="message")
#sink()
# Move the logfile to results
#file.rename("console_log", "results/console_log")
