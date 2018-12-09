# Install required packages
list.of.packages <- c("Rblpapi", 
                      "here", 
                      "doParallel", 
                      "xts")

new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)

# Reset all directories
unlink("data", recursive=T )
unlink("trials", recursive=T)
unlink("temp", recursive=T)
unlink("results", recursive=T)

# Copy in the sample trials
dir.create("trials")
file.copy(from=file.path("scripts/trading/sample_trials",list.files("scripts/trading/sample_trials")), 
          to="trials", 
          overwrite = TRUE, recursive = FALSE, 
          copy.mode = TRUE)

# Run the entire processing chain
source("scripts/1_query_source.R")
source("scripts/2_process_data.R")
source("scripts/3_run_trials.R")
source("scripts/4_report.R")
source("scripts/5_cross_validate.R")