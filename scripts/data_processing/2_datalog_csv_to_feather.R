print("")
print("NEXT: Cleaning up datalog and converting CSVs to feather...")
# Clear environment
rm(list=ls())
# Log the time taken for the script
begin <- Sys.time()
# Read in data pipeline functions
source("R/set_paths.R")
source("R/data_pipeline_functions.R")

# Create a dataframe of data log files
print("Scanning datalog...")
data_log <- convert_datalog_to_dataframe()

# Remove empty csvs
print("Removing any empty CSVs in the datalog...")
data_log <- remove_empty_csvs(data_log)

# Show avaialable datasets
print("Available data_types in the datalog:")
print(unique(data_log$data_type))

print("Converting all csv files in the datalog to feather file format...")
# Get the list of files
filenames <- data_log$filename  

# For each csv, check if there is a feather and, if not, create a feather sidecar
mclapply(filenames, function(i){
  # only look at csv files
  if(tools::file_ext(i) == "csv") {
    # define the new possible feather file path
    j <- file.path(datalog_directory, paste(tools::file_path_sans_ext(i), ".feather", sep=""))
    # define the full csv file path
    i <- file.path(datalog_directory, i)
    # only convert if it hasn't been done already
    if(!file.exists(j)){
      # read the csv into memory
      df <- fread(i, 
                  stringsAsFactors = FALSE, 
                  colClasses = 'character')
      # write the 
      write_feather(df, j)
  }
  }
})

print("CSV to feather conversions done.")
print("Checking there are the same number of CSVs and feather files:")
data_log <- convert_datalog_to_dataframe()
print(knitr::kable(data_log %>% 
  group_by(ext) %>% 
  summarise(n())))
# Get execution time
end <- Sys.time()
print(end-begin)
