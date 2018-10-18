# Read in shared functions
source('shared_functions.R')

# Time the script
begin <- Sys.time()
print("Converting all csv files in the datalog to feather file format")
# Create a dataframe of data log files
data_log <- convert_datalog_to_dataframe()
data_log <- remove_empty_csvs(data_log)

# Get the list of files
filenames <- data_log$filename  

# For each csv, check if there is a feather and, if not,
# create a feather sidecar
lapply(filenames, function(i){
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

print("done.")
# Get execution time
end <- Sys.time()
end-begin