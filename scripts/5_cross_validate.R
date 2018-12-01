# Load paths and functions
source("scripts/data_pipeline/set_paths.R")
source("scripts/data_pipeline/data_pipeline_functions.R")

# Laod the datalog file names
data_log <- convert_datalog_to_dataframe()
# Take a quick look
glimpse(data_log)

# List the datalog sources
unique(data_log$source)

# Count how many of each data type there are
for (datasource in unique(data_log$source)) {
  data_source_log <- data_log %>% 
         filter(source == datasource) 
  data_source_types <- data_source_log %>%
         group_by(data_type) %>%
         summarise(n())
  print(paste("Data type counts for data source:", datasource))
  print(knitr::kable(data_source_types))
  
  for (datalabel in unique(data_source_log$data_type)) {
    data_type_log <- data_source_log %>% 
      filter(data_type == datalabel) 
    data_source_types <- data_type_log %>%
      group_by(data_label) %>%
      summarise(n())
    print(paste("Data label counts for", datasource, datalabel))
    print(knitr::kable(data_source_types))
  }
}