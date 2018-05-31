source('011_utils.R')
data_log <- convert_datalog_to_dataframe()
head(data_log)

unique(data_log$source)
unique(data_log$data_type)
unique(data_log$data_label)

###########################################################################################################################################
# Building market dataset
market_data_log <- dplyr::filter(data_log, grepl("ticker_market_data", data_type))
market_data_log$ticker <- str_split_fixed(market_data_log$data_label, "__", 4)[,2]
market_data_log$ticker <- tools::file_path_sans_ext(market_data_log$ticker)
market_data_log$ticker <- paste(str_split_fixed(market_data_log$ticker, " ", 3)[,1], str_split_fixed(market_data_log$ticker, " ", 3)[,2])
market_data_log$latest_date <- str_split_fixed(market_data_log$data_label, "__", 4)[,1]
head(market_data_log)
length((unique(market_data_log$ticker)))
#View(market_data)

# Connec to database
db <- dbConnect(SQLite(), "datasets/market.sqlite")
tickers <- unique(market_data_log$ticker)
for (i in 1:length(tickers)) {
  # Defin table name
  table_name <- str_replace_all(tickers[i],"[[:punct:]\\s]+","_")
  # Get list of files relating to the ticker
  data_files <- dplyr::filter(market_data_log, grepl(tickers[i],ticker))
  # build a dat aframe to commit
  # read in the first data file
  all_data <- read_csv(paste("datalog", data_files$filename[1], sep = "/"), col_types = cols(.default = "d", date = "c"))
  # add timestamp ID and source
  all_data$TIMESTAMP <- data_files$timestamp[1]
  all_data$source <- data_files$source[1]
  # Append other files data if they exist
  if (nrow(data_files) >=2) {
      for (j in 2:nrow(data_files)-1) {
        table_data <- read_csv(paste("datalog", data_files$filename[j], sep = "/"), col_types = cols(.default = "d", date = "c"))
        table_data$TIMESTAMP <- data_files$timestamp[j]
        table_data$source <- data_files$source[j]
        all_data <- bind_rows(all_data, table_data)
      }
  
    }
  # Append the dataframe to the SQLite table
  dbWriteTable(db, table_name, all_data, append = TRUE)
  # Delete any duplicates, where a duplicate is a specific day from the same timestamp
  # NOTE: NO ERROR HANDLING FOR NEW COLUMNS. NEED TO BUILD THAT IN.
  clear_duplicates <- dbSendQuery(db, paste("DELETE FROM '", table_name,
                 "' WHERE rowid NOT IN (
                  SELECT MIN(rowid) 
                  FROM '", table_name, 
                 "' GROUP BY TIMESTAMP, date)", sep=""))
  dbClearResult(clear_duplicates)
  }



# Testing a SQL table
length(dbListTables(db))
dbListTables(db)
dbListFields(db, table_name)
sample <- dbGetQuery(db, paste('SELECT * FROM',table_name))
nrow(sample)
#dbGetQuery(db, paste("PRAGMA table_info(", "SNH_SJ",");"))
#View(dbGetQuery(db, paste("SELECT * FROM","SNH_SJ","WHERE date BETWEEN '2011-01-11' AND '2011-08-11'")))


######################################################################
# Building fundamental dataset

# PROBLEM - Different field of the samer ticker on the same day have different timestamps

fundamental_data_log <- dplyr::filter(data_log, grepl("ticker_fundamental_data", data_type))
fundamental_data_log$ISIN <- str_split_fixed(fundamental_data_log$data_label, "__", 4)[,2]
fundamental_data_log$ISIN <- tools::file_path_sans_ext(fundamental_data_log$ISIN)
fundamental_data_log$ISIN <- str_split_fixed(fundamental_data_log$ISIN, " ", 2)[,1]
fundamental_data_log$latest_date <- str_split_fixed(fundamental_data_log$data_label, "__", 4)[,1]
head(fundamental_data_log)
unique(fundamental_data_log$ISIN)


# Connec to database
db <- dbConnect(SQLite(), "datasets/fundamentals.sqlite")
ISINs <- unique(fundamental_data_log$ISIN)
for (i in 1:length(ISINs)) {
  # Define table name
  table_name <- str_replace_all(ISINs[i],"[[:punct:]\\s]+","_")
  # Get list of files relating to the ticker
  data_files <- dplyr::filter(fundamental_data_log, grepl(ISINs[i],ISIN))
  # build a dat aframe to commit
  # read in the first data file
  all_data <- read_csv(paste("datalog", data_files$filename[1], sep = "/"), col_types = cols(.default = "d", date = "c"))
  # add timestamp ID and source
  all_data$TIMESTAMP <- data_files$timestamp[1]
  all_data$source <- data_files$source[1]
  # Append other files data if they exist
  if (nrow(data_files) >=2) {
    for (j in 2:nrow(data_files)-1) {
      table_data <- read_csv(paste("datalog", data_files$filename[j], sep = "/"), col_types = cols(.default = "d", date = "c"))
      table_data$TIMESTAMP <- data_files$timestamp[j]
      table_data$source <- data_files$source[j]
      all_data <- bind_rows(all_data, table_data)
    }
    
  }
  # Append the dataframe to the SQLite table
  dbWriteTable(db, table_name, all_data, append = TRUE)
  # Delete any duplicates, where a duplicate is a specific day from the same timestamp
  # NOTE: NO ERROR HANDLING FOR NEW COLUMNS. NEED TO BUILD THAT IN.
  clear_duplicates <- dbSendQuery(db, paste("DELETE FROM '", table_name,
                                            "' WHERE rowid NOT IN (
                                            SELECT MIN(rowid) 
                                            FROM '", table_name, 
                                            "' GROUP BY TIMESTAMP, date)", sep=""))
  dbClearResult(clear_duplicates)
}



# Testing a SQL table
length(dbListTables(db))
dbListTables(db)
dbListFields(db, "GB00B17BBQ50")
sample <- dbGetQuery(db, paste('SELECT * FROM',table_name))
nrow(sample)
#dbGetQuery(db, paste("PRAGMA table_info(", "SNH_SJ",");"))
#View(dbGetQuery(db, paste("SELECT * FROM","SNH_SJ","WHERE date BETWEEN '2011-01-11' AND '2011-08-11'")))


#########################################################################
# Building metadata dataset
metadata_log <- dplyr::filter(data_log, grepl("metadata_array", data_type))
metadata_log$latest_date <- tools::file_path_sans_ext(metadata_log$data_label)
head(metadata_log)


