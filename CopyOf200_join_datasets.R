source('011_utils.R')
data_log <- convert_datalog_to_dataframe()
head(data_log)

unique(data_log$source)
unique(data_log$data_type)
unique(data_log$data_label)

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
db <- dbConnect(SQLite(), "datasets/marketdata.sqlite")
tickers <- unique(market_data_log$ticker)
for (i in 1:length(tickers)) {
  # Defin table name
  table_name <- str_replace_all(tickers[i],"[[:punct:]\\s]+","_")
  # Check if a table exists
  table_attributes <- dbGetQuery(db, paste("PRAGMA table_info(", table_name,");"))
  # If it does not, create it
  if(length(table_attributes$name) == 0) {
    create_table <- dbSendQuery(conn=db,
                                paste("CREATE TABLE", table_name,
                                      "(TIMESTAMP INT,
                                      date DATETIME,
                                      PRIMARY KEY (TIMESTAMP))
                                      "))
    dbClearResult(create_table)}
  # Get list of files relating to the ticker
  data_files <- dplyr::filter(market_data_log, grepl(tickers[i],ticker))
  # build a dat aframe to commit
  # read in the first data file
  all_data <- read_csv(paste("datalog", data_files$filename[1], sep = "/"), col_types = cols(.default = "d", date = "c"))
  # add timestamp ID and source
  all_data$TIMESTAMP <- as.numeric(data_files$timestamp[1])
  all_data$source <- data_files$source[1]
  # Append other files data if they exist
  if (nrow(data_files) >=2) {
      for (j in 2:nrow(data_files)-1) {
        table_data <- read_csv(paste("datalog", data_files$filename[j], sep = "/"), col_types = cols(.default = "d", date = "c"))
        table_data$TIMESTAMP <- as.numeric(data_files$timestamp[j])
        table_data$source <- data_files$source[j]
        all_data <- bind_rows(all_data, table_data)
      }
  
      }
  # Check that all columns are present in DB, if they are not add them. Maybe not necessary
  source_columns <- colnames(all_data)
  table_columns <- dbListFields(db, table_name)
  new_columns <- source_columns[which(!source_columns %in% table_columns)]
  #dbGetQuery(db, paste("PRAGMA table_info(", table_name,");"))
  for (column in new_columns){
       dbGetQuery(db, paste("ALTER TABLE",table_name,"ADD COLUMN",column))
  }
  #dbGetQuery(db, paste("PRAGMA table_info(", table_name,");"))
  
    
  dbSendQuery(db, paste("UPDATE",table_name,"" ))
  }

str(all_data)

length(dbListFields(db, data_source))
length(dbListTables(db))
dbListTables(db)
View(dbGetQuery(db, 'SELECT * FROM SNH_SJ'))
dbGetQuery(db, paste("PRAGMA table_info(", "SNH_SJ",");"))
View(dbGetQuery(db, paste("SELECT * FROM","SNH_SJ","WHERE date BETWEEN '2011-01-11' AND '2011-08-11'")))

str(all_data)
# NOTE TO RIAZ: LOOKS LIKE YOU HAVE TO USE CHARACTERS STRINGS FOR DATES WITH SQLITE

colnames(all_data)
dbListFields(db, table_name)


dbGetQuery(db, paste("SELECT * FROM", table))

dbListTables(db)


############################3

# Building fundamental dataset
fundamental_data_log <- dplyr::filter(data_log, grepl("ticker_fundamental_data", data_type))
fundamental_data_log$ISIN <- str_split_fixed(fundamental_data_log$data_label, "__", 4)[,2]
fundamental_data_log$ISIN <- tools::file_path_sans_ext(fundamental_data_log$ISIN)
fundamental_data_log$ISIN <- str_split_fixed(fundamental_data_log$ISIN, " ", 2)[,1]
fundamental_data_log$latest_date <- str_split_fixed(fundamental_data_log$data_label, "__", 4)[,1]
head(fundamental_data_log)
unique(fundamental_data_log$ISIN)

# Building metadata dataset
metadata_log <- dplyr::filter(data_log, grepl("metadata_array", data_type))
metadata_log$latest_date <- tools::file_path_sans_ext(metadata_log$data_label)
head(metadata_log)


