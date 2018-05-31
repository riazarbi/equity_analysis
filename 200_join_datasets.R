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


library(RSQlite)
db <- dbConnect(SQLite(), "marketdata.sqlite")

tickers <- unique(market_data_log$ticker)
for (i in 1:length(tickers)) {
  table_name <- str_replace_all(tickers[i],"[[:punct:]\\s]+","_")
  data_files <- dplyr::filter(market_data_log, grepl(tickers[i],ticker))
  all_data <- read_csv(paste("datalog", data_files$filename[1], sep = "/"), col_types = cols(.default = "d", date = "D"))
  all_data$TIMESTAMP <- data_files$timestamp[1]
  all_data$source <- data_files$source[1]
  
  if (nrow(data_files) >=2) {
      for (j in 2:nrow(data_files)-1) {
        table_data <- read_csv(paste("datalog", data_files$filename[j], sep = "/"), col_types = cols(.default = "d", date = "D"))
        table_data$TIMESTAMP <- data_files$timestamp[j]
        table_data$source <- data_files$source[j]
        all_data <- bind_rows(all_data, table_data)
      }
  #assign(table_name, all_data)
  
    }
  dbWriteTable(db, table_name, all_data, overwrite=TRUE)
  }

length(dbListFields(db, data_source))
length(dbListTables(db))

View(dbGetQuery(db, 'SELECT * FROM TEST'))
str(all_data)
# NOTE TO RIAZ: LOOKS LIKE YOU HAVE TO USE CHARACTERS STRINGS FOR DATES WITH SQLITE

table <- "TEST"
dbSendQuery(conn=db,
           paste("CREATE TABLE",table,"
           (TIMESTAMP DATETIME,
           date DATETIME,
           PRIMARY KEY (TIMESTAMP))
           "))
           
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


