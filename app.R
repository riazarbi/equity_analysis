library(shiny)
library(tidyverse)
###################################################################
# Get working directory
working_directory <- getwd()

# Define directory paths
dimensions_directory <- file.path(working_directory, "dimensions")
data_directory <- file.path(working_directory, "datalog")

###################################################################
# Define functions

### Filter log directory files
get_datalog <- function(timestamp_days_age_limit, 
                          source_substring, 
                          data_type_substring, 
                          data_label_substring) 
{  
  file_list <- list.files(data_directory)
  data_log <- as.data.frame(str_split_fixed(file_list, "__", 4), stringsAsFactors=FALSE)
  colnames(data_log) <- c("timestamp", "source", "data_type", "data_label")
  data_log$filename <- file_list
  
  timestamp_days_age_limit <- timestamp_days_age_limit * 8640000000
  timestamp_days_age_limit <- as.numeric(as.POSIXct(Sys.time()))*10^5 - timestamp_days_age_limit
  
  data_log <- dplyr::filter(data_log, timestamp >= timestamp_days_age_limit)
  data_log <- dplyr::filter(data_log, grepl(source_substring,source))
  data_log <- dplyr::filter(data_log, grepl(data_type_substring, data_type))
  data_log <- dplyr::filter(data_log, grepl(data_label_substring, data_label))
  return(data_log)
}

datalog <- (get_datalog(1000, "*", "*", "20180303"))

# Define timestamp range
now <- as.numeric(as.POSIXct(Sys.time()))*10^5
max_log_age <- as.integer((now - as.numeric(max(datalog$timestamp)))/8640000000)
min_log_age <- as.integer((now - as.numeric(min(datalog$timestamp)))/8640000000)+10

# Define data source range
datasource <- unique(datalog$source)
datasource

# Define data source range
datatype <- as.vector(unique(datalog$data_type))
str(datatype)

# Define data source range
datalabel <- as.vector(unique(datalog$data_label))
str(datalabel)

####################################################################
# Define UI for slider demo app ----
ui <- fluidPage(
  
  # App title ----
  titlePanel("Datalog Explorer"),
  
  # Sidebar layout with input and output definitions ----
  sidebarLayout(
    
    # Sidebar to demonstrate various slider options ----
    sidebarPanel(
      
      # Input: Simple integer interval ----
      sliderInput("log_age", "Max Age (Days):",
                  min = max_log_age, max = min_log_age,
                  value = 10000, step = 1),
      
      selectInput("source", "Source:", 
                  datasource),
      
      # Input: Decimal interval with step value ----
      selectInput("data_type", "Data Type:",
                  datatype),
      
      # Input: Specification of range within an interval ----
      selectInput("data_label", "Data Label:",
                  datalabel)
      

      
    ),
    
    # Main panel for displaying outputs ----
    mainPanel(
      
      # Output: Table summarizing the values entered ----
      tableOutput("values")
      
    )
  )
)


####################################################################################
# Define server logic for slider examples ----
server <- function(input, output) {
  
  # Reactive expression to create data frame of all input values ----
  filtered_datalog <- get_datalog(input$log_age, input$source, input$data_type,input$data_label)$filename
  
  # Show the values in an HTML table ----
  output$values <- renderTable({
    filtered_datalog
  })
  
}

####################################################################################
shinyApp(ui, server)