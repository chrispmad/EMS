# Convert historic data chunk (huge) to a database.
library(sqldf)
library(data.table)
library(readr)
library(stringr)
library(dplyr)

# Filepaths
csv_filepath = "data/ems_sample_results_historic_expanded.csv"
db_filepath = "output/EMS.sqlite"

# # Establish connection to (new) database
# if(file.exists(db_filepath)){
#   try(dbDisconnect(con))
#   # file.remove(db_filepath)
# }

con <- dbConnect(RSQLite::SQLite(), db_filepath)

# Read in recent data chunk to get column names and data types.
d = read.csv('data/recent_ems_data_chunk.csv')

d_col_types = data.frame(col_name = names(d),
                         type = as.vector(sapply(d, typeof))) |> 
  add_row(col_name = 'COLLECTION_DATE', type = 'DATE')

# Define SQL statement to create a table
d_col_types_sql = d_col_types |> 
  # dplyr::add_row(col_name = "UNIQ_ID", type = "character") |>
  dplyr::mutate(key_status = case_when(
    col_name %in% c('UNIQ_ID') ~ "PRIMARY KEY",
    col_name %in% c('EMS_ID','MONITORING_LOCATION','LOCATION_TYPE','PARAMETER','UNIT','COLLECTION_DATE') ~ "KEY",
    T ~ ""
  )) |> 
  dplyr::reframe(a = paste0(col_name," ",stringr::str_to_upper(type)," ",key_status))

sql = paste0("CREATE TABLE IF NOT EXISTS results (
       ",paste0(d_col_types_sql$a,collapse = ",\n"),
             ")")

# Execute the SQL statement
dbExecute(con, sql)
DBI::dbListTables(con)

DBI::dbListFields(con, 'results')

# dbWriteTable(conn = con, "results", d, row.names = FALSE, append = TRUE)

# Set up parameters for our loop. This will cycle through the giant historic dataset (a CSV file)
# and add it by chunks into the SQLITE database.
chunk_size <- 1000000
start_row <- 1
chunk_num <- 1
continue = TRUE
i <- 0

while(continue){

  i = i + 1
  
  print(paste0('Iteration ',i,': rows ',1 + (i-1)*chunk_size,' to ', (i)*chunk_size))
  
  d_c = suppressMessages(read_csv(file = csv_filepath, skip = 1 + (i-1)*chunk_size, n_max = chunk_size, col_names = F))
  
  if(nrow(d_c) == 0) {
    continue = FALSE
    print(paste0("Finished at ",Sys.time()))
    dbDisconnect(con)
    break
  } else {
    
    names(d_c) <- d_col_types$col_name
    
    # Pull out year, month, day for start and end of collection.
    d_c$COL_START_YEAR = str_extract(d_c$COLLECTION_START,"^[0-9]{4}")
    d_c$COL_START_MONTH = str_remove(str_extract(d_c$COLLECTION_START,"[0-9]{6}"),"^[0-9]{4}")
    d_c$COL_START_DAY = str_remove(str_extract(d_c$COLLECTION_START,"[0-9]{8}"),"^[0-9]{6}")
    
    # Make proper date fields.
    # d_c$COLLECTION_START = lubridate::ymd(paste(d_c$COL_START_YEAR,d_c$COL_START_MONTH,d_c$COL_START_DAY, sep = "-"))
    # d_c$COLLECTION_END = lubridate::ymd(paste(d_c$COL_END_YEAR,d_c$COL_END_MONTH,d_c$COL_END_DAY, sep = "-"))
    d_c$COLLECTION_DATE = (paste(d_c$COL_START_YEAR,d_c$COL_START_MONTH,d_c$COL_START_DAY, sep = "-"))

    # Drop those intermediate year, month and day columns.
    d_c$COL_START_DAY = NULL; d_c$COL_START_MONTH = NULL; d_c$COL_START_YEAR = NULL; 

    dbWriteTable(conn = con, "results", d_c, row.names = FALSE, append = TRUE)
    
  }
}

# Close the database connection
# dbDisconnect(con)
