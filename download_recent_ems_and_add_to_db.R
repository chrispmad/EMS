# Title: Download Environmental Monitoring System data
#
# Date: 2024-07-31
#
# Author(s): Chris Madsen (chris.madsen@gov.bc.ca)
# 
# Description: This script downloads the two massive .csv files from the BC Data Catalogue's entry
# for the Environmental Monitoring System database (https://catalogue.data.gov.bc.ca/dataset/bc-environmental-monitoring-system-results)

library(sqldf)
library(data.table)
library(readr)
library(stringr)
library(dplyr)
library(lubridate)
library(ggplot2)

# Turn off scientific notation. This is useful for the date fields, as they are 
# stored as 13-digit numbers that otherwise get converted to scientific notation,
# which is not very helpful for converting to date data type.
options(scipen = 999)

# Filepaths
db_filepath = "output/EMS.sqlite"
new_data_chunk_filepath = 'data/recent_ems_data_chunk.csv'
onedrive_path = paste0(stringr::str_extract(getwd(),".*(?=Downloads/)"),"OneDrive - Government of BC/data")

options(timeout = max(100000, getOption("timeout")))

# Download recent EMS data file.
if(!file.exists('data/recent_ems_data_chunk.csv')){
  download.file(
    url = 'https://pub.data.gov.bc.ca/datasets/949f2233-9612-4b06-92a9-903e817da659/ems_sample_results_current_expanded.csv',
    destfile = new_data_chunk_filepath
  )
}

# Read in new data chunk.
rec_d <- data.table::fread(new_data_chunk_filepath)

# Convert the COLLECTION_START column from scientific notation to character.
rec_d = rec_d[,COLLECTION_START := as.character(COLLECTION_START)]

# Convert date columns from numeric to dates.
# Pull out year, month, day for start and end of collection.
rec_d$COL_START_YEAR = str_extract(rec_d$COLLECTION_START,"^[0-9]{4}")
rec_d$COL_START_MONTH = str_remove(str_extract(rec_d$COLLECTION_START,"[0-9]{6}"),"^[0-9]{4}")
rec_d$COL_START_DAY = str_remove(str_extract(rec_d$COLLECTION_START,"[0-9]{8}"),"^[0-9]{6}")

# Make proper date fields.
rec_d$COLLECTION_DATE = lubridate::ymd(paste(rec_d$COL_START_YEAR,rec_d$COL_START_MONTH,rec_d$COL_START_DAY, sep = "-"))

# Drop those intermediate year, month and day columns.
rec_d$COL_START_DAY = NULL; rec_d$COL_START_MONTH = NULL; rec_d$COL_START_YEAR = NULL; 

# What is the most recent and the oldest data in the recent data chunk?
min(rec_d$COLLECTION_DATE,na.rm=T)
max(rec_d$COLLECTION_DATE,na.rm=T)

# Is our local EMS database empty/missing? If so, transfer from OneDrive
if(!file.exists('output/EMS.sqlite')){
  file.copy(from = paste0(onedrive_path,'/CNF/EMS.sqlite'),
            to = 'output/EMS.sqlite')
}
if(file.size('output/EMS.sqlite') == 0){
  file.copy(from = paste0(onedrive_path,'/CNF/EMS.sqlite'),
            to = 'output/EMS.sqlite',
            overwrite = T)
}
# Identify the most recent date in the database.
con = dbConnect(RSQLite::SQLite(), 'output/EMS.sqlite')
# con = dbConnect(RSQLite::SQLite(), db_filepath)

cat("\nQuerying database to find most recent collection date...")

all_dates_from_db =  DBI::dbGetQuery(con,
                                     "SELECT COLLECTION_DATE FROM results;") |> 
  mutate(COLLECTION_DATE = lubridate::ymd(COLLECTION_DATE)) |> 
  pull(COLLECTION_DATE)

most_recent_date = max(all_dates_from_db,na.rm=T)

# Filter the recent data chunk to just include dates at or after the most recent
# collection date in the database.

cat("\nFiltering recent data file to include dates at or after this most recent date.")

rec_d_f = rec_d[COLLECTION_DATE >= most_recent_date | is.na(COLLECTION_DATE),]

# Arrange by date; oldest date first.
rec_d_f = rec_d_f |>
  arrange(COLLECTION_DATE)

# Are there any records that are already present in the database for the 
# most recent date, and are also present in the new data chunk to be uploaded?
# Remove them, if so.

cat("\nPulling out all records from the database for that most recent date...")

# Pull out all records from the database for that most recent date.
recs_in_db_for_date = DBI::dbGetQuery(con,
                                      paste0("select * from results where COLLECTION_DATE like '",most_recent_date,"';")) |> 
  tidyr::as_tibble()
cat("\nRecords pulled.")

# Change data type for COLLECTION_START from string to date type - this is temporary.
recs_in_db_for_date = recs_in_db_for_date |> 
  dplyr::mutate(COLLECTION_DATE = lubridate::ymd(COLLECTION_DATE))

recs_in_db_for_date = data.table::as.data.table(recs_in_db_for_date)

# Perform an anti-join to make sure we are not going to add duplicate rows to the 
# database.

rec_all_to_add = rec_d_f[!recs_in_db_for_date, on = .(EMS_ID,MONITORING_LOCATION,COLLECTION_DATE,LOCATION_TYPE,ANALYZING_AGENCY,UNIT)]

# Save a table of parameters so that we know what we could filter this dataset by
# in the future.
params_in_db = rec_all_to_add |> 
  dplyr::count(PARAMETER, sort = T, name = 'number_rows_in_recent_data_chunk') |> 
  mutate(date_range_of_recent_data_chunk = paste0(min(rec_all_to_add$COLLECTION_DATE),' - ',max(rec_all_to_add$COLLECTION_DATE)))

openxlsx::write.xlsx(params_in_db, 'output/parameters_in_database.xlsx')

gc()

# Make sure that the date column is actually just strings - better for database.
rec_all_to_add = rec_all_to_add[, COLLECTION_DATE := as.character(COLLECTION_DATE),]

cat(paste0("Number of new rows to write to database: ",nrow(rec_all_to_add)))

# Check - are our data types the same in this new data compared to the data already
# present in the database?
names(recs_in_db_for_date) == names(rec_all_to_add)
unlist(recs_in_db_for_date |> lapply(typeof)) == unlist(rec_all_to_add |> lapply(typeof))

# Now it's time to add the new data to our database!
col_type_comp = tidyr::tibble(colname = names(recs_in_db_for_date)) |> 
  dplyr::rowwise() |> 
  dplyr::mutate(col_type = typeof(recs_in_db_for_date[[colname]])) |> 
  dplyr::ungroup() |> 
  dplyr::left_join(
    tidyr::tibble(colname = names(rec_all_to_add)) |> 
      dplyr::rowwise() |> 
      dplyr::mutate(new_col_type = typeof(rec_all_to_add[[colname]])) |> 
      dplyr::ungroup()
  ) |> 
  dplyr::filter(col_type != new_col_type)

# for(i in 1:ncol(rec_all_to_add)){
#   new_rec_type = typeof(rec_all_to_add[[i]])
#   old_rec_type = typeof(recs_in_db_for_date[[i]])
#   if(new_rec_type != old_rec_type){
#     rec_all_to_add[[i]] = 
#   }
# }
dbWriteTable(conn = con, "results", rec_all_to_add, row.names = FALSE, append = TRUE)

# Let's perform a check that the new data was successfully written to the database.
record_to_check = rec_all_to_add |> arrange(desc(COLLECTION_START)) |> slice(1)

records_to_add = rec_all_to_add |> dplyr::filter(EMS_ID == record_to_check$EMS_ID,
                                                 MONITORING_LOCATION == record_to_check$MONITORING_LOCATION,
                                                 COLLECTION_DATE == record_to_check$COLLECTION_DATE)

cat("\nQuerying database to test that our upload has worked...")
upload_test = DBI::dbGetQuery(con,
                paste0("select * from results where EMS_ID like '",record_to_check$EMS_ID,
                       "' and MONITORING_LOCATION like '",record_to_check$MONITORING_LOCATION,
                       "' and COLLECTION_DATE like '",record_to_check$COLLECTION_DATE,"';")) |> 
  as_tibble()
  
if(nrow(records_to_add) == nrow(upload_test)){
  upload = 'successful'
} else {
  upload = 'not successful'
}

cat(paste0("Upload test: ",upload))

# # Filter database by things like date (could be a combo of year, month etc.)
# dbGetQuery(con, "select * from results where strftime('%Y', COLLECTION_DATE) == '2024';")
# 
# # Filter database by parameter type.
# dbGetQuery(con, "select * from results where PARAMETER like 'pH|Phosphorus Total';")

DBI::dbDisconnect(con)

if(upload == 'successful'){
  file.remove(new_data_chunk_filepath)
  # file.copy(from = 'output/EMS.sqlite', to = paste0(onedrive_path,"/CNF/EMS.sqlite"), overwrite = T)
  # print("Replaced the older version of the EMS database on the OneDrive path in data/CNF folder!")
}
