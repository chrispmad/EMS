library(DBI)
library(openxlsx)
library(dplyr)

#con <- dbConnect(RSQLite::SQLite(), "J:/2 SCIENCE - Invasives/GENERAL/Operations/EnvironmentalMonitoringSystemDatabase/EMS.sqlite")
con <- dbConnect(RSQLite::SQLite(), "output/EMS.sqlite")

query <- "PRAGMA table_info(results);"
result <- dbSendQuery(con, query)
column_names <- fetch(result, n = -1)
column_names
dbClearResult(result)


query <- "SELECT DISTINCT location_type FROM results;"
result <- dbSendQuery(con, query)
location_types <- fetch(result, n = -1)
location_types
dbClearResult(result)

query <- "
SELECT parameter, COUNT(*) AS n
FROM results
GROUP BY parameter
ORDER BY n DESC;
"

result <- dbSendQuery(con, query)
param_counts <- fetch(result, n = -1)
param_counts
dbClearResult(result)

# Pull all relevant columns
all_results <- dbGetQuery(con, "SELECT parameter, COLLECTION_DATE FROM results;")

# Convert collection_date to Date
all_results <- all_results %>%
  mutate(COLLECTION_DATE = lubridate::ymd(COLLECTION_DATE))

# Summarize per parameter
param_summary <- all_results %>%
  group_by(PARAMETER) %>%
  summarise(
    n = n(),
    start_date = min(COLLECTION_DATE, na.rm = TRUE),
    end_date   = max(COLLECTION_DATE, na.rm = TRUE)
  ) %>%
  arrange(desc(n))

param_summary


# Create a new workbook
wb <- createWorkbook()

# Add sheets
addWorksheet(wb, "Parameter Counts")
addWorksheet(wb, "Location Types")

# Write data frames
writeData(wb, sheet = "Parameter Counts", x = param_summary)
writeData(wb, sheet = "Location Types", x = location_types)

# Save workbook
saveWorkbook(wb, "./output/data_summary.xlsx", overwrite = TRUE)

