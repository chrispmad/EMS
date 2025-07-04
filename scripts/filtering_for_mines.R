library(DBI)

#con <- dbConnect(RSQLite::SQLite(), "J:/2 SCIENCE - Invasives/GENERAL/Operations/EnvironmentalMonitoringSystemDatabase/EMS.sqlite")
con <- dbConnect(RSQLite::SQLite(), "output/EMS.sqlite")

print(dbGetInfo(con))
print(dbListObjects(con))

query <- "PRAGMA table_info(results);"
result <- dbSendQuery(con, query)
column_names <- fetch(result, n = -1)
column_names
dbClearResult(result)


# get the unique location types from the db
query <- "SELECT DISTINCT location_type FROM results;"
result <- dbSendQuery(con, query)
location_types <- fetch(result, n = -1)
location_types
dbClearResult(result)

# get unique permits from the db
query <- "SELECT DISTINCT permit FROM results;"
result <- dbSendQuery(con, query)
permits <- fetch(result, n = -1)
permits
dbClearResult(result)

# get the unique LOCATION_PURPOSE from the db
query <- "SELECT DISTINCT location_purpose FROM results;"
result <- dbSendQuery(con, query)
location_purposes <- fetch(result, n = -1)
location_purposes
dbClearResult(result)

# select all monitoring locations that metions mines or mills not case sensitive
query <- "SELECT * FROM results WHERE monitoring_location LIKE '%mine%' OR monitoring_location LIKE '%mill%' OR monitoring_location LIKE '%MINE%' OR monitoring_location LIKE '%MILL%';"
result <- dbSendQuery(con, query)
mines_and_mills <- fetch(result, n = -1)
mines_and_mills
dbClearResult(result)

unique(mines_and_mills$MONITORING_LOCATION)
unique(mines_and_mills$PARAMETER)

library(dplyr)
#count the number of unique parameter

 mines_and_mills |> 
  group_by(PARAMETER) |> 
  summarise(count = n()) |> 
  arrange(desc(count))

mines_and_mills = mines_and_mills |> 
  dplyr::mutate(COLLECTION_DATE = as.Date(COLLECTION_DATE, format = "%Y-%m-%d")) |> 
  dplyr::filter(COLLECTION_DATE >= "2020-01-01")

unique_parameters<-mines_and_mills |> 
  group_by(PARAMETER) |> 
  summarise(count = n()) |> 
  arrange(desc(count))

# get where phosphorus is mentioned in the parameter
phosphorus_parameters <- mines_and_mills |> 
  dplyr::filter(grepl("phosphorus", PARAMETER, ignore.case = TRUE))

unique(mines_and_mills$UNIT)

unique_units<-mines_and_mills |> 
  group_by(UNIT) |> 
  summarise(count = n()) |> 
  arrange(desc(count))

# only take where the units are mg/L
mines_and_mills <- mines_and_mills |> 
  dplyr::filter(UNIT == "mg/L")

#spatialise with sf
library(sf)
mines_and_mills_sf <- mines_and_mills |> 
  dplyr::mutate(LATITUDE = as.numeric(LATITUDE), LONGITUDE = as.numeric(LONGITUDE)) |> 
  dplyr::filter(!is.na(LATITUDE) & !is.na(LONGITUDE)) |> 
  sf::st_as_sf(coords = c("LONGITUDE", "LATITUDE"), crs = 4326)

# summarise result by monitoring_location and year
mines_and_mills_summary <- mines_and_mills_sf |> 
  dplyr::group_by(MONITORING_LOCATION, month = format(COLLECTION_DATE, "%m")) |> 
  dplyr::summarise(RESULT = mean(RESULT, na.rm = TRUE), .groups = 'drop')

bc<-bcmaps::bc_bound()

library(ggplot2)
library(viridis)
ggplot()+
  geom_sf(data = bc, fill = "lightgrey", color = "black") +
  geom_sf(data = mines_and_mills_summary, aes(color = RESULT)) +
  theme_minimal() +
  labs(title = "Mines and Mills",
       x = "Lon",
       y = "Lat",
       color = "Parameter") +
  scale_color_viridis_c(option = "D") +
  facet_wrap(~ month, ncol = 3)+
  theme(legend.position = "bottom")








