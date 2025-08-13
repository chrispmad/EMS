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

 # Just look at dissolved oxygen and phosphorous (for now??)
 mines_and_mills_DO_phos = mines_and_mills |> 
   dplyr::filter(stringr::str_detect(PARAMETER,"([oO]xygen|[pP]hospho)")) |> 
   dplyr::filter(!stringr::str_detect(PARAMETER,"Demand"))
 
 mines_and_mills_DO_phos |> 
   dplyr::count(PARAMETER, sort = T)
 
 # Drop parameters that have very few records - these are usually oddballs.
 mines_and_mills_DO_phos = mines_and_mills_DO_phos |> 
   dplyr::add_count(PARAMETER) |> 
   dplyr::filter(n > 100)
 
 mines_and_mills_DO_phos |> 
   dplyr::count(PARAMETER, sort = T)
 
 # What's distribution of time like?
 mines_and_mills_DO_phos = mines_and_mills_DO_phos |> 
   dplyr::mutate(COLLECTION_DATE = as.Date(COLLECTION_DATE, format = "%Y-%m-%d")) |> 
   dplyr::filter(!is.na(COLLECTION_DATE))
 
 mines_and_mills_DO_phos |> 
   dplyr::mutate(the_year = lubridate::year(COLLECTION_DATE)) |> 
   dplyr::count(the_year, sort = T)
 # Nice spread!
 
# And units?
 mines_and_mills_DO_phos |> 
   dplyr::count(UNIT,sort=T)
 
 mines_and_mills_DO_phos = mines_and_mills_DO_phos |> 
   dplyr::filter(UNIT == 'mg/L')

 # summarise mean values per parameter by subwatershed.
 subw = sf::read_sf("//SFP.IDIR.BCGOV/S140/S40203/WFC AEB/General/2 SCIENCE - Invasives/AIS_R_Projects/CMadsen_Wdrive/shared_data_sets/subwatersheds_BC.shp")
 
 mines_and_mills_DO_phos = mines_and_mills_DO_phos |> 
   dplyr::mutate(PARAMETER = dplyr::case_when(
     stringr::str_detect(PARAMETER,"Oxygen") ~ "Dissolved Oxygen",
     stringr::str_detect(PARAMETER,"Phosph") ~ "Phosphorous",
     T ~ "Unknown"
   ))

 mines_and_mills_sf = mines_and_mills_DO_phos |> 
   dplyr::filter(!is.na(LONGITUDE)) |> 
   dplyr::group_by(COLLECTION_DATE,PARAMETER,LATITUDE,LONGITUDE) |> 
   dplyr::reframe(RESULT = mean(RESULT, na.rm=T)) |> 
   sf::st_as_sf(coords = c("LONGITUDE","LATITUDE"),
                crs = 4326) |> 
   dplyr::select(RESULT,PARAMETER,COLLECTION_DATE)
 
 DO_by_subw = mines_and_mills_sf |> 
   dplyr::filter(PARAMETER == "Dissolved Oxygen") |> 
   sf::st_transform(sf::st_crs(subw)) |> 
   sf::st_join(subw |> dplyr::select(WATERSHED_,WATERSHE_1)) |> 
   dplyr::group_by(WATERSHED_, WATERSHE_1, PARAMETER) |> 
   dplyr::reframe(RESULT = mean(RESULT,na.rm=T)) |> 
   dplyr::rename(DisOx = RESULT) |> 
   dplyr::select(-PARAMETER)
 
 phos_by_subw = mines_and_mills_sf |> 
   dplyr::filter(PARAMETER == "Phosphorous") |> 
   sf::st_transform(sf::st_crs(subw)) |> 
   sf::st_join(subw |> dplyr::select(WATERSHED_,WATERSHE_1)) |> 
   dplyr::group_by(WATERSHED_, WATERSHE_1, PARAMETER) |> 
   dplyr::reframe(RESULT = mean(RESULT,na.rm=T)) |> 
   dplyr::rename(Phos = RESULT) |> 
   dplyr::select(-PARAMETER)
 
 # Check for wild outliers.
 DO_by_subw = DO_by_subw |> 
   dplyr::filter(DisOx <= 15*mean(DisOx))
 
 phos_by_subw = phos_by_subw |> 
   dplyr::filter(Phos <= 15*mean(Phos))
 
 subw_w_dat = subw |> 
   dplyr::left_join(DO_by_subw) |> 
   dplyr::left_join(phos_by_subw)
 
 ggplot() + 
   geom_sf(data = bc, fill = 'grey') +
   geom_sf(data = subw_w_dat[!is.na(subw_w_dat$DisOx),],
           aes(fill = DisOx)) + 
   labs(fill = "Dissolved Oxygen \n(mg/L)",
        title = paste0(nrow(subw_w_dat[!is.na(subw_w_dat$DisOx),]),
                       " subwatersheds with dissolved oxygen data"),
        subtitle = "(Subset of EMS data that mention 'mines' or 'mills')")
 
 ggsave(filename = "output/EMS_mine_mills_dissolved_oxygen_data.jpg",
        width = 8, height = 8)
 
 ggplot() + 
   geom_sf(data = bc, fill = 'grey') +
   geom_sf(data = subw_w_dat[!is.na(subw_w_dat$Phos),],
           aes(fill = Phos)) + 
   labs(fill = "Phosphorous (mg/L)",
        title = paste0(nrow(subw_w_dat[!is.na(subw_w_dat$Phos),]),
                       " subwatersheds with phosphorous data"),
        subtitle = "(Subset of EMS data that mention 'mines' or 'mills')")
 
 ggsave(filename = "output/EMS_mine_mills_phosphorous_data.jpg",
        width = 8, height = 8)
 
 
 
 # - - - - - - - - - - - - - - - - - - - - - - - - - - #
 # RESUME JOHN'S CODE #
 
 
 
 
 # mines_and_mills = mines_and_mills |> 
 #  dplyr::mutate(COLLECTION_DATE = as.Date(COLLECTION_DATE, format = "%Y-%m-%d")) |> 
 #  dplyr::filter(COLLECTION_DATE >= "2020-01-01")

# unique_parameters<-mines_and_mills |> 
#   group_by(PARAMETER) |> 
#   summarise(count = n()) |> 
#   arrange(desc(count))

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

# Maybe just look at dissolved oxygen?
mines_and_mills_DO = mines_and_mills_sf |> 
  dplyr::filter(stringr::str_detect(PARAMETER,"[oO]xygen")) |> 
  dplyr::filter(stringr::str_detect(PARAMETER,"issolved"))


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

ggsave(filename = "output/")







