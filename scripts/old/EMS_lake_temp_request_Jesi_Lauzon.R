library(sqldf)
library(data.table)
library(readr)
library(stringr)
library(dplyr)
library(lubridate)
library(ggplot2)
library(bcdata)

db_filepath = "output/EMS.sqlite"
new_data_chunk_filepath = 'data/recent_ems_data_chunk.csv'
onedrive_path = paste0(stringr::str_extract(getwd(),".*(?=Downloads/)"),"OneDrive - Government of BC/data")

bc_cities = bcmaps::bc_cities()
bc_cities = bc_cities[bc_cities$NAME == "Prince George",]
# Snag the natural resource region that contains Prince George, where Jesi 
# works.
reg = bcmaps::nr_regions()
reg = reg[str_detect(reg$REGION_NAME,"Omineca"),]

con = dbConnect(RSQLite::SQLite(), 'output/EMS.sqlite')

temps = dbGetQuery(con, "select * from results where PARAMETER like 'Temperature'")

temps = temps |> 
  dplyr::filter(!is.na(LATITUDE)) #|> 
  # dplyr::filter(LOCATION_TYPE %in% c("DITCH OR CULVERT","LAKE OR POND","MONITORING WELL","PROVINCIAL OBS WELL NETWORK","RIVER,STREAM OR CREEK"))

temps_sf = sf::st_as_sf(temps, coords = c("LONGITUDE","LATITUDE"), crs = 4326)

# Convert collection start field to date type and 
# Filter dates for only recent dates? E.g. last 10 years of temp data
temps_2015_plus = temps_sf |> 
  dplyr::mutate(COLLECTION_START = lubridate::ymd_hms(COLLECTION_START)) |> 
  dplyr::filter(lubridate::year(COLLECTION_START) >= 2015)

# Filter for just spring/summer (months from March to September)
temps_2015_plus = temps_2015_plus |> 
  dplyr::filter(lubridate::month(COLLECTION_START) >= 3 & lubridate::month(COLLECTION_START) <= 9)

# Filter for just those 3 lakes or fraser river portion
lakes = bcdc_query_geodata('freshwater-atlas-lakes') |> 
  filter(GNIS_NAME_1 %in% c("Stuart Lake","Whitefish Lake","Bowron Lake")) |> 
  collect()

lakes_b = lakes |> 
  sf::st_buffer(50000)

# If we have multiples, just use the lake that's in the Omineca region.
lakes_b = lakes_b |> 
  dplyr::select(GNIS_NAME_1,WATERSHED_GROUP_ID) |> 
  dplyr::add_count(GNIS_NAME_1) |> 
  dplyr::mutate(keep_me = dplyr::case_when(
    n == 1 ~ "keep",
    T ~ as.character(sf::st_intersects(geometry,reg,sparse = T))
  )) |> 
  dplyr::filter(keep_me != "integer(0)")

stone_creek_campground = data.frame(lat = 53.63738606710232, lng = -122.66467920298419) |> 
  sf::st_as_sf(coords = c("lng","lat"), crs = 4326) |> 
  sf::st_transform(3005) |> 
  sf::st_buffer(30000)

rivers = bcdc_query_geodata('freshwater-atlas-rivers') |> 
  filter(GNIS_NAME_1 == "Fraser River") |> 
  filter(INTERSECTS(stone_creek_campground)) |> 
  collect() |> 
  sf::st_zm() |> 
  dplyr::group_by(GNIS_NAME_1) |> 
  dplyr::summarise()

wb_filters = dplyr::bind_rows(lakes_b, rivers) |> 
  sf::st_transform(4326) |> 
  dplyr::select(waterbody_name = GNIS_NAME_1)

# ggplot() + geom_sf(data = wb_filters, aes(fill = waterbody_name)) + 
#   geom_sf(data = temps_2015_plus)

# Now use these waterbody polygons to query the temp records we filtered above for specific years, seasons etc!
temps_2015_plus_relevant = temps_2015_plus |> 
  sf::st_join(wb_filters) |> 
  dplyr::filter(!is.na(waterbody_name)) 

temps_2015_plus_relevant |> 
  sf::st_drop_geometry() |> 
  dplyr::count(waterbody_name)

g = ggplot() +
  geom_sf(data = lakes, aes(color = GNIS_NAME_1)) + 
  geom_sf(data = wb_filters, aes(color = waterbody_name), fill = 'transparent') + 
  geom_sf(data = temps_2015_plus_relevant) + 
  geom_sf(data = bc_cities) +
  geom_sf_label(data = bc_cities, aes(label = NAME)) + 
  ggspatial::annotation_scale() + 
  labs(title = "EMS records for 2015 - present, March to September",
       subtitle = "Searched 30km around lakes and in Fraser River near \nStone Creek Campground")

g

ggsave(filename = "request_data_outputs/Jesi_Lauzon_Data_Request.png",
       g,
       width = 8, height = 8)

temps_2015_plus_relevant_tbl = temps_2015_plus_relevant |> 
  dplyr::mutate(latitude = sf::st_coordinates(geometry)[,2],
                longitude = sf::st_coordinates(geometry)[,1]) |> 
  sf::st_drop_geometry() |> 
  as_tibble()

temps_trim = temps_2015_plus_relevant_tbl |> 
  dplyr::select(EMS_ID,MONITORING_LOCATION,COLLECTION_START,PARAMETER,RESULT,waterbody_name,latitude,longitude) |> 
  dplyr::mutate(YEAR = lubridate::year(COLLECTION_START))

openxlsx::write.xlsx(temps_trim, "request_data_outputs/Jesi_Lauzon_Data_Request.xlsx")

# # Trim down columns
# 
# temps_trim |> 
#   dplyr::mutate(YEAR = factor(YEAR, levels = c(min(temps_trim$YEAR):max(temps_trim$YEAR)))) |> 
#   ggplot(aes(x = COLLECTION_START, y = RESULT, col = waterbody_name)) + 
#   geom_point()
