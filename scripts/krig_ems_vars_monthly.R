library(DBI)
library(RSQLite)
library(sf)
library(ggplot2)
library(dplyr)
library(bcmaps)
library(raster)
library(automap)
library(gstat)
library(stringr)
library(terra)
library(raster)

variable_to_search <- "Temperature"
#year_to_search<-2023
year_to_search<-"All"

path_to_user = str_extract(getwd(), ".*Users/[A-Z]+")
onedrive_path = paste0(path_to_user,"/OneDrive - Government of BC/data/CNF/")

bc_vect = terra::vect(sf::st_transform(bcmaps::bc_bound(),4326))
bc_vect_alb = terra::vect(sf::st_transform(bcmaps::bc_bound(),3005))

conn<-dbConnect(RSQLite::SQLite(),"../EMS/output/EMS.sqlite")

#dbListTables(conn) # list the table(s)

test1<-dbGetQuery(conn, paste0("select * from results where parameter like '%",variable_to_search,"%'"))

dbDisconnect(conn)
# pH <-dbGetQuery(conn, "select * from results where parameter like 'pH' and strftime('&Y', COLLECTION_DATE) >= 2022")

##date column - collection start and collection end - if need date specific - convert back to datetime

# class(test1)
# str(test1)

test1<-test1[!is.na(test1$LATITUDE),]
test1<-test1[!is.na(test1$LONGITUDE),]

tempSF<-st_as_sf(test1, coords = c("LONGITUDE","LATITUDE"), crs = 4326)

# ggplot()+
#   geom_sf(data = tempSF)

tempSF$COLLECTION_DATE<-as.Date(tempSF$COLLECTION_DATE)
#class(tempSF$COLLECTION_DATE[1])
#summary(tempSF$COLLECTION_DATE)

# temp2024<-tempSF[tempSF$COLLECTION_DATE >= paste0(as.character(year_to_search),"-01-01") &
#                    tempSF$COLLECTION_DATE < paste0(as.character(year_to_search+1),"-01-01"),] %>%
#           filter(!is.na(MONITORING_LOCATION))
temp2024<-tempSF

ggplot() + geom_histogram(data = temp2024, aes(COLLECTION_DATE))

### Increase the number of location types!
results<-temp2024 %>%
  dplyr::select(c(RESULT,COLLECTION_DATE,LOCATION_TYPE,LOCATION_PURPOSE,MONITORING_LOCATION, geometry)) %>% 
  dplyr::filter(!is.na(RESULT)) %>% 
  dplyr::filter(LOCATION_TYPE == "RIVER,STREAM OR CREEK" |
                  LOCATION_TYPE == "MONITORING WELL" |
                  LOCATION_TYPE == "LAKE OR POND") #%>% 
# dplyr::group_by(MONITORING_LOCATION) %>% 
# dplyr::summarise(medianVal = median(RESULT))

results_albers = sf::st_transform(results, 3005)

results_albers = results_albers |>
  dplyr::mutate(COLLECTION_DATE = as.Date(COLLECTION_DATE, format = "%Y-%m-%d")) |>
  dplyr::filter(COLLECTION_DATE >= "2020-01-01")

# Simplify our input data so that there is one point of data per 100km^2 raster cell.
interp_grid = sf::st_make_grid(sf::st_as_sf(bc_vect_alb), cellsize = c(10000,10000)) |> 
  sf::st_as_sf() |> 
  sf::st_filter(sf::st_as_sf(bc_vect_alb))

results_albers_overlap = interp_grid |> 
  dplyr::mutate(row_id = row_number()) |> 
  sf::st_join(results_albers)

results_albers_overlap = results_albers_overlap |> 
  dplyr::group_by(row_id) |> 
  dplyr::mutate(medianVal = median(RESULT)) |> 
  dplyr::ungroup() |> 
  dplyr::filter(!duplicated(row_id))

results_albers_as_centroids = results_albers_overlap |> 
  sf::st_centroid()

results_albers_as_centroids <- results_albers_as_centroids |>
  mutate(month = month(COLLECTION_DATE))

results_albers_as_centroids_no_na <- results_albers_as_centroids |> 
  filter(!is.na(medianVal))

output_dir <- paste0(gsub("CNF/", "", onedrive_path), "raster/monthly_temperature/")
dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)

bc = bcmaps::bc_bound() |> 
  dplyr::summarise() |> 
  terra::vect()

#create interpolation grid encompassing Canada and USA
bbox <- sf::st_bbox(st_as_sf(bc))

grid10km <- expand.grid(
  X = seq(from = bbox["xmin"], to = bbox["xmax"], by = 10000),
  Y = seq(from = bbox["ymin"], to = bbox["ymax"], by = 10000)) %>%
  mutate(Z = 0)  %>% 
  raster::rasterFromXYZ(crs = 3005) 
KRgrid10km <- as(grid10km, "SpatialGrid")


# Loop over months
for (m in sort(unique(results_albers_as_centroids_no_na$month))) {
  message("Processing month: ", m)
  
  # Filter data for this month
  data_month <- results_albers_as_centroids_no_na |> filter(month == m)
  
  
  if (nrow(data_month) < 10) {
    message("Skipping month ", m, ": not enough data points")
    next
  }
  data_month <- data_month |> 
    filter(!is.na(medianVal), medianVal > 0)
  # Compute log10
  data_month$logMedian <- log10(data_month$medianVal + 0.001)
  
  # Fit variogram
  var_model_month <- autofitVariogram(logMedian ~ 1, as(data_month, "Spatial"))
  
  # Create Kriging model
  kr_model <- gstat(formula = logMedian ~ 1,
                    locations = as(data_month, "Spatial"),
                    model = var_model_month$var_model)
  
  
  # Predict
  interp <- predict(kr_model, KRgrid10km)
  
  # Back-transform predictions
  interp$var1.pred <- 10^(interp$var1.pred - 0.001)
  interp$var1.var  <- 10^(interp$var1.var  - 0.001)
  
  # Convert to raster and mask
  rast_pred <- terra::rast(interp)
  rast_pred <- terra::mask(rast_pred, bc_vect_alb)
  
  rast_var <- terra::rast(interp, layer = "var1.var")
  rast_var <- terra::mask(rast_var, bc_vect_alb)
  
  # Crop and mask
  masked_rast <- terra::crop(rast_pred, bc_vect_alb)
  masked_rast <- terra::mask(masked_rast, bc_vect_alb)
  
  # Save
  fname_base <- paste0(output_dir, variable_to_search, "_", year_to_search, "_", m)
  terra::writeRaster(rast_pred, paste0(fname_base, "_krig.tif"), overwrite = TRUE)
  terra::writeRaster(rast_var, paste0(fname_base, "_var_krig.tif"), overwrite = TRUE)
  terra::writeRaster(masked_rast, paste0(fname_base, "_masked_krig.tif"), overwrite = TRUE)
  
  message("Finished processing month ", m)
}