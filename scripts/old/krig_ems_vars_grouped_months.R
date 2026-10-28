
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
library(lubridate)
library(snow)
set.seed(123)

krig_ems_monthly <- function(var_name, confidence_interval = 0.99) {
  path_to_user <- str_extract(getwd(), ".*Users/[A-Z]+")
  onedrive_path <- file.path(path_to_user, "OneDrive - Government of BC/data/")
  #new_path <- gsub("CNF/", "", onedrive_path)
  
  # Reference layers
  bc_vect <- terra::vect(sf::st_transform(bcmaps::bc_bound(), 4326))
  bc_vect_alb <- terra::vect(sf::st_transform(bcmaps::bc_bound(), 3005))
  ref <- terra::rast(file.path(onedrive_path, "CNF/reference_raster_wgs84.tif"))
  
  conn <- dbConnect(RSQLite::SQLite(), "../EMS/output/EMS.sqlite")
  raw_data <- dbGetQuery(conn, paste0("SELECT * FROM results WHERE parameter LIKE '%", var_name, "%'"))
  dbDisconnect(conn)
  
  # Clean data
  raw_data <- raw_data |> 
    filter(!is.na(LATITUDE), !is.na(LONGITUDE), !is.na(RESULT)) |> 
    mutate(
      COLLECTION_DATE = as.Date(COLLECTION_DATE),
      year = year(COLLECTION_DATE),
      month = month(COLLECTION_DATE)
    )
  
  raw_data <- raw_data |> filter(!LOCATION_TYPE %in% c('STACK', 'STORAGE', 'SEEPAGE OR SEEPAGE POOLS', 'IN PLANT'))
  
  # Loop over months
  for (m in 1:12) {
    cat(paste0("\nProcessing month: ", m, "\n"))
    
    data_month <- raw_data |> filter(month == m)
    
    if (nrow(data_month) < 10) {
      cat("Skipping: Not enough data\n")
      next
    }
    
    # Outlier filtering
    if (confidence_interval != 1) {
      Q1 <- quantile(data_month$RESULT, probs = 1 - confidence_interval)
      Q3 <- quantile(data_month$RESULT, probs = 1 - (1 - confidence_interval))
      IQR <- Q3 - Q1
      data_month <- data_month |> filter(RESULT >= (Q1 - 1.5 * IQR), RESULT <= (Q3 + 1.5 * IQR))
    }
    
    # Spatial preprocessing
    results_sf <- st_as_sf(data_month, coords = c("LONGITUDE", "LATITUDE"), crs = 4326) |> 
      st_transform(3005)
    
    # Make 10km grid
    interp_grid <- st_make_grid(st_as_sf(bc_vect_alb), cellsize = c(10000, 10000)) |> 
      st_as_sf() |> 
      st_filter(st_as_sf(bc_vect_alb))
    
    # Join data to grid
    joined <- interp_grid |> 
      mutate(row_id = row_number()) |> 
      st_join(results_sf) |> 
      group_by(row_id) |> 
      summarize(medianVal = median(RESULT, na.rm = TRUE), .groups = "drop") |> 
      filter(!is.na(medianVal)) |> 
      st_centroid()
    
    if (nrow(joined) < 5) {
      cat("Skipping: Not enough median values after grouping\n")
      next
    }
    
    # Interpolation
    pointscrs <- st_transform(joined, 4326)
    pointscrs$lon <- st_coordinates(pointscrs)[, 1]
    pointscrs$lat <- st_coordinates(pointscrs)[, 2]
    
    var_model <- autofitVariogram(medianVal ~ 1, as(pointscrs, "Spatial"), fix.values = c(0, NA, NA))
    kr_model <- gstat(formula = medianVal ~ 1, locations = as(pointscrs, "Spatial"), model = var_model$var_model)
    
    KRgrid10km <- as(raster(ref), "SpatialGrid")
    
    beginCluster(n = 4)
    krig_rast <- clusterR(raster(ref), interpolate, args = list(kr_model))
    endCluster()
    
    spat_rast <- rast(krig_rast)
    masked_rast <- mask(crop(spat_rast, bc_vect), bc_vect)
    
    var_save <- gsub(" ", "_", var_name)
    out_path <- paste0(onedrive_path, "/raster/monthly_temperature/", var_save, "_All_", sprintf("%02d", m), "_krig.tif")
    mask_path <- paste0(onedrive_path, "/raster/monthly_temperature/", var_save, "_All_", sprintf("%02d", m), "_masked_krig.tif")
    
    terra::writeRaster(spat_rast, out_path, overwrite = TRUE)
    terra::writeRaster(masked_rast, mask_path, overwrite = TRUE)
    
    cat(paste0("Saved: ", mask_path, "\n"))
  }
}

krig_ems_monthly("Temperature")
