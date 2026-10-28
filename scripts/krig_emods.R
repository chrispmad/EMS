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
library(lubridate)
set.seed(123)
#var_name = "Phosphorus"; confidence_interval = 0.99

krig_ems<-function(var_name, confidence_interval = 0.99, output_name = var_name[1]){
  year_to_search = 'All'
  
  bc = bcmaps::bc_bound() |> 
    dplyr::summarise() |> 
    terra::vect()
  
  path_to_user = str_extract(getwd(), ".*Users/[A-Z]+")
  onedrive_path = paste0(path_to_user,"/OneDrive - Government of BC/data/CNF/")
  
  bc_vect = terra::vect(sf::st_transform(bcmaps::bc_bound(),4326))
  bc_vect_alb = terra::vect(sf::st_transform(bcmaps::bc_bound(),3005))
  
  conn<-dbConnect(RSQLite::SQLite(),"../AIS_EMS/output/enmods.sqlite")
  dbListTables(conn)
  dbListFields(conn, "results")
  

  
  #var_name <- c("calcium", "phosphorus", "chlorophyll")
  
  query <- paste0(
    "SELECT *
   FROM results
   WHERE medium IN (
     'Water - Fresh',
     'Water - Other',
     'Water - Surface Film',
     'Water - Ground'
   )
   AND (",
    paste0(
      "LOWER(parameter) LIKE '%", tolower(var_name), "%'",
      collapse = " OR "
    ),
    ")"
  )
  
  raw_data <- DBI::dbGetQuery(conn, query)
  #raw_data <- DBI::dbReadTable(conn, "results")
  
  
  
  dbDisconnect(conn)
  
  
  cat(paste0("Unique parameters are: \n", unique(raw_data$parameter),"\n"))
  
  
  ### fix units in the data
  source("scripts/standardize_units.R")
  raw_data <-standardise_units(raw_data,var_name)
  # raw_data |> 
  #   group_by(UNIT) |>
  #   summarise(n = n()) |>
  #   arrange(desc(n))
  
  # raw_data <- raw_data |>
  #   dplyr::filter(UNIT == "mg/L")
  raw_data<-raw_data[!is.na(raw_data$lat),]
  raw_data<-raw_data[!is.na(raw_data$lon),]
  
  data_sf<-sf::st_as_sf(raw_data, coords = c("lon","lat"), crs = 4326)
  # data_sf$COLLECTION_DATE<-as.Date(data_sf$COLLECTION_DATE)
  
  # results<-data_sf %>%
  #   dplyr::select(c(RESULT,COLLECTION_DATE,LOCATION_TYPE,LOCATION_PURPOSE,MONITORING_LOCATION, geometry)) %>% 
  #   dplyr::filter(!is.na(RESULT)) %>% 
  #   dplyr::filter(LOCATION_TYPE != 'STACK',
  #                 LOCATION_TYPE != 'STORAGE',
  #                 LOCATION_TYPE != 'SEEPAGE OR SEEPAGE POOLS',
  #                 LOCATION_TYPE != 'IN PLANT')
  
  results = data_sf |> 
    mutate(value = as.numeric(value))
  
  
  if(confidence_interval != 1){
    # Find confidence interval for values.
    conf_int_bounds <- t.test(results$value, conf.level = confidence_interval)$conf.int[c(1:2)]
    
    # calculate first quantile
    Quantile1 <- quantile(results$value, probs=1-confidence_interval)
    
    # calculate third quantile
    Quantile3 <- quantile(results$value, probs=1-(1-confidence_interval))
    
    # calculate inter quartile range
    IQR = Quantile3-Quantile1
    
    outliers = results[results$value > Quantile3 + (IQR*1.5) | results$value < Quantile1 - (IQR*1.5),]
    
    # Tell user about the number of rows about to be dropped.
    cat(paste0("\nNote: ",nrow(outliers)," (",round(100*nrow(outliers)/nrow(results),3),
               "% of all records) outlier data points found outside of the 1st and 99th quantiles (",
               round(Quantile1,2)," and ",round(Quantile3,2),", respectively)... removed!"))
    
    # Find and drop outlier rows
    results = results[!(results$value > Quantile3 + (IQR*1.5) | results$value < Quantile1 - (IQR*1.5)),]
  }
  
  # Simplify our input data so that there is one point of data per 100km^2 raster cell.
  interp_grid = sf::st_make_grid(sf::st_as_sf(bc_vect_alb), cellsize = c(10000,10000)) |> 
    sf::st_as_sf() |> 
    sf::st_filter(sf::st_as_sf(bc_vect_alb))
  
  results_albers = sf::st_transform(results, 3005)
  
  results_albers_overlap = interp_grid |> 
    dplyr::mutate(row_id = row_number()) |> 
    sf::st_join(results_albers)
  
  results_albers_overlap = results_albers_overlap |> 
    dplyr::group_by(row_id) |> 
    dplyr::mutate(medianVal = median(value)) |> 
    dplyr::ungroup() |> 
    dplyr::filter(!duplicated(row_id))
  
  results_albers_as_centroids = results_albers_overlap |> 
    sf::st_centroid()
  
  #p1<-ggplot() + geom_sf(data = results_albers_as_centroids, aes(fill = medianVal, col = medianVal))
  #ggsave(paste0(filename = "./images/median_",var_name,".png"), plot = p1, dpi = 300)
  #for turbidity
  #results_albers_as_centroids <- results_albers_as_centroids %>% filter(medianVal < 500)
  
  results_albers_as_centroids_no_na = results_albers_as_centroids |> dplyr::filter(!is.na(medianVal))
  
  toInterp<-sf::st_transform(results_albers_as_centroids_no_na, 3005)
  
  tointerpPt<- toInterp %>% 
    dplyr::mutate(lon = sf::st_coordinates(.)[,1],
                  lat = sf::st_coordinates(.)[,2])
  
  #create interpolation grid encompassing Canada and USA
  bbox <- sf::st_bbox(sf::st_as_sf(bc))
  
  
  # Bring in raster for reference (dimensions, extent, etc.)
  # pred_bioc = terra::rast("CMI/climate/wc2.1_5m/wc2.1_5m_bioc_ACCESS-CM2_ssp585_2021-2040.tif")
  bc_vect = terra::vect(sf::st_transform(bcmaps::bc_bound(),4326))
  # pred_bioc_clipped = mask(crop(pred_bioc, bc_vect), bc_vect)
  
  #terra::plot(pred_bioc_clipped$bio01)
  
  # ref = pred_bioc_clipped$bio01
  
  ref = terra::rast(paste0(onedrive_path, "reference_raster_wgs84.tif"))
  
  # ext_ref <- ext(ref)
  # res_ref <- res(ref)
  # x_seq <- seq(from = ext_ref[1], to = ext_ref[2], by = res_ref[1])
  # y_seq <- seq(from = ext_ref[3], to = ext_ref[4], by = res_ref[2])
  # grid10km <- expand.grid(x = x_seq, y = y_seq)
  # grid10km <- rast(grid10km, crs = crs(ref))
  # grid10km$nlyr<-1
  
  # grid10km = terra::resample(grid10km, ref)
  
  #grid10km<-raster(grid10km)
  # st_crs(results_albers_as_centroids_no_na)
  pointscrs<-sf::st_transform(results_albers_as_centroids_no_na,4326)
  # st_crs(grid10km)
  # st_crs(pointscrs)
  
  #pointscrs$medianVal<-log10(pointscrs$medianVal+0.001)
  varKRVar <- autofitVariogram(medianVal ~ 1, 
                               as(pointscrs, "Spatial"),
                               verbose=TRUE,
                               fix.values = c(0,NA,NA))
  
  KRvarmod <- gstat(formula=medianVal~1,
                    locations=as(pointscrs,"Spatial"),
                    model=varKRVar$var_model
  )
  
  KRgrid10km <- as(raster(ref), "SpatialGrid")
  #KRVar_interpolation <- predict(KRvarmod, KRgrid10km, debug.level = -1)
  
  # interp_r = terra::rast(KRVar_interpolation)
  # maskrast = terra::mask(interp_r$var1.pred, ref)
  # 
  # KRVar_interpolation_raster <- raster(KRVar_interpolation) 
  # plot(KRVar_interpolation_raster)
  # 
  # spatRast<-rast(KRVar_interpolation_raster)
  # 
  # # testrast<-crop(spatRast, bc_vect)
  # # maskrast<-mask(testrast, bc_vect)
  # # plot(maskrast)
  # 
  # KRVar_interpolation_variance_raster<-raster(KRVar_interpolation, layer = "var1.var")
  # spatRastVar<-rast(KRVar_interpolation_variance_raster)
  
  library(snow)
  
  cat("Starting Cluster...\n")
  beginCluster(n = 4)
  
  krig_rast<-clusterR(raster(ref), raster::interpolate, args = list(KRvarmod))
  
  endCluster()
  cat("Finished Cluster\n")
  spatRast<-rast(krig_rast)
  plot(spatRast)
  #spatRast[[1]] <- 10^(spatRast[[1]] - 0.001)
  plot(spatRast)
  
  testrast<-crop(spatRast, bc_vect)
  maskrast<-mask(testrast, bc_vect)
  
  plot(maskrast)
  
  
  
  
  var_save<-gsub("[^[:alnum:]]+", "_", output_name)
  new_path <- gsub("CNF/", "", onedrive_path)
  
  
  terra::writeRaster(spatRast, paste0(new_path,"raster/",var_save,"_",year_to_search,"_krig.tif"), overwrite = T)
  #terra::writeRaster(spatRastVar, paste0(new_path,"raster/",var_save,"_",year_to_search,"_var_krig.tif"), overwrite = T)
  terra::writeRaster(maskrast, paste0(new_path,"raster/",var_save,"_",year_to_search,"_masked_krig.tif"), overwrite = T)
  #return(maskrast)
}
