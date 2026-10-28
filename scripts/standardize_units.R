standardise_units <- function(dat, var_name) {
  
  var_name <- tolower(var_name)
  
  if (any(grepl("calcium|phosphorus|chlorophyll", var_name))) {
    
    dat <- dat |>
      dplyr::filter(unit %in% c("mg/L", "µg/L")) |>
      dplyr::mutate(
        value = dplyr::if_else(unit == "µg/L", value / 1000, value),
        unit = "mg/L"
      )
    
  } else if (any(grepl("temperature", var_name))) {
    
    dat <- dat |>
      dplyr::filter(unit %in% c("°C", "deg C", "Celsius","degC")) |>
      dplyr::mutate(unit = "°C")
    
  } else if (any(grepl("oxygen", var_name))) {
    
    dat <- dat |>
      dplyr::filter(unit %in% c("mg/L", "µg/L")) |>
      dplyr::mutate(
        value = dplyr::if_else(unit == "µg/L", value / 1000, value),
        unit = "mg/L"
      )
    
  } else if (any(grepl("pH", var_name, ignore.case = TRUE))) {
    
    dat <- dat |>
      dplyr::filter(is.na(unit) | unit %in% c("", "pH units"))
    
  }
  
  return(dat)
}