library(renmods)
library(dplyr) 
library(stringr)
library(DBI)

#a search for some of the variables that we need - add to these as required.
pattern <- "\\bph\\b|calcium|chlorophyll|oxygen|phosphorus|temperature"





#make the connection
db <- renmods_connect()

#whats in here?
#where_sampled = db |> count(Medium, sort = TRUE) |> collect()
#loc_all = db |> count(Location_Type, sort = TRUE) |> collect()

# only get the locations we use
loc_types <- c("River, Stream, or Creek", "Lake or Pond", "Ditch or Culvert", "Estuary")




## Look for whats in here again, what variables are available?
props_any <- db |>
  distinct(Observed_Property_ID, Observed_Property_Name, Medium) |>
  collect() |>
  filter(str_detect(Observed_Property_ID, regex(pattern, ignore_case = TRUE)))



# this is making a query for the new set up of the database and the renmods library.
q <- db |>
  filter(
    Location_Type %in% loc_types,
    Observed_Property_ID %in% !!props_any$Observed_Property_ID,
    QC_Type == "NORMAL"
  )

# and finally get the data - use the filter/query we made above
dat <- q |> collect()

unique(dat$Observed_Property_ID)

# we don't want some of these - like air temperature is not useful
vars_to_drop = c("Temperature (Air) (temp.)","Final pH of solvent in leachability methods (acidity)",
                 "Acidity pH 4.5 (as CaCO3) (fl. conc.)", "Oxygen Dissolved (ratio)", "Carbonaceous Biochemical Oxygen Demand (fl. conc.)",
                 "Acidity to pH 8.3 (equiv./vol.)","Alkalinity pH 4.5/4.2 (fl. conc.)", "Biochemical Oxygen Demand (mass flow)" ,
                 "Organo-phosphorus (unitless)")

dat = dat |> 
  dplyr::filter(!Observed_Property_ID %in% vars_to_drop)

# now only keep what is useful. I think these columns are all we need.
dat_filtered <- dat |>
  filter(
    Observed_Property_Result_Type == "NUMERIC",
    !is.na(Result_Value),
    !is.na(Location_Latitude), !is.na(Location_Longitude)
  ) |>
  transmute(
    Location_ID, Location_Name, Location_Type, Medium,
    lat = Location_Latitude,
    lon = Location_Longitude,
    date = ren_date,
    parameter = Observed_Property_ID,
    fraction = Fraction,              
    value = Result_Value,
    unit = Result_Unit,
    detection = Detection_Condition, 
    mdl = Method_Detection_Limit,
    depth_upper = Depth_Upper,
    depth_lower = Depth_Lower,
    status = Result_Status
  )

### parameter to lower case
dat_filtered = dat_filtered |> 
  dplyr::mutate(parameter = stringr::str_to_lower(parameter))


#then finally, save it
# no dates, only character

out <- dat_filtered |>
  mutate(date = as.character(date),
         value = as.numeric(value))
  

final <- "output/enmods.sqlite"
tmp   <- "output/enmods_tmp.sqlite"
if (file.exists(tmp)) file.remove(tmp)

con <- dbConnect(RSQLite::SQLite(), tmp)
dbWriteTable(con, "results", out)
dbExecute(con, "CREATE INDEX idx_param ON results (parameter)")
dbExecute(con, "CREATE INDEX idx_loc   ON results (Location_ID)")
dbExecute(con, "CREATE INDEX idx_date  ON results (date)")
dbWriteTable(con, "meta", tibble::tibble(
  pulled = as.character(Sys.Date()),
  n_rows = nrow(out),
  location_types = paste(loc_types, collapse = "; ")
))
n_db <- dbGetQuery(con, "SELECT COUNT(*) AS n FROM results")$n
dbDisconnect(con)

stopifnot(n_db == nrow(out)) 
file.copy(tmp, final, overwrite = TRUE)
file.remove(tmp)

renmods_disconnect(db)

# the delete of temp was annoying. This deals with it
gc() 
stopifnot(n_db == nrow(out))
file.copy(tmp, final, overwrite = TRUE)

for (i in 1:5) {                  
  unlink(tmp)
  if (!file.exists(tmp)) break
  Sys.sleep(2)
}

