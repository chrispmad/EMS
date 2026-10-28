# must be run first
source("./scripts/krig_emods.R")


## these are the names of the two ph fields - looking for ph alone returns a lot more variables we don't want
ph_vars<-c("ph (acidity)", "ph-field (acidity)")
krig_ems(ph_vars, output_name = "pH")

krig_ems("temperature", output_name = "temperature")

calc_vars <- c("calcium")
krig_ems(calc_vars, output_name = "calcium")

## keeps mg/l only - far more records than ratio. This can be changed in stanadize units script
oxygen <-c("oxygen", "disolved oxygen")
krig_ems(oxygen, output_name = "oxygen")


