# must be run first
source("./scripts/krig_emods.R")


## these are the names of the two ph fields - looking for ph alone returns a lot more variables we don't want
ph_vars<-c("ph (acidity)", "ph-field (acidity)")
krig_enmods(ph_vars, output_name = "pH")

krig_enmods("temperature", output_name = "temperature")

calc_vars <- c("calcium")
krig_enmods(calc_vars, output_name = "calcium")

## keeps mg/l only - far more records than the others. We convert ug/l to mg/l and use these, as long as density of water is consistent. This can be changed in stanadize units script
oxygen <-c("oxygen", "disolved oxygen")
krig_enmods(oxygen, output_name = "oxygen")

chloro <-c("chlorophyll")
krig_enmods(chloro, output_name = "chlorophyll")

phos <-c("phosphorus")
krig_enmods(phos, output_name = "phosphorus")

al<-c("alumninium")
krig_enmods(al, output_name = "alumninium")
