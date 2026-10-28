source("scripts/krig_ems.R")


# krigs<-c("Conductivity", "Nitrates", "Oxygen", "Turbidity", "Phosphorus", "Nitrites",
#          "Nitrates", "")
krigs<-c("Temperature", "Chlorophyll", "Oxygen Dissolved", "Phosphorus Total Dissolved",
         "Calcium Dissolved")

for(i in 1:length(krigs)){
  cat(paste0("\nRunning ",krigs[i]))
  krig_ems(krigs[i])
  cat(paste0("\nFinished ",krigs[i]))
}
#krig_ems("Temperature")
#krig_ems_monthly("Temperature")
