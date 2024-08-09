library(DBI)

con <- dbConnect(RSQLite::SQLite(), "J:/2 SCIENCE - Invasives/GENERAL/Operations/EnvironmentalMonitoringSystemDatabase/EMS.sqlite")
lcon <- dbConnect(RSQLite::SQLite(), "output/EMS.sqlite")

d = dbGetQuery(con, "select * from results where PARAMETER like '%Calcium%'")
#Started the above query at 12:49 PM... didn't complete by almost 1 PM. Woof.

d = dbGetQuery(lcon, "select * from results where PARAMETER like '%Calcium%'")
# Query started at 1:56 PM...

