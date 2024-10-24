# Download and join FACTS Common Attributes and Hazardous Fuels Treatment data
library(tidyverse)
library(sf)
library(curl)

# download Hazardous Fuels (polygons) geodatabase ####
# define url
hf_poly_url <-
  'https://data.fs.usda.gov/geodata/edw/edw_resources/fc/S_USA.Activity_HazFuelTrt_PL.gdb.zip'

# download to temp file
tmp <- tempfile(fileext = ".zip")
curl_download(hf_poly_url, tmp, quiet = FALSE)

# extract the file
outDir <-
  "C:\\Users\\anson\\Documents\\FACTS_reclass\\databases\\hazardous_fuels.gdb"
unzip(tmp, junkpaths = TRUE, exdir = outDir)

# read the file, drop geometry, and write csv to disk
st_read(
  "C:\\Users\\anson\\Documents\\FACTS_reclass\\databases\\hazardous_fuels.gdb"
) %>%
  st_drop_geometry() %>%
  write_csv("./databases/gdb_to_csv/hf.csv")

gc()

# check for unique identifier
length(unique(hf_poly_raw$ACTIVITY_CN)) == nrow(hf_poly_raw)
# unique identifier is ACTIVITY_CN

# download Common Attributes geodatabases ####
# define urls
ca_poly_urls <- c(
  "https://data.fs.usda.gov/geodata/edw/edw_resources/fc/Actv_CommonAttribute_PL_Region01.zip",
  "https://data.fs.usda.gov/geodata/edw/edw_resources/fc/Actv_CommonAttribute_PL_Region02.zip",
  "https://data.fs.usda.gov/geodata/edw/edw_resources/fc/Actv_CommonAttribute_PL_Region03.zip",
  "https://data.fs.usda.gov/geodata/edw/edw_resources/fc/Actv_CommonAttribute_PL_Region04.zip",
  "https://data.fs.usda.gov/geodata/edw/edw_resources/fc/Actv_CommonAttribute_PL_Region05.zip",
  "https://data.fs.usda.gov/geodata/edw/edw_resources/fc/Actv_CommonAttribute_PL_Region06.zip",
  "https://data.fs.usda.gov/geodata/edw/edw_resources/fc/Actv_CommonAttribute_PL_Region08.zip",
  "https://data.fs.usda.gov/geodata/edw/edw_resources/fc/Actv_CommonAttribute_PL_Region09.zip",
  "https://data.fs.usda.gov/geodata/edw/edw_resources/fc/Actv_CommonAttribute_PL_Region10.zip"
)

# define destinations for downloads
filenames <- paste0(
  "C:\\Users\\anson\\Documents\\FACTS_reclass\\databases\\regions\\",
  names(regions)[1:9],
  ".zip"
  )

# download
multi_download(ca_poly_urls, filenames)

# define destinations for extracted files
exdirs <- paste0(
  "C:\\Users\\anson\\Documents\\FACTS_reclass\\databases\\regions\\",
  c("R01", "R02", "R03", "R04", "R05", "R06", "R08", "R09", "R10"),
  ".gdb"
  )

# unzip all files
map2(filenames, exdirs, ~ unzip(.x, junkpaths = TRUE, exdir = .y))

# read geodatabases, drop geometry, and write csv to disk.
# loading all into memory at once is too much.
map2(
  exdirs,
  rnames[1:9],
  function(x, y) {
    dat <- st_read(x) %>%
      st_drop_geometry()
    write_csv(dat, paste0("./databases/gdb_to_csv/", y, ".csv"))
    rm(dat)
    },
  .progress = TRUE
  )

gc()

# read in csvs
rnames <-
  c("R01", "R02", "R03", "R04", "R05", "R06", "R08", "R09", "R10", "hf")
csvs <- list.files(path = "./databases/gdb_to_csv",
                   pattern = "*.csv",
                   full.names = TRUE)

map2(
  csvs,
  rnames,
  function(x, y) {
    dat <- read_csv(x)
    assign(y, dat, envir = .GlobalEnv)
    rm(dat)
    },
  .progress = TRUE
  )

# bind all Common Attributes into a single data frame
all <- rbind(R01, R02, R03, R04, R05, R06, R08, R09, R10)

# join Common Attributes and Hazardous Fuels Treatment data
all_join <- full_join(all, hf, by = c("EVENT_CN" = "ACTIVITY_CN"))
