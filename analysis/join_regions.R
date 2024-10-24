# join and clean raw data from different regions
# uses csv files from:
# https://data.fs.usda.gov/geodata/edw/datasets.php?xmlKeyword=FACTS+Common+Attributes

# load libraries
library(tidyverse)

# list filenames
csvs <- list.files(path = "./raw/all_raw_20240825",
                   pattern = "*.csv",
                   full.names = TRUE)
# import and clean data
data <- lapply(
  csvs,
  \(x) data.table::fread(x) |> # read data quickly with data.table:fread
    select(id = OBJECTID, # select only the columns
           acres = GIS_ACRES,
           activity = ACTIVITY,
           activity_code = ACTIVITY_CODE,
           method = METHOD,
           equipment = EQUIPMENT,
           nfpors = NFPORS_TREATMENT,
           fka = FUELS_KEYPOINT_AREA) |>
    mutate(id = as.integer(id), # ensure id is integer (sometimes read as char?)
           region = str_sub(x, -6, -5), # extract region from filename
           acres = as.numeric(acres), # ensure acres is numeric
           .before = 1)
) |>
  bind_rows() # bind all data together into a single data frame

write_csv(data, "tidy_facts.csv")
