# About SWERI Data Tools
SWERI Data Tools contains the code used to compile the Treatment Index - the core database at the heart of TWIG. TWIG is a project of [ReSHAPE](https://reshapewildfire.org/home). 

# Contents 
```
.
├── .github/workflows          # Utility for unit testing pull requests
├── analysis                   # Visualization of FACTS Activity attributes 
├── docker                     # Docker compose setup
├── gp_tool                    # Geoprocessing utilities
├── load_testing               # Configure load testing with Locust
├── shell_scripts              # Automate processes
├── sweri_utils                # Python utilities
├── .gitignore                 # Ignore files in commits
├── _init_.py                  # Use directory as python package
├── daily_progression.py       # Build daily fire progression polygons  
├── error_flagging.py          # Identify and flag potential errors
├── intersections.py           # Calculate treatment intersections
├── package_list.txt           # Create conda environment
├── requirements.testing.txt   # Testing utilities
├── treatment_index.py         # Create the treatment index
└── README.md                  # README file - you are here!
```
# Main Script Summaries

The SWERI Data Tools use python3, psycopg, PostgreSQL, and ArcPy to query, transform, and index geospatial records from multiple endpoints. 
The scripts SWERI data tools are:

## treatment_index.py
Queries source databases and crosswalks attributes from each source into a common schema.
Current source databases:
- [NFPORS](https://usgs.nfpors.gov/arcgis/rest/services/treatmentPoly/FeatureServer/0)
- [FACTS Hazardous Fuel Treatment Reduction: Polygon](https://data.fs.usda.gov/geodata/edw/datasets.php?xmlKeyword=Hazardous+Fuel+Treatment)
- [FACTS Common Attributes](https://data.fs.usda.gov/geodata/edw/datasets.php?xmlKeyword=common+attributes)
	
FACTS Common attributes records are a subset of the database detirmined to be treatments based on a process detailed in the _Data Descriptor_ paper

## error_flagging.py
Finds and flags errors in treatment index records. Error codes are added to the 'error' field and separed by a semicolon ';'
Current error codes are:
- DUPLICATE-KEEP : Record has duplicates and should be used as the representative record
- DUPLICATE-DROP : Record is a duplicate and should be dropped during analysis
- HIGH-COST : Cost of treatment is greater than $10,000 per acre
- CHECK_UOM : Unit of Measure is listed as a unit that may affect cost calculations (EACH, MILES)

## intersections.py
Determines the geographic intersection between treatments and other layers of interest. 
Current layers intersected:
1. TWIG Treatment Index

## daily_progression.py

The `daily_progression.py` script is used to query [WFIGS Current Wildfire Perimeters](https://gis.reshapewildfire.org/arcgis/home/item.html?id=c537b9e406c64450b55e1be2a4ae7db9) and records any changes from the previous day. These changes are stored in order to visualize the daily progression of wildfires over time.
This layer is time-enabled with both start and end times.

- start time field: `start_date`
- end time field: `removal_date`

If a record still exists in [WFIGS Current Wildfire Perimeters](https://gis.reshapewildfire.org/arcgis/home/item.html?id=c537b9e406c64450b55e1be2a4ae7db9) as of the last `daily_progression.py` run, removal_date for that record will be null

## sweri_utils folder
Contains a collection python files that contain helper functions used throughout the main sweri-data-tools scripts

# Creating a TWIG Treatment Index Database Replica
TBD

# Citing the Treatment Index
We appreciate citations for TWIG, the Treatment Index, and this software because it lets us find out what people have been doing with them and motivates further grant funding. 
A forthcoming _Data Descriptor_ paper will include a DOI and it is recommended that users cite this paper when it becomes available. 
Please [contact us](aidan-franko@nau.edu) for more information. 

# Open Code & Reproducible Science
SWERI Data Tools exist to facilitate open and reprobucible science and reporting. Internal discussions regarding the appropriate license are ongoing. 
