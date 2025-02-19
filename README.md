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
# Get Started
The SWERI Data Tools use python3, psycopg, PostgreSQL, and ArcPy to query, transform, and index geospatial records from multiple endpoints. 
The core functions of the SWERI data tools are to:
1. Query source databases and crosswalk attributes from each source using `treatment_index.py`
2. Identify and flag potential errors using `error_flagging.py`
3. Determine where treatment and wildfire boundaries intersect using `intersections.py`


The `daily_progression.py` script is used to query [WFIGS Current Wildfire Perimeters ](https://gis.reshapewildfire.org/arcgis/home/item.html?id=c537b9e406c64450b55e1be2a4ae7db9) and records any changes from the previous day. These changes are stored in order to visualize the daily progression of wildfires over time.

Configuration and testing utilities are also included. 

# Citing the Treatment Index
We appreciate citations for TWIG, the Treatment Index, and this software because it lets us find out what people have been doing with them and motivates further grant funding. 
A forthcoming _Data Descriptor_ paper will include a DOI and it is recommended that users cite this paper when it becomes available. 
Please [contact us](aidan-franko@nau.edu) for more information. 

Open Code & Reproducible Science
SWERI Data Tools exist to facilitate open and reprobucible science and reporting. Internal discussions regarding the appropriate license are ongoing. 
