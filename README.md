# About SWERI Data Tools

SWERI Data Tools contains the code used to compile the Treatment Index - the core database at the heart of TWIG. TWIG is
a project of [ReSHAPE](https://reshapewildfire.org/home).

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

The SWERI Data Tools use python3, psycopg, PostgreSQL, and ArcPy to query, transform, and index geospatial records from
multiple endpoints.
The scripts SWERI data tools are:

## treatment_index.py

Queries source databases and crosswalks attributes from each source into a common schema. To run treatment_index.py, 
see [TWIG Treatment Index Database Replica](#TWIG-Treatment-Index-Database-Replica) below.

Current source databases:

- [NFPORS](https://usgs.nfpors.gov/arcgis/rest/services/treatmentPoly/FeatureServer/0)
- [FACTS Hazardous Fuel Treatment Reduction: Polygon](https://data.fs.usda.gov/geodata/edw/datasets.php?xmlKeyword=Hazardous+Fuel+Treatment)
- [FACTS Common Attributes](https://data.fs.usda.gov/geodata/edw/datasets.php?xmlKeyword=common+attributes)

FACTS Common attributes records are a subset of the database determined to be treatments based on a process detailed in
the _Data Descriptor_ paper

## error_flagging.py

Finds and flags errors in treatment index records. Error codes are added to the 'error' field and separated by a
semicolon ';'
Current error codes are:

- DUPLICATE-KEEP : Record has duplicates and should be used as the representative record
- DUPLICATE-DROP : Record is a duplicate and should be dropped during analysis
- HIGH-COST : Cost of treatment is greater than $10,000 per acre
- CHECK_UOM : Unit of Measure is listed as a unit that may affect cost calculations (EACH, MILES)

## intersections.py

Determines the geographic intersection between treatments and other layers of interest and calculates the geodesic area
of the overlap in acres.
Current layers intersected:

1. [TWIG Treatment Index](https://gis.reshapewildfire.org/arcgis/home/item.html?id=3d8263f3ee89400fb9da5f5fb5bbf7f1)
2. [WFIGS Current Wildfire Perimeters](https://gis.reshapewildfire.org/arcgis/home/item.html?id=c537b9e406c64450b55e1be2a4ae7db9)
3. [WFIGS Interagency Wildfire Perimeters](https://gis.reshapewildfire.org/arcgis/home/item.html?id=6ecd119b49dd4a23bfb2565cb09c544f)
4. [Interagency Wildland Fire Perimeter History](https://gis.reshapewildfire.org/arcgis/home/item.html?id=d767df2022ae40ffbfa62a1243469404)
5. [US States](https://gis.reshapewildfire.org/arcgis/home/item.html?id=0081c306a470410fa7334164511a8407)
6. [US Census Counties](https://gis.reshapewildfire.org/arcgis/home/item.html?id=a5d6566f2c424e668cb53ce4bd391bc5)
7. [US Federal Lands](https://gis.reshapewildfire.org/arcgis/home/item.html?id=d9fbc27a04064b45954f94c3d60dced9)
8. [119th Congressional Districts](https://gis.reshapewildfire.org/arcgis/home/item.html?id=e3939b55dbea448abed5cc2c03075a6f)

## daily_progression.py

The `daily_progression.py` script is used to
query [WFIGS Current Wildfire Perimeters](https://gis.reshapewildfire.org/arcgis/home/item.html?id=c537b9e406c64450b55e1be2a4ae7db9)
and records any changes from the previous day. These changes are stored in order to visualize the daily progression of
wildfires over time.
This layer is time-enabled with both start and end times.

- start time field: `start_date`
- end time field: `removal_date`

If a record still exists
in [WFIGS Current Wildfire Perimeters](https://gis.reshapewildfire.org/arcgis/home/item.html?id=c537b9e406c64450b55e1be2a4ae7db9)
as of the last `daily_progression.py` run, removal_date for that record will be null

# Utilities, Docker, and Automation

This repository also contains the following related items:

- Module of utilities used by most of the scripts
- Docker related files running intersections processing
- Geoprocessing tools used by the SWERI RESHAPE TWIG Viewer
- Load testing using Locust
- Shell scripts for automating certain processes
- GitHub Actions workflow for unit testing

## sweri_utils package

Contains python modules with helper utilities used by scripts throughout the sweri-data-tools repository.

```
.
└── sweri_utils         # Utilities package
    ├── conversion.py   # Data conversion utilities
    ├── download.py     # Data download utilities
    ├── files.py        # File creation and management utilities
    ├── s3.py           # AWS s3 utilities
    ├── sql.py          # SQL utilities
    └── tests.py        # Unit tests for utilities
```

## docker

The docker directory contains files for running intersections in a docker container.
The [docker-compose.yml](/docker/docker-compose.yml) file defines the services required to run the application in a
Docker environment and a named volume postgres_data to persist PostgreSQL data
. It includes the following services:

- **pgadmin**: Runs the pgAdmin4 web interface for managing PostgreSQL databases. It uses environment variables for
  configuration and maps a host directory to store pgAdmin data.
- **db**: Runs a PostgreSQL database with PostGIS extensions. It uses environment variables for configuration, maps a
  host
  directory to store database data, and includes a health check to ensure the database is ready.
- **app**: Builds and runs the [intersections script](intersections.py). It depends on the database service, uses
  environment variables for
  configuration, and mounts the project directory to the container.

The [Dockerfile](docker/Dockerfile) is used to create a Docker image for running the intersections script in a
containerized environment.

## gp_tool

The gp_tool directory contains two scripts and associated `.atbx` files that are used for running and publishing
geoprocessing tools used by
the [SWERI RESHAPE TWIG Viewer](https://reshapewildfire.org/twig/layers).

- [aoi-intersections.py](gp_tool/aoi_intersections.py): This tool dynamically calculates intersections for an input are
  of interest and returns an Esri FeatureSet. The result is used to display metrics about the intersecting features in
  the TWIG Viewer [representative](https://reshapewildfire.org/twig/representatives)
  and [area summary](https://reshapewildfire.org/twig/area) tools.
- [gp_download.py](gp_tool/gp_download.py): This tool downloads features from an Esri Feature Service, and is used
  throughout the TWIG viewer.

## load_testing

The [locustfile.py](load_testing/locustfile.py) script is used for load testing the SWERI TWIG Viewer and associated
services using [Locust](https://locust.io/). It
simulates multiple users interacting with the application to measure its performance under load to help identify
performance bottlenecks.

## shell_scripts

This directory contains shell scripts which are used by a scheduler to automate the process of updating the treatment
index and daily progressions datasets.

## Gihub Actions

the [.github/workflows](.github/workflows) directory contains
a [GitHub Actions workflow file](.github/workflows/unit-test.yml) that runs unit tests on the [sweri_utils](sweri_utils)
module.

# TWIG Treatment Index Database Replica
Download a replica:
- [Full TWIG Treatment Index Copy](https://sweri-treament-index.s3.us-west-2.amazonaws.com/treatment_index.zip)

Create a replica using [treatment_index.py](#treatment_indexpy):
- [Lookup Tables and Treatment Index Schema](https://sweri-treament-index.s3.us-west-2.amazonaws.com/database_scaffolding.zip)

The Lookup Tables and Treatment Index Schema download contains all tables and feature classes needed to run treatment_index.py
on a local postgres database. Tables and Feature Classes should be uploaded to the same postgres schema. After the initial setup, treatment_index.py
can target that schema and populate the treatment_index feature class. 

# Citing the Treatment Index

We appreciate citations for TWIG, the Treatment Index, and this software because it lets us find out what people have
been doing with them and motivates further grant funding.
A forthcoming _Data Descriptor_ paper will include a DOI and it is recommended that users cite this paper when it
becomes available.
Please [contact us](aidan-franko@nau.edu) for more information.

# Open Code & Reproducible Science

SWERI Data Tools exist to facilitate open and reproducible science and reporting. Internal discussions regarding the
appropriate license are ongoing. 
