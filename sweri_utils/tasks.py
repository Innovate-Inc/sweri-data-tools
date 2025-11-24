from osgeo.gdal import VectorTranslateOptions, VectorTranslate
import json
import logging
import re

from sweri_utils.common_attributes import add_fields_and_indexes, common_attributes_date_filtering, exclude_by_acreage, \
    exclude_facts_hazardous_fuels, include_logging_activities, include_fire_activites, include_fuel_activities, \
    activity_filter, include_other_activites, set_included, common_attributes_insert
from sweri_utils.download import fetch_features
from sweri_utils.files import download_file_from_url, extract_and_remove_zip_file, gdb_to_postgres
from sweri_utils.sql import trim_whitespace, create_db_conn_from_envs

from worker import app

@app.task
def service_chunk_to_postgres(url, params, schema, destination_table, ogr_db_string):
    r = fetch_features(url + '/query', params, return_full_response=True)
    logging.info(r)

    options = VectorTranslateOptions(format='PostgreSQL',
                                     accessMode='append',
                                     geometryType=['POLYGON', 'PROMOTE_TO_MULTI'],
                                     layerName=f'{schema}.{destination_table}_buffer')
    # commit chunks to database in
    _ = VectorTranslate(destNameOrDestDS=ogr_db_string, srcDS=f"ESRIJSON:{json.dumps(r)}", options=options)
    del _

@app.task()
def common_attributes_processing(url, projection, common_attributes_fc_name, schema, ogr_db_string, facts_haz_table, treatment_index):
    # expression pulls just the number out of the url, 01-10
    conn = create_db_conn_from_envs()
    region_number = re.sub("\D", "", url)
    ca_table_name = f'common_attributes_{region_number}'
    gdb = f'Actv_CommonAttribute_PL_Region{region_number}.gdb'

    zip_file = f'{ca_table_name}.zip'

    logging.info(f'Downloading {url}')
    download_file_from_url(url, zip_file)

    logging.info(f'Extracting {zip_file}')
    extract_and_remove_zip_file(zip_file)

    # special input srs for common attributes
    # https://gis.stackexchange.com/questions/112198/proj4-postgis-transformations-between-wgs84-and-nad83-transformations-in-alask
    # without modifying the proj4 srs with the towgs84 values, the data is not in the "correct" location

    input_srs = '+proj=longlat +datum=NAD83 +no_defs +type=crs +towgs84=-0.9956,1.9013,0.5215,0.025915,0.009426,0.011599,-0.00062'
    gdb_to_postgres(gdb, projection, common_attributes_fc_name, ca_table_name, schema, ogr_db_string, input_srs)

    add_fields_and_indexes(conn, schema, ca_table_name, region_number)

    common_attributes_date_filtering(conn, schema, ca_table_name)
    exclude_by_acreage(conn, schema, ca_table_name)
    exclude_facts_hazardous_fuels(conn, schema, ca_table_name, facts_haz_table)

    fields_to_trim = ['activity', 'method', 'equipment']

    for field in fields_to_trim:
        trim_whitespace(conn, schema, ca_table_name, field)

    include_logging_activities(conn, schema, ca_table_name)
    include_fire_activites(conn, schema, ca_table_name)
    include_fuel_activities(conn, schema, ca_table_name)
    activity_filter(conn, schema, ca_table_name)
    include_other_activites(conn, schema, ca_table_name)

    set_included(conn, schema, ca_table_name)

    common_attributes_insert(conn, schema, ca_table_name, treatment_index)