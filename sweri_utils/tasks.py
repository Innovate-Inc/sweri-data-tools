from osgeo.gdal import VectorTranslateOptions, VectorTranslate
import json
import logging
import re

from sweri_utils.common_attributes import add_fields_and_indexes, common_attributes_date_filtering, exclude_by_acreage, \
    exclude_facts_hazardous_fuels, include_logging_activities, include_fire_activites, include_fuel_activities, \
    activity_filter, include_other_activites, set_included, common_attributes_insert, nfpors_insert, nfpors_fund_code, \
    nfpors_treatment_date_and_status, ifprs_insert, ifprs_treatment_date, ifprs_status_consolidation, \
    hazardous_fuels_date_filtering, hazardous_fuels_insert, \
    remove_wildfire_non_treatment, create_nfpors_where_clause
from sweri_utils.download import fetch_features, prep_buffer_table, get_ids, get_query_params_chunk, swap_buffer_table
from sweri_utils.files import download_file_from_url, extract_and_remove_zip_file, gdb_to_postgres
from sweri_utils.sql import trim_whitespace, create_db_conn_from_envs
from sweri_utils.sweri_logging import log_this, logger
from celery import group, chord

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
def update_nfpors(nfpors_url, schema, wkid, ogr_db_string, chunk_size=40):
    where = create_nfpors_where_clause()
    destination_table = 'nfpors'
    out_fields = ['*']

    prep_buffer_table(schema, destination_table)
    try:
        ids = get_ids(nfpors_url, where=where)
        header = []
        for params in get_query_params_chunk(ids, wkid, out_fields, chunk_size):
            header.append(service_chunk_to_postgres.s(nfpors_url, params, schema, destination_table, ogr_db_string))

    except Exception as e:
        logger.error(f'Error downloading NFPORS: {e}... continuing')
        pass

    return header, destination_table


@app.task()
def nfpors_finalize_task(schema, insert_table, destination_table):
    conn = create_db_conn_from_envs()

    swap_buffer_table(schema, destination_table)
    nfpors_insert(conn, schema, insert_table)
    nfpors_fund_code(conn, schema, insert_table)
    nfpors_treatment_date_and_status(conn, schema, insert_table)

@app.task()
def nfpors_download_and_insert(nfpors_url, schema, insert_table, wkid, ogr_db_string):

    conn = create_db_conn_from_envs()
    header, destination_table = update_nfpors(schema, wkid, nfpors_url, ogr_db_string)
    chord(header)(nfpors_finalize_task.s(schema, insert_table, destination_table))

@app.task()
def update_ifprs(schema, wkid, service_url, ogr_db_string, chunk_size=70):
    conn = create_db_conn_from_envs()

    where = '''
    (Class IN ('Actual Treatment','Estimated Treatment')) AND ((completiondate > DATE '1984-01-01 00:00:00')
    OR (completiondate IS NULL AND initiationdate > DATE '1984-01-01 00:00:00')
    OR (completiondate IS NULL AND initiationdate IS NULL AND createdondate > DATE '1984-01-01 00:00:00'))
'''

    destination_table = 'ifprs'
    out_fields = ['*']

    prep_buffer_table(schema, destination_table)
    try:
        ids = get_ids(service_url, where=where)
        header = []
        for params in get_query_params_chunk(ids, wkid, out_fields, chunk_size):
            header.append(service_chunk_to_postgres.s(service_url, params, schema, destination_table, ogr_db_string))

    except Exception as e:
        logger.error(f'Error downloading IFPRS: {e}... continuing')
        pass
    return header, destination_table

@app.task()
def ifprs_finalize_task(schema, insert_table, destination_table):

    swap_buffer_table(schema, destination_table)
    ifprs_insert(schema, insert_table)
    ifprs_treatment_date(schema, insert_table)
    ifprs_status_consolidation(schema, insert_table)

@app.task()
def ifprs_download_and_insert(schema, insert_table, wkid, ifprs_url, ogr_db_string):
    # IFPRS processing and insert
    conn = create_db_conn_from_envs()
    header, destination_table = update_ifprs(schema, wkid, ifprs_url, ogr_db_string)
    chord(header)(ifprs_finalize_task.s(schema, insert_table, destination_table))

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

@app.task()
def common_attributes_download_and_insert(projection, ogr_db_string, schema, treatment_index, facts_haz_table):
    conn = create_db_conn_from_envs()

    common_attributes_fc_name = 'Actv_CommonAttribute_PL'
    urls = [
    'https://data.fs.usda.gov/geodata/edw/edw_resources/fc/Actv_CommonAttribute_PL_Region01.zip',
    'https://data.fs.usda.gov/geodata/edw/edw_resources/fc/Actv_CommonAttribute_PL_Region02.zip',
    'https://data.fs.usda.gov/geodata/edw/edw_resources/fc/Actv_CommonAttribute_PL_Region03.zip',
    'https://data.fs.usda.gov/geodata/edw/edw_resources/fc/Actv_CommonAttribute_PL_Region04.zip',
    'https://data.fs.usda.gov/geodata/edw/edw_resources/fc/Actv_CommonAttribute_PL_Region05.zip',
    'https://data.fs.usda.gov/geodata/edw/edw_resources/fc/Actv_CommonAttribute_PL_Region06.zip',
    'https://data.fs.usda.gov/geodata/edw/edw_resources/fc/Actv_CommonAttribute_PL_Region08.zip',
    'https://data.fs.usda.gov/geodata/edw/edw_resources/fc/Actv_CommonAttribute_PL_Region09.zip',
    'https://data.fs.usda.gov/geodata/edw/edw_resources/fc/Actv_CommonAttribute_PL_Region10.zip'

    ]

    header = []

    for url in urls:
        header.append(common_attributes_processing.s(url, projection, common_attributes_fc_name, schema, ogr_db_string,
                                     facts_haz_table, treatment_index))

    chord(header)(common_attributes_type_filter.s(schema, treatment_index))


@app.task()
def common_attributes_type_filter(schema, treatment_index):
    conn = create_db_conn_from_envs()
    cursor = conn.cursor()
    with conn.transaction():
        cursor.execute(f'''           
            DELETE from {schema}.{treatment_index} 
            WHERE
            type IN (
                SELECT value from {schema}.common_attributes_lookup
                WHERE filter = 'type'
                AND include = 'FALSE')
            AND
            identifier_database = 'FACTS Common Attributes';
        ''')

@app.task()
def hazardous_fuels_download_and_insert(hazardous_fuels_table, facts_haz_gdb_url, facts_haz_gdb, out_wkid, facts_haz_fc_name,
                                        schema, insert_table, ogr_db_string):
    # FACTS Hazardous Fuels
    conn = create_db_conn_from_envs()
    hazardous_fuels_zip_file = f'{hazardous_fuels_table}.zip'
    download_file_from_url(facts_haz_gdb_url, hazardous_fuels_zip_file)
    extract_and_remove_zip_file(hazardous_fuels_zip_file)

    # special input srs for common attributes
    # https://gis.stackexchange.com/questions/112198/proj4-postgis-transformations-between-wgs84-and-nad83-transformations-in-alask
    # without modifying the proj4 srs with the towgs84 values, the data is not in the "correct" location
    input_srs = '+proj=longlat +datum=NAD83 +no_defs +type=crs +towgs84=-0.9956,1.9013,0.5215,0.025915,0.009426,0.011599,-0.00062'
    gdb_to_postgres(facts_haz_gdb, out_wkid, facts_haz_fc_name, hazardous_fuels_table,
                    schema, ogr_db_string, input_srs)
    hazardous_fuels_date_filtering(conn, schema, hazardous_fuels_table)
    hazardous_fuels_insert(conn, schema, insert_table, hazardous_fuels_table)
    remove_wildfire_non_treatment(conn, schema, insert_table)
