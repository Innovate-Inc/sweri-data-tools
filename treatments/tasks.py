import re
import os
from sweri_utils.files import gdb_to_postgres, download_file_from_url, extract_and_remove_zip_file
from worker import app
from sweri_utils.logging import logging
from sweri_utils.sql import connect_to_pg_db
from treatments.utils import add_fields_and_indexes, common_attributes_date_filtering, exclude_by_acreage, exclude_facts_hazardous_fuels, \
    include_fuel_activities, include_logging_activities, include_fire_activites, trim_whitespace, activity_filter, \
    include_other_activites, set_included, common_attributes_insert, common_attributes_treatment_date, common_attributes_type_filter

@app.task(time_limit=14400)
def capture_common_attributes_region(url, schema, projection, treatment_index):
    conn = connect_to_pg_db(os.getenv('DB_HOST'), os.getenv('DB_PORT'), os.getenv('DB_NAME'),
                            os.getenv('DB_USER'), os.getenv('DB_PASSWORD'))
    ogr_db_string = f"PG:dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')} port={os.getenv('DB_PORT')} host={os.getenv('DB_HOST')}"

    common_attributes_fc_name = 'Actv_CommonAttribute_PL'
    # expression pulls just the number out of the url, 01-10
    region_number = re.sub("\D", "", url)
    table_name = f'common_attributes_{region_number}'
    gdb = f'Actv_CommonAttribute_PL_Region{region_number}.gdb'

    zip_file = f'{table_name}.zip'

    logging.info(f'Downloading {url}')
    download_file_from_url(url, zip_file)

    logging.info(f'Extracting {zip_file}')
    extract_and_remove_zip_file(zip_file)

    gdb_to_postgres(gdb, projection, common_attributes_fc_name, table_name, schema, ogr_db_string)

    add_fields_and_indexes(conn, schema, table_name, region_number)

    common_attributes_date_filtering(conn, schema, table_name)
    exclude_by_acreage(conn, schema, table_name)
    exclude_facts_hazardous_fuels(conn, schema, table_name, facts_haz_table)

    trim_whitespace(conn, schema, table_name)

    include_logging_activities(conn, schema, table_name)
    include_fire_activites(conn, schema, table_name)
    include_fuel_activities(conn, schema, table_name)
    activity_filter(conn, schema, table_name)
    include_other_activites(conn, schema, table_name)

    set_included(conn, schema, table_name)

    common_attributes_insert(conn, schema, table_name, treatment_index)
    common_attributes_treatment_date(conn, schema, table_name, treatment_index)
    common_attributes_type_filter(conn, schema, treatment_index)
