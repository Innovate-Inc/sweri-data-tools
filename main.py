import logging
import os
import sys
from datetime import datetime

from dotenv import load_dotenv

from intersections.sweri_intersections import run_intersections
from sweri_utils.sql import connect_to_pg_db
from treatment_index import run_treatment_index

if __name__ == "__main__":
    logging.info('starting data processing')
    load_dotenv('.env')
    script_start = datetime.now()
    sr_wkid = 4326

    # GIS user credentials
    root_site_url = os.getenv('ESRI_ROOT_URL')
    portal_url = os.getenv('ESRI_PORTAL_URL')
    portal_user = os.getenv('ESRI_USER')
    portal_password = os.getenv('ESRI_PW')

    # treatment index specific environment variables
    facts_haz_gdb_url = os.getenv('FACTS_GDB_URL')
    ifprs_url = os.getenv('IFPRS_URL')
    nfpors_url = os.getenv('NFPORS_URL')

    #intersection specific environment variables
    intersection_src_url = os.getenv('INTERSECTION_SOURCES_URL')
    intersection_src_view_url = os.getenv('INTERSECTION_SOURCES_VIEW_URL')

    # views and data sources
    intersections_view_id = os.getenv('INTERSECTIONS_VIEW_ID')
    intersections_data_ids = [os.getenv('INTERSECTIONS_DATA_ID_1'), os.getenv('INTERSECTIONS_DATA_ID_2')]

    treatment_index_view_id = os.getenv('TREATMENT_INDEX_VIEW_ID')
    treatment_index_data_ids = [os.getenv('TREATMENT_INDEX_DATA_ID_1'), os.getenv('TREATMENT_INDEX_DATA_ID_2')]
    additional_polygon_view_ids = [os.getenv('TREATMENT_INDEX_AGENCY_VIEW_ID'),
                                   os.getenv('TREATMENT_INDEX_CATEGORY_VIEW_ID')]

    treatment_index_points_view_id = os.getenv('TREATMENT_INDEX_POINTS_VIEW_ID')
    additional_point_view_ids = [os.getenv('TREATMENT_INDEX_AGENCY_POINTS_VIEW_ID'),
                                 os.getenv('TREATMENT_INDEX_CATEGORY_POINTS_VIEW_ID')]
    treatment_index_points_data_ids = [os.getenv('TREATMENT_INDEX_POINTS_DATA_ID_1'),
                                       os.getenv('TREATMENT_INDEX_POINTS_DATA_ID_2')]

    # s3 details for filegdb export
    s3_bucket = os.getenv('S3_BUCKET')
    s3_obj_name = os.getenv('S3_OBJ_NAME')

    # cache info for treatment index
    response_cache_info = {os.getenv('RESPONSE_CACHE_BUCKET_NAME'): [
        os.getenv('TREATMENT_INDEX_RESPONSE_CACHE_PREFIX'),
        os.getenv('TREATMENT_INDEX_POINTS_RESPONSE_CACHE_PREFIX')
    ]}

    ############### database connections ################
    # local docker db environment variables
    db_schema = os.getenv('SCHEMA')

    ogr_db_string = f"PG:dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')} port={os.getenv('DB_PORT')} host={os.getenv('DB_HOST')}"
    insert_table = 'treatment_index'
    treatment_index_points_table = 'treatment_index_points'
    ############## processing in docker ################
    try:
        # Get current day and env run day for treatment index
        ti_run_day_index = int(os.getenv('TI_RUN_DAY_INDEX'))
        day_of_week_index = datetime.now().weekday()

        # If today is run day
        if ti_run_day_index == day_of_week_index:
            treatments_pg_conn = connect_to_pg_db(os.getenv('DB_HOST'), int(os.getenv('DB_PORT')) if os.getenv('DB_PORT') else 5432,
                               os.getenv('DB_NAME'), os.getenv('DB_USER'), os.getenv('DB_PASSWORD'))
            run_treatment_index(treatments_pg_conn, db_schema, insert_table, ogr_db_string, sr_wkid, facts_haz_gdb_url,
                                nfpors_url, ifprs_url, root_site_url, portal_url, portal_user, portal_password,
                                treatment_index_view_id,
                                treatment_index_data_ids, additional_polygon_view_ids, treatment_index_points_view_id,
                                treatment_index_points_data_ids, additional_point_view_ids, s3_bucket, s3_obj_name, response_cache_info=response_cache_info)
        # reconnect to db after treatment index processing to avoid any connection issues for intersection processing
        intersections_pg_conn = connect_to_pg_db(os.getenv('DB_HOST'), int(os.getenv('DB_PORT')) if os.getenv('DB_PORT') else 5432,
                                   os.getenv('DB_NAME'), os.getenv('DB_USER'), os.getenv('DB_PASSWORD'))
        run_intersections(intersections_pg_conn, db_schema,
                          script_start, sr_wkid, intersection_src_url, intersection_src_view_url,
                          root_site_url,
                          portal_url,
                          portal_user,
                          portal_password, intersections_view_id, intersections_data_ids)
        logging.info(f'completed intersection processing, total runtime: {datetime.now() - script_start}')
    except Exception as e:
        logging.error(f'ERROR - data processing failed: {e}')
        sys.exit(1)
