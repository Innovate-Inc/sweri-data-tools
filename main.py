import logging
import os
import sys
import time
from datetime import datetime

from dotenv import load_dotenv

from intersections.sweri_intersections import run_intersections
from sweri_utils.sql import connect_to_pg_db
from sweri_utils.sweri_logging import log_this

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

    #intersection specific environment variables
    intersection_src_url = os.getenv('INTERSECTION_SOURCES_URL')
    intersection_src_view_url = os.getenv('INTERSECTION_SOURCES_VIEW_URL')
    # views
    intersections_view_id = os.getenv('INTERSECTIONS_VIEW_ID')
    intersections_data_ids = [os.getenv('INTERSECTIONS_DATA_ID_1'), os.getenv('INTERSECTIONS_DATA_ID_2')]

    ############### database connections ################
    # local docker db environment variables
    db_schema = os.getenv('SCHEMA')
    pg_conn = connect_to_pg_db(os.getenv('DB_HOST'), int(os.getenv('DB_PORT')) if os.getenv('DB_PORT') else 5432,
                               os.getenv('DB_NAME'), os.getenv('DB_USER'), os.getenv('DB_PASSWORD'))
    ############## processing in docker ################
    try:
        # TODO: add treatment index processing here
        run_intersections(pg_conn, db_schema,
                          script_start, sr_wkid, intersection_src_url, intersection_src_view_url,
                          root_site_url,
                          portal_url,
                          portal_user,
                          portal_password, intersections_view_id, intersections_data_ids)
        logging.info(f'completed intersection processing, total runtime: {datetime.now() - script_start}')
    except Exception as e:
        logging.error(f'ERROR - data processing failed: {e}')
        sys.exit(1)