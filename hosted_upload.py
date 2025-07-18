from arcgis.gis import GIS
from dotenv import load_dotenv
import os

from sweri_utils.sql import connect_to_pg_db
from sweri_utils.hosted import hosted_upload_and_swizzle

from sweri_utils.sweri_logging import logging, log_this

if __name__ == '__main__':
    load_dotenv()
    logger = logging.getLogger(__name__)

    cur, conn = connect_to_pg_db(os.getenv('RDS_DB_HOST'), os.getenv('RDS_DB_PORT'), os.getenv('RDS_DB_NAME'),
                                 os.getenv('RDS_DB_USER'), os.getenv('RDS_DB_PASSWORD'))

    postgis_schema = os.getenv('RDS_DB_SCHEMA')
    start_objectid = 0
    chunk = 1000

    gis = GIS("https://gis.reshapewildfire.org/arcgis", os.getenv("ESRI_USER"), os.getenv("ESRI_PW"), expiration=120)

    # Treatment Index Points
    treatment_index_points_data_ids = [os.getenv('TREATMENT_INDEX_POINTS_DATA_ID_1'),
                                       os.getenv('TREATMENT_INDEX_POINTS_DATA_ID_2')]
    treatment_index_points_view_id = os.getenv('TREATMENT_INDEX_POINTS_VIEW_ID')
    treatment_index_points_table = 'treatment_index_points'

    hosted_upload_and_swizzle(gis, treatment_index_points_view_id, treatment_index_points_data_ids,
                              conn, postgis_schema, treatment_index_points_table, chunk, start_objectid)

    # Refresh gis since it lasts 1 hour
    gis = GIS("https://gis.reshapewildfire.org/arcgis", os.getenv("ESRI_USER"), os.getenv("ESRI_PW"), expiration=120)

    # Daily Progression
    daily_progression_data_ids = [os.getenv('DAILY_PROGRESSION_DATA_ID_1'), os.getenv('DAILY_PROGRESSION_DATA_ID_2')]
    daily_progression_view_id = os.getenv('DAILY_PROGRESSION_VIEW_ID')
    daily_progression_table = 'daily_progression'

    hosted_upload_and_swizzle(gis, daily_progression_view_id, daily_progression_data_ids,
                              conn, postgis_schema, daily_progression_table, chunk, start_objectid)

    # Refresh gis since it lasts 1 hour
    gis = GIS("https://gis.reshapewildfire.org/arcgis", os.getenv("ESRI_USER"), os.getenv("ESRI_PW"), expiration=120)

    # Treatment Index Polygons
    treatment_index_data_ids = [os.getenv('TREATMENT_INDEX_DATA_ID_1'), os.getenv('TREATMENT_INDEX_DATA_ID_2')]
    treatment_index_view_id = os.getenv('TREATMENT_INDEX_VIEW_ID')
    treatment_index_table = 'treatment_index'

    hosted_upload_and_swizzle(gis, treatment_index_view_id, treatment_index_data_ids, conn, postgis_schema,
                              treatment_index_table, chunk, start_objectid)
