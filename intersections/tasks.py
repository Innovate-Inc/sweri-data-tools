import sys
from os import path

from intersections.utils import create_db_conn_cursor_from_envs
from sweri_utils.sql import delete_from_table
from worker import app
import logging
import watchtower

logger = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',filename='./treatment_index.log', encoding='utf-8', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
cw = watchtower.CloudWatchLogHandler()
cw.setFormatter(logging.Formatter('%(asctime)s %(levelname)-8s %(message)s'))
logger.addHandler(cw)


@app.task
def calculate_intersections_and_insert(schema, insert_table, source_key, target_key):
    """
    Calculate intersections between features from two sources and insert the results into a specified table.
    ST_AREA(ST_TRANSFORM(ST_INTERSECTION(a.shape, b.shape),4326)::geography) * 0.000247105 as acre_overlap is used so we can calculate the geodesic area
    Args:
        schema (str): The name of the schema to use.
        insert_table (str): The name of the table to insert intersection results into.
        source_key (str): The key identifying the source features.
        target_key (str): The key identifying the target features.

    Returns:
        None
    """
    cursor, conn = create_db_conn_cursor_from_envs('DOCKER')
    with conn:
        # delete existing intersections and replace with new ones
        delete_from_table(cursor, schema, insert_table, f"id_1_source = '{source_key}' and id_2_source = '{target_key}'")
        logger.info(f'beginning intersections on {source_key} and {target_key}')
        query = f""" insert into {schema}.{insert_table} (acre_overlap, id_1, id_1_source, id_2, id_2_source)
             select ST_AREA(ST_TRANSFORM(ST_INTERSECTION(a.shape, b.shape),4326)::geography) * 0.000247105 as acre_overlap, 
             a.unique_id as id_1, 
             a.feat_source as id_1_source, 
             b.unique_id as id_2, 
             b.feat_source as id_2_source
             from {schema}.intersection_features a, {schema}.intersection_features b
             where ST_IsValid(a.shape) and ST_IsValid(b.shape) and ST_INTERSECTS (a.shape, b.shape)  
             and a.feat_source = '{source_key}'
             and b.feat_source = '{target_key}';"""
        cursor.execute(query)
        cursor.execute('COMMIT;')
        logger.info(f'completed intersections on {source_key} and {target_key}, inserted into {schema}.{insert_table} ')
