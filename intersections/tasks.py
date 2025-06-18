from intersections.utils import create_db_conn_from_envs, insert_feature_into_db
from sweri_utils.download import fetch_geojson_features
from sweri_utils.logging import log_this
from sweri_utils.sql import delete_from_table, copy_table_across_servers, insert_from_db
from worker import app
import logging
# import watchtower

logger = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',filename='./treatment_index.log', encoding='utf-8', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
# cw = watchtower.CloudWatchLogHandler()
# cw.setFormatter(logging.Formatter('%(asctime)s %(levelname)-8s %(message)s'))
# logger.addHandler(cw)


@app.task(time_limit=14400)
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
    conn = create_db_conn_from_envs('DOCKER')
    with conn:
        logger.info(f'beginning intersections on {source_key} and {target_key}')
        cursor = conn.cursor()
        with conn.transaction():
            query = f""" 
                 delete from {schema}.{insert_table} where id_1_source = '{source_key}' and id_2_source = '{target_key}';
                 insert into {schema}.{insert_table} (objectid, acre_overlap, id_1, id_1_source, id_2, id_2_source)
                 select sde.next_rowid('{schema}', '{insert_table}'), ST_AREA(ST_TRANSFORM(ST_INTERSECTION(a.shape, b.shape),4326)::geography) * 0.000247105 as acre_overlap, 
                 a.unique_id as id_1, 
                 a.feat_source as id_1_source, 
                 b.unique_id as id_2, 
                 b.feat_source as id_2_source
                 from {schema}.intersection_features a, {schema}.intersection_features b
                 where ST_IsValid(a.shape) and ST_IsValid(b.shape) and ST_INTERSECTS (a.shape, b.shape)  
                 and a.feat_source = '{source_key}'
                 and b.feat_source = '{target_key}';"""
            cursor.execute(query)
            logger.info(f'completed intersections on {source_key} and {target_key}, inserted into {schema}.{insert_table} ')

@app.task(time_limit=14400)
def fetch_and_insert_intersection_features(key, value, wkid, docker_schema, insert_table):
    docker_conn= create_db_conn_from_envs()
    delete_from_table(docker_conn, docker_schema, insert_table, f"feat_source = '{key}'")
    if value['source_type'] == 'url':
        logger.info(f'fetching geojson features from {value["source"]}')
        out_feat = fetch_geojson_features(value['source'], 'SHAPE IS NOT NULL', None, None, wkid,
                                    out_fields=None, chunk_size=value['chunk_size'])
        for f in out_feat:
            insert_feature_into_db(docker_conn, insert_table, f, key, value['id'], docker_schema, wkid)
    elif value['source_type'] == 'db_table':
        logger.info(f'copying data from rds db for {value["source"]}')
        # this will copy the current table from the production server and use that data for intersections, and remove the older source
        insert_from_db(
            docker_conn,
            docker_schema,
            insert_table,
            ['unique_id', 'feat_source'],
            value['source'],
            [value['id'], f"'{key}' as feat_source"],
            'shape',
            'shape',
            wkid )
        # copy_table_across_servers(
        #     rds_conn,
        #     rds_schema,
        #     value['source'],
        #     docker_conn,
        #     docker_schema,
        #     insert_table,
        #     [value['id'], f"'{key}' as feat_source", f'ST_MakeValid(ST_TRANSFORM(shape, {wkid}))'],
        #     ['unique_id', 'feat_source', 'shape'])
    else:
        raise ValueError('invalid source type: {}'.format(value['source_type']))

