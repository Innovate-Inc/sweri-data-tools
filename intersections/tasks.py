from intersections.utils import insert_feature_into_db
from sweri_utils.download import get_ids, get_all_features
from sweri_utils.sweri_logging import log_this
from sweri_utils.sql import delete_from_table, insert_from_db, create_db_conn_from_envs
from worker import app
import logging

# import watchtower

logger = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', filename='./treatment_index.log',
                    encoding='utf-8', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')


# cw = watchtower.CloudWatchLogHandler()
# cw.setFormatter(logging.Formatter('%(asctime)s %(levelname)-8s %(message)s'))
# logger.addHandler(cw)


@app.task(time_limit=1440000)
def calculate_intersections_and_insert(schema, insert_table, source_key, target_key, source_object_ids):
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
    conn = create_db_conn_from_envs()
    with conn:
        logger.info(f'beginning intersections on {source_key} and {target_key}')
        cursor = conn.cursor()
        with conn.transaction():
            query = f"""
                    WITH intersection_data AS (
                        SELECT
                            ST_AREA(ST_TRANSFORM(ST_INTERSECTION(a.shape, b.shape),4326)::geography) * 0.000247105 AS acre_overlap,
                            a.unique_id AS id_1,
                            a.feat_source AS id_1_source,
                            b.unique_id AS id_2,
                            b.feat_source AS id_2_source,
                            a.objectid AS objectid,
                            b.shape as shape
                        FROM {schema}.intersection_features a, {schema}.intersection_features b
                        WHERE a.objectid IN {source_object_ids} AND b.feat_source = '{target_key}' and ST_INTERSECTS(a.shape, b.shape)
                    ),
                    target_union AS (
                        SELECT ST_UnaryUnion(ST_Collect(shape)) as shape, objectid, id_2_source
                        FROM intersection_data
                        GROUP BY objectid, id_2_source 
                    ), 
                    dissolve_intersection_data AS (
                         SELECT ST_AREA(ST_TRANSFORM(ST_INTERSECTION(a.shape, b.shape), 4326)::geography) *
                                0.000247105   AS acre_overlap,
                                a.unique_id   AS id_1,
                                a.feat_source AS id_1_source,
                                b.id_2_source  AS id_2_source
                         FROM sweri.intersection_features a,
                              target_union b
                         WHERE a.objectid = b.objectid
                    ),
                    first_insert AS (
                        INSERT INTO {schema}.{insert_table} (acre_overlap, id_1, id_1_source, id_2, id_2_source)
                        SELECT acre_overlap, id_1, id_1_source, id_2, id_2_source
                        FROM intersection_data
                        WHERE acre_overlap > 0
                    )

                    INSERT INTO {schema}.{insert_table} (acre_overlap, id_1, id_1_source, id_2, id_2_source)
                    SELECT acre_overlap, id_1, id_1_source, 'dissolve', id_2_source
                    FROM dissolve_intersection_data;
                    """
            cursor.execute(query)
        del cursor
    conn.close()
    del conn
    logger.info(f'completed intersections on {source_key} and {target_key}, inserted into {schema}.{insert_table} ')


@app.task(time_limit=1440000)
def fetch_and_insert_intersection_features(key, value, wkid, docker_schema, insert_table):
    docker_conn = create_db_conn_from_envs()
    delete_from_table(docker_conn, docker_schema, insert_table, f"feat_source = '{key}'")
    if value['source_type'] == 'url':
        logger.info(f'fetching geojson features from {value["source"]}')
        ids = get_ids(value['source'], 'SHAPE IS NOT NULL', None, None)
        for chunk in get_all_features(value['source'], ids, wkid, out_fields=None, chunk_size=value['chunk_size'], format='geojson'):
            for f in chunk:
                insert_feature_into_db(docker_conn, insert_table, f, key, value['id'], docker_schema, wkid)
    elif value['source_type'] == 'db_table':
        logger.info(f'copying data from rds db for {value["source"]}')
        insert_from_db(
            docker_conn,
            docker_schema,
            insert_table,
            ['unique_id', 'feat_source'],
            value['source'],
            [value['id'], f"'{key}' as feat_source"],
            'shape',
            'shape',
            wkid)
    else:
        raise ValueError('invalid source type: {}'.format(value['source_type']))
