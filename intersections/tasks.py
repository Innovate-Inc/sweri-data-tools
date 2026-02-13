from intersections.utils import insert_feature_into_db
from sweri_utils.download import get_ids, fetch_features
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
            # snapping the collection of target features to a grid before dissolving to prevent topology errors that can arise when dissolving features with very small gaps or overlaps
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
                        SELECT ST_UnaryUnion(ST_SnapToGrid(ST_Collect(shape), 0.000000001)) as shape, objectid, id_2_source
                        FROM intersection_data
                        GROUP BY objectid, id_2_source
                    ), 
                    dissolve_intersection_data AS (
                         SELECT ST_AREA(ST_TRANSFORM(ST_INTERSECTION(a.shape, b.shape), 4326)::geography) *
                                0.000247105   AS acre_overlap,
                                a.unique_id   AS id_1,
                                a.feat_source AS id_1_source,
                                b.id_2_source  AS id_2_source
                         FROM {schema}.intersection_features a,
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
                    FROM dissolve_intersection_data
                    WHERE acre_overlap > 0;
                    """
            cursor.execute(query)
        del cursor
    conn.close()
    del conn
    logger.info(f'completed intersections on {source_key} and {target_key}, inserted into {schema}.{insert_table} ')


@app.task
def insert_from_db_task(
        schema: str,
        insert_table: str,
        insert_fields: list[str],
        from_table: str,
        from_fields: list[str],
        from_shape: str = 'shape',
        to_shape: str = 'shape',
        wkid: int = 4326):
    conn = create_db_conn_from_envs()
    insert_from_db(
        conn,
        schema,
        insert_table,
        insert_fields,
        from_table,
        from_fields,
        from_shape,
        to_shape,
        wkid)


@app.task
def service_chunk_to_postgres(url, params, schema, destination_table, key, value, wkid):
    conn = create_db_conn_from_envs()
    logger.info(f'fetching geojson features from {value["source"]}')
    r = fetch_features(url + '/query', params, return_full_response=True)
    for f in r['features']:
        insert_feature_into_db(conn, destination_table, f, key, value['id'], schema, wkid)
