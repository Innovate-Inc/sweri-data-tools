import json
import os

from sweri_utils.sql import connect_to_pg_db
import logging


def insert_feature_into_db(conn, target_table, feature, fc_name, id_field, schema, to_srid=4326):
    if ('geometry' not in feature) or ('properties' not in feature):
        raise KeyError('missing geometry or properties')
    if id_field not in feature['properties']:
        raise KeyError(f'missing or incorrect id field: {id_field}')

    json_geom = json.dumps(feature['geometry'])
    q = f"INSERT INTO {schema}.{target_table} (objectid, unique_id, feat_source, shape) VALUES (sde.next_rowid('{schema}', '{target_table}'),'{feature['properties'][id_field]}', '{fc_name}', ST_CurveToLine(ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON('{json_geom}'), 4326), {to_srid})));"
    cursor = conn.cursor()
    try:
        with conn.transaction():
            cursor.execute(q)
    except Exception as err:
        logging.error(f'error inserting feature: {err}, {q}')


def chunk_it(source_object_ids: list, *, divide_factor=2, chunk_size=None):
    """
    Create a chunk from the source object IDs based on the divide factor
    Args:
        source_object_ids: list of ints
        divide_factor: int
        chunk_size: int

    Returns: multiple tuples based on divide_factor unless chunk_size is provided, then returns multiple tuples based on chunk_size

    """
    # Calculate new chunk size (at least 1)
    if chunk_size is None:
        chunk_size = max(1, len(source_object_ids) // divide_factor)

    # Process in smaller chunks
    for i in range(0, len(source_object_ids), chunk_size):
        yield source_object_ids[i:i + chunk_size]
