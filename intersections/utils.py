import json
import os

from sweri_utils.sql import connect_to_pg_db
import logging

def create_db_conn_from_envs():
    docker_db_host = os.getenv('DB_HOST')
    docker_db_port = int(os.getenv('DB_PORT'))
    docker_db_name = os.getenv('DB_NAME')
    docker_db_user = os.getenv('DB_USER')
    docker_db_password = os.getenv('DB_PASSWORD')
    return connect_to_pg_db(docker_db_host, docker_db_port, docker_db_name, docker_db_user, docker_db_password)


def insert_feature_into_db(conn, target_table, feature, fc_name, id_field, schema, to_srid=4326):
    if ('geometry' not in feature) or ('properties' not in feature):
        raise KeyError('missing geometry or properties')
    if id_field not in feature['properties']:
        raise KeyError(f'missing or incorrect id field: {id_field}')

    json_geom = json.dumps(feature['geometry'])
    q = f"INSERT INTO {target_table} (objectid, unique_id, feat_source, shape) VALUES (sde.next_rowid('{schema}', '{target_table}'),'{feature['properties'][id_field]}', '{fc_name}',ST_MakeValid(ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON('{json_geom}'), 4326), {to_srid})));"
    cursor = conn.cursor()
    try:
        with conn.transaction():
            cursor.execute(q)
    except Exception as err:
        logging.error(f'error inserting feature: {err}, {q}')