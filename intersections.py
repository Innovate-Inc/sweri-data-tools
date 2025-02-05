import json
import logging
import os
import sys
from datetime import datetime
import requests as r
from dotenv import load_dotenv

from sweri_utils.conversion import create_csv_and_upload_to_s3
from sweri_utils.download import get_ids, get_all_features
from sweri_utils.s3 import upload_to_s3
from sweri_utils.sql import connect_to_pg_db, rename_postgres_table, insert_from_db, pg_copy_to_csv

import watchtower

logger = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', encoding='utf-8', level=logging.INFO,
                    datefmt='%Y-%m-%d %H:%M:%S')

logger.addHandler(watchtower.CloudWatchLogHandler())


def get_intersection_features(url, layer_id=0):
    intersection_r = r.get(url + f'/{layer_id}/query',
                           params={'f': 'json', 'where': '1=1', 'outFields': '*'})
    intersections_json = intersection_r.json()
    if 'features' not in intersections_json:
        raise KeyError('features')
    return intersections_json['features']


def configure_intersection_sources(features, coded_vals, start):
    intersection_sources = {}
    intersection_targets = {}

    for f in features:
        att = f['attributes']
        s = {
            'source': att['source'],
            'id': att['uid_fields'],
            'source_type': att['source_type'],
            'last_run': att['last_run'],
            'frequency_days': att['frequency_days']
        }
        if att['name'] in coded_vals:
            s['name'] = coded_vals[att['name']]

        att = att
        # always set targets
        if att['use_as_target'] == 1:
            intersection_targets[att['id_source']] = s
        if att['last_run'] is not None and (start - datetime.fromtimestamp(att['last_run'] / 1000)).days < att[
            'frequency_days']:
            logger.info(f'skipping {s["name"] if s["name"] else att["id_source"]}, last run less than frequency')
            continue

        intersection_sources[att['id_source']] = s

    return intersection_sources, intersection_targets


def update_last_run(features, start_time, url, layer_id):
    """
    Updates the `last_run` attribute for a list of features in a specified layer.

    Args:
        features (list): A list of feature dictionaries to update.
        start_time (datetime): The start time to set as the new `last_run` timestamp.
        url (str): The base URL of the feature service.
        layer_id (int): The ID of the layer to update.

    Raises:
        ValueError: If the update request fails or the response is missing `updateResults`.
    """

    update_feat = json.dumps([
        {
            'attributes': {
                'objectid': f['attributes']['objectid'],
                'last_run': start_time.timestamp() * 1000
            }
        } for f in features
    ])

    update_r = r.post(url + f'/{layer_id}/updateFeatures', params={'f': 'json', 'features': update_feat})
    if 'updateResults' not in update_r.json():
        raise ValueError('update failed: missing update results')
    errors = [e for e in update_r.json()['updateResults'] if e['success'] is False]
    if len(errors) > 0:
        raise ValueError(f'update failed: {errors}')
    logger.info('completed last run update')


def query_coded_value_domain(url, layer):
    domain_r = r.get(url + f'/queryDomains',
                     params={'f': 'json', 'layers': layer})
    d_json = domain_r.json()
    if 'domains' not in d_json or ('domains' in d_json and len(d_json['domains']) == 0):
        raise ValueError('missing domains')
    if 'codedValues' not in d_json['domains'][0]:
        raise ValueError('missing coded values or incorrect domain type')
    cv = d_json['domains'][0]['codedValues']
    return {c['code']: c['name'] for c in cv}


def calculate_intersections_and_insert(cursor, schema, insert_table, source_key, target_key):
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
    cursor.execute('BEGIN;')
    cursor.execute(query)
    cursor.execute('COMMIT;')
    logger.info(f'completed intersections on {source_key} and {target_key}, inserted into {schema}.{insert_table} ')


def calculate_intersections_from_sources(intersect_sources, intersect_targets, new_intersections_name, cursor, schema):
    for source_key, source_value in intersect_sources.items():
        for target_key, target_value in intersect_targets.items():
            if target_key == source_key:
                continue
            logger.info(f'performing intersections on {source_key} and {target_key}')
            calculate_intersections_and_insert(cursor, schema, new_intersections_name, source_key, target_key)
            logger.info(f'completed intersections on {source_key} and {target_key}')


def swap_intersection_tables(cursor, schema):
    """
    swaps new intersections and existing intersections table
    :param cursor:
    :param schema: target schema
    :return:
    """
    logger.info('moving to postgres table updates')
    # rename backup backup to temp table to make space for new backup
    rename_postgres_table(cursor, schema, 'intersections_backup',
                          'intersections_backup_temp')
    logger.info(f'{schema}.intersections_backup renamed to {schema}.intersections_backup_temp')

    # rename current table to backup table
    rename_postgres_table(cursor, schema, 'intersections', 'intersections_backup')
    logger.info(f'{schema}.intersections renamed to {schema}.intersections_backup')

    # rename new intersections table to new data
    rename_postgres_table(cursor, schema, 'new_intersections', 'intersections')
    logger.info(f'{schema}.new_intersections renamed to {schema}.intersections')

    # drop temp backup table
    cursor.execute(f'DROP TABLE IF EXISTS {schema}.intersections_backup_temp CASCADE;')
    logger.info(f'{schema}.intersections_backup_temp deleted')


def fetch_features_and_dump_geojson(service_url, where, geometry=None, geom_type=None, out_sr=4326,
                                    out_fields=None, chunk_size=100):
    ids = get_ids(service_url, where, geometry, geom_type)
    out_features = []
    # get all features
    for f in get_all_features(service_url, ids, out_sr, out_fields, chunk_size, 'geojson'):
        # change to insert directly into db
        out_features += f

    if len(out_features) == 0:
        raise Exception(f'No features fetched for ids: {ids}')
    return out_features


def insert_feature_into_db(cursor, target_table, feature, fc_name, id_field):
    if ('geometry' not in feature) or ('properties' not in feature):
        raise KeyError('missing geometry or properties')
    if id_field not in feature['properties']:
        raise KeyError(f'missing or incorrect id field: {id_field}')

    json_geom = json.dumps(feature['geometry'])
    q = f"INSERT INTO {target_table} (unique_id, feat_source, shape) VALUES ('{feature['properties'][id_field]}', '{fc_name}',ST_SetSRID(ST_GeomFromGeoJSON('{json_geom}'), 4326));"
    try:
        cursor.execute('BEGIN;')
        cursor.execute(q)
        cursor.execute('COMMIT;')
    except Exception as e:
        logger.error(f'error inserting feature: {e}, {q}')
        cursor.execute('ROLLBACK;')


def configure_intersection_features_table(cursor, schema):
    logger.info('moving to postgres table updates')
    # drop temp backup table
    cursor.execute(f'DROP TABLE IF EXISTS {schema}.intersection_features_backup_temp CASCADE;')
    logger.info(f'{schema}.intersection_features_backup_temp deleted')
    # rename backup backup to temp table to make space for new backup
    rename_postgres_table(cursor, schema, 'intersection_features_backup',
                          'intersection_features_backup_temp')
    logger.info(
        f'{schema}.intersection_features_backup renamed to {schema}.intersection_features_backup_temp')

    # rename current table to backup table
    rename_postgres_table(cursor, schema, 'intersection_features', 'intersection_features_backup')
    logger.info(f'{schema}.intersection_features renamed to {schema}.intersection_features_backup')
    # rename new intersections table to new data
    cursor.execute(
        f'CREATE TABLE {schema}.intersection_features AS SELECT * FROM {schema}.intersection_features_backup')
    logger.info(f'created {schema}.intersection_features from {schema}.intersection_features_backup')


def configure_new_intersections_table(cursor, schema):
    logger.info('moving to postgres table updates')
    # drop temp backup table
    cursor.execute(f'DROP TABLE IF EXISTS {schema}.new_intersections CASCADE;')
    logger.info(f'{schema}.new_intersection deleted')
    # rename backup backup to temp table to make space for new backup
    cursor.execute(f'CREATE TABLE {schema}.new_intersections AS TABLE {schema}.intersections WITH NO DATA;')
    logger.info(f'created {schema}.new_intersections from {schema}.intersections')


def delete_intersection_features(cursor, schema, source):
    delete_feat_q = f"DELETE FROM {schema}.intersection_features WHERE feat_source = '{source}';"
    logger.info(f'running {delete_feat_q}')
    cursor.execute('BEGIN;')
    cursor.execute(delete_feat_q)
    cursor.execute('COMMIT;')
    logging.info(f'deleted feat_source: {source} from {schema}.intersection_features')


def fetch_features_to_intersect(intersect_sources, cursor, schema, insert_table, wkid):
    for key, value in intersect_sources.items():
        if value['source_type'] == 'url':
            out_feat = fetch_features_and_dump_geojson(value['source'], 'SHAPE IS NOT NULL', None, None, wkid)
            for f in out_feat:
                insert_feature_into_db(cursor, f'{schema}.{insert_table}', f, key, value['id'])
        elif value['source_type'] == 'db_table':
            insert_from_db(cursor, schema, insert_table,
                           ('unique_id', 'feat_source'), value['source'],
                           # do not need to specify object id as we are using sde.net_rowid() in the insert
                           (value['id'], f"'{key}'"))
        else:
            raise ValueError('invalid source type: {}'.format(value['source_type']))


def refresh_spatial_index_analyze(cursor, schema, table):
    logger.info(f'refreshing spatial index on {schema}.{table}')
    cursor.execute('BEGIN;')
    cursor.execute(f'DROP INDEX IF EXISTS {table}_shape_idx;')
    # recreate index
    cursor.execute(f'CREATE INDEX {table}_shape_idx ON {schema}.{table} USING GIST (shape);')
    cursor.execute('COMMIT;')
    # run VACUUM ANALYZE to help with performance
    cursor.execute(f'VACUUM ANALYZE {schema}.{table};')
    logger.info(f'refreshed spatial index on {schema}.{table}')


if __name__ == '__main__':
    logger.info('starting intersection processing')
    load_dotenv('.env')
    script_start = datetime.now()
    wkid = 4326
    # get db schema
    db_schema = os.getenv('SCHEMA')
    db_host = os.getenv('DB_HOST')
    db_port = int(os.getenv('DB_PORT'))
    db_name = os.getenv('DB_NAME')
    db_user = os.getenv('DB_USER')
    db_pw = os.getenv('DB_PASSWORD')

    # psycopg2 connection, because arcsde connection is extremely slow during inserts
    pg_cursor, conn = connect_to_pg_db(db_host, db_port, db_name, db_user, db_pw)

    # configure intersection sources
    intersection_src_url = os.getenv('INTERSECTION_SOURCES_URL')

    ############## setting intersection sources ################
    intersections = get_intersection_features(intersection_src_url)
    # handle coded value domains
    cvs = query_coded_value_domain(intersection_src_url, 0)
    intersect_sources, intersect_targets = configure_intersection_sources(intersections, cvs, script_start)

    ############## setting up intersection features ################
    # setup intersection features table
    configure_intersection_features_table(pg_cursor, db_schema)
    # delete source features and fetch new ones
    for src in intersect_sources.keys():
        delete_intersection_features(pg_cursor, db_schema, src)

    ############## fetching features ################
    # get latest features based on source
    fetch_features_to_intersect(intersect_sources, pg_cursor, db_schema, 'intersection_features', wkid)
    # refresh the spatial index
    refresh_spatial_index_analyze(pg_cursor, db_schema, 'intersection_features')

    ############## calculating intersections ################
    # setup table
    configure_new_intersections_table(pg_cursor, db_schema)
    # calculate intersections
    calculate_intersections_from_sources(intersect_sources, intersect_targets, 'new_intersections', pg_cursor, db_schema)
    # create the template for the new intersect
    swap_intersection_tables(pg_cursor, db_schema)

    ############## update run info on intersection sources table ################
    update_last_run(intersections, script_start, intersection_src_url, 0)
    ############## write to csv and upload to s3 ################
    logger.info('uploading csv to s3')
    create_csv_and_upload_to_s3(pg_cursor, db_schema, 'intersections', f'intersections_{db_schema}.csv',
                                os.getenv('S3_BUCKET'))
    logger.info('completed upload to s3')
    conn.close()

    logger.info('completed intersection processing')
