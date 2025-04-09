import json
import logging
import os
from datetime import datetime
from celery import group

import requests as r
from dotenv import load_dotenv

from utils import create_db_conn_cursor_from_envs
from tasks import calculate_intersections_and_insert
from sweri_utils.conversion import create_csv_and_upload_to_s3, create_coded_val_dict
from sweri_utils.download import fetch_features, fetch_geojson_features
from sweri_utils.s3 import import_s3_csv_to_postgres_table
from sweri_utils.sql import  rename_postgres_table,  refresh_spatial_index, \
    rotate_tables, copy_table_across_servers, delete_from_table, run_vacuum_analyze,  \
    calculate_index_for_fields
import watchtower

logger = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', encoding='utf-8', level=logging.INFO,
                    datefmt='%Y-%m-%d %H:%M:%S')
# logger.addHandler(watchtower.CloudWatchLogHandler())
logger.setLevel(logging.INFO)


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
            'frequency_days': att['frequency_days'],
            'name': att['name']
        }

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


def calculate_intersections_from_sources(intersect_sources, intersect_targets, new_intersections_name, cursor, schema):
    t = []
    for source_key, source_value in intersect_sources.items():
        for target_key, target_value in intersect_targets.items():
            if target_key == source_key:
                continue
            # logger.info(f'performing intersections on {source_key} and {target_key}')
            t.append(calculate_intersections_and_insert.s(schema, new_intersections_name, source_key, target_key))
            # logger.info(f'completed intersections on {source_key} and {target_key}')
    g = group(t)()
    g.get()

def insert_feature_into_db(cursor, target_table, feature, fc_name, id_field, to_srid=3857):
    if ('geometry' not in feature) or ('properties' not in feature):
        raise KeyError('missing geometry or properties')
    if id_field not in feature['properties']:
        raise KeyError(f'missing or incorrect id field: {id_field}')

    json_geom = json.dumps(feature['geometry'])
    q = f"INSERT INTO {target_table} (unique_id, feat_source, shape) VALUES ('{feature['properties'][id_field]}', '{fc_name}',ST_MakeValid(ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON('{json_geom}'), 4326), {to_srid})));"
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
    # drop existing intersections table
    cursor.execute(f'DROP TABLE IF EXISTS {schema}.new_intersections CASCADE;')
    logger.info(f'{schema}.new_intersection deleted')
    # recreate new intersections from existing intersections
    cursor.execute(f'CREATE TABLE {schema}.new_intersections AS SELECT * FROM {schema}.intersections;')
    logger.info(f'created {schema}.new_intersections from {schema}.intersections')


def fetch_features_to_intersect(intersect_sources, docker_cursor, docker_schema, insert_table, rds_cursor, rds_schema,
                                wkid):

    for key, value in intersect_sources.items():
        # remove existing features
        delete_from_table(docker_cursor, docker_schema, insert_table, f"feat_source = '{key}'")
        if value['source_type'] == 'url':
            logger.info(f'fetching geojson features from {value["source"]}')
            out_feat = fetch_geojson_features(value['source'], 'SHAPE IS NOT NULL', None, None, wkid)
            for f in out_feat:
                insert_feature_into_db(docker_cursor, f'{docker_schema}.{insert_table}', f, key, value['id'], wkid)
        elif value['source_type'] == 'db_table':
            logger.info(f'copying data from rds db for {value["source"]}')
            # this will copy the current table from the production server and use that data for intersections, and remove the older source
            copy_table_across_servers(
                rds_cursor,
                rds_schema,
                value['source'],
                docker_cursor,
                docker_schema,
                insert_table,
                [value['id'], f"'{key}' as feat_source", f'ST_MakeValid(ST_TRANSFORM(shape, {wkid}))'],
                ['unique_id', 'feat_source', 'shape'])
        else:
            raise ValueError('invalid source type: {}'.format(value['source_type']))
    logger.info(f'Removing null shapes from {docker_schema}.{insert_table}')
    docker_cursor.execute('BEGIN;')
    # remove null shapes and unique ids
    docker_cursor.execute(f"DELETE FROM {docker_schema}.{insert_table} WHERE shape IS NULL OR unique_id is NULL;")
    docker_cursor.execute('COMMIT;')
    logger.info(f'Finished populating {docker_schema}.{insert_table}')


def calculate_intersections_and_swap_tables(docker_db_cursor, docker_schema, intersect_sources, intersect_targets):
    # setup table
    configure_new_intersections_table(docker_db_cursor, docker_schema)
    # calculate intersections
    calculate_intersections_from_sources(intersect_sources, intersect_targets, 'new_intersections', docker_db_cursor,
                                         docker_schema)
    # create the template for the new intersect
    rotate_tables(docker_db_cursor, docker_schema, 'intersections', 'intersections_backup', 'new_intersections',
                  drop_temp=True)


def update_intersections_rds_db(rds_cursor, rds_schema, s3_bucket):
    logger.info('importing intersections csv into postgres')
    import_s3_csv_to_postgres_table(rds_cursor,
                                    rds_schema,
                                    ['objectid', 'id_1', 'id_2', 'id_1_source', 'id_2_source', 'acre_overlap'],
                                    'intersections_s3',
                                    s3_bucket,
                                    f'intersections_{rds_schema}.csv')
    # swap the tables in the rds db
    logger.info('swapping tables')
    rotate_tables(rds_cursor, rds_schema, 'intersections', 'intersections_backup', 'intersections_s3',
                  drop_temp=False)
    # update indices
    calculate_index_for_fields(rds_cursor, rds_schema, 'intersections',
                               ['objectid', 'id_1', 'id_2', 'id_1_source', 'id_2_source', 'acre_overlap'])


def update_intersection_features_rds_db(docker_cursor, docker_schema, rds_cursor, rds_schema):
    # we do not use the object id column in docker, but it is required in the rds db
    create_intersection_features_objectid(docker_cursor, docker_schema)
    copy_table_across_servers(docker_cursor, docker_schema, 'intersection_features', rds_cursor, rds_schema,
                              'intersection_features_dump', ['objectid', 'unique_id', 'feat_source', 'shape'],
                              ['objectid', 'unique_id', 'feat_source', 'shape'], True)
    logger.info('importing intersection_features into postgres')
    # swap the tables in the rds db
    logger.info('swapping tables')
    rotate_tables(rds_cursor, rds_schema, 'intersection_features', 'intersection_features_backup',
                  'intersection_features_dump', drop_temp=False)
    # update indices
    calculate_index_for_fields(rds_cursor, rds_schema, 'intersection_features',
                               ['objectid', 'unique_id', 'unique_id', 'feat_source'], True)


def create_intersection_features_objectid(docker_cursor, docker_schema):
    docker_cursor.execute('BEGIN;')
    docker_cursor.execute(f'''
            DROP SEQUENCE IF EXISTS intersection_features_objectid_seq;
            CREATE SEQUENCE intersection_features_objectid_seq START 1;
            UPDATE {docker_schema}.intersection_features set objectid = nextval('intersection_features_objectid_seq')
        ''')
    docker_cursor.execute('COMMIT;')


def run_intersections(docker_db_cursor, docker_conn, docker_schema, rds_db_cursor, rds_db_conn, rds_schema, s3_bucket,
                      start, wkid, intersection_source_list_url):
    ############## setting intersection sources ################
    intersections = fetch_features(f'{intersection_source_list_url}/0/query',
                                   {'f': 'json', 'where': '1=1', 'outFields': '*', 'orderByFields': 'source_type ASC'})

    intersect_sources, intersect_targets = configure_intersection_sources(intersections, script_start)
    
    if len(intersect_sources.keys()) == 0:
        logging.info('no intersections to run')
        return

    ############## setting up intersection features ################
    configure_intersection_features_table(docker_db_cursor, docker_schema)
    ############## fetching features ################
    # get latest features based on source
    fetch_features_to_intersect(intersect_sources, docker_db_cursor, docker_schema, 'intersection_features',
                               rds_db_cursor, rds_schema, wkid)
    # refresh the spatial index
    refresh_spatial_index(docker_db_cursor, docker_schema, 'intersection_features')
    
    # run VACUUM ANALYZE to increase performance after bulk updates
    run_vacuum_analyze(docker_conn, docker_db_cursor, docker_schema, 'intersection_features')
    ############## calculate intersections ################
    calculate_intersections_and_swap_tables(docker_db_cursor, docker_schema, intersect_sources, intersect_targets)

    ############## write to csv, upload to s3, and swap in new intersections ################
    logger.info('uploading csv to s3')
    create_csv_and_upload_to_s3(docker_db_cursor, docker_schema, 'intersections',
                                ['id_1', 'id_2', 'id_1_source', 'id_2_source', 'acre_overlap'],
                               f'intersections_{docker_schema}.csv', s3_bucket)
    logger.info('completed upload to s3')
    update_intersections_rds_db(rds_db_cursor, rds_schema, s3_bucket)

    ############# update intersection features table ################
    update_intersection_features_rds_db(docker_db_cursor, docker_schema, rds_db_cursor, rds_schema)

    ############# update run info on intersection sources table ################
    update_last_run(intersections, start, intersection_source_list_url, 0)

    # close connections
    docker_conn.close()
    rds_db_conn.close()

if __name__ == '__main__':
    logger.info('starting intersection processing')
    load_dotenv('../.env')
    script_start = datetime.now()
    sr_wkid = 3857

    # s3 bucket used for intersections
    s3_bucket_name = os.getenv('S3_BUCKET')
    # configure intersection sources
    intersection_src_url = os.getenv('INTERSECTION_SOURCES_URL')
    ############### database connections ################
    # local docker db environment variables
    docker_db_schema = os.getenv('DOCKER_DB_SCHEMA')
    docker_pg_cursor, docker_pg_conn = create_db_conn_cursor_from_envs('DOCKER')

    # rds db params
    rds_db_schema = os.getenv('RDS_SCHEMA')
    rds_pg_cursor, rds_pg_conn = create_db_conn_cursor_from_envs('RDS')
    ############## intersections processing in docker ################
    # function that runs everything for creating new intersections in docker, uploading the results to s3, and swapping the tables on the rds instance
    run_intersections(docker_pg_cursor, docker_pg_conn, docker_db_schema, rds_pg_cursor, rds_pg_conn, rds_db_schema,
                      s3_bucket_name,
                      script_start, sr_wkid, intersection_src_url)
    logger.info(f'completed intersection processing, total runtime: {datetime.now() - script_start}')
