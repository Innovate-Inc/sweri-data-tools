import sys
from os import path
# for importing from sibling directories
sys.path.append( path.dirname( path.dirname( path.abspath(__file__) ) ) )

import json
import logging
import os
from datetime import datetime
from arcgis import GIS
from celery import group
import requests as r
from dotenv import load_dotenv
from sweri_utils.conversion import create_csv_and_upload_to_s3
from sweri_utils.download import fetch_features, fetch_geojson_features
from sweri_utils.s3 import import_s3_csv_to_postgres_table
from sweri_utils.sql import  rename_postgres_table,  refresh_spatial_index, \
    rotate_tables, copy_table_across_servers, delete_from_table, run_vacuum_analyze,  \
    calculate_index_for_fields
import watchtower

from intersections.utils import create_db_conn_from_envs
from intersections.tasks import calculate_intersections_and_insert, fetch_and_insert_intersection_features

logger = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', encoding='utf-8', level=logging.INFO,
                    datefmt='%Y-%m-%d %H:%M:%S')

cw_handler = watchtower.CloudWatchLogHandler()
cw_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)-8s %(message)s'))
logger.addHandler(cw_handler)
logger.setLevel(logging.INFO)

def configure_intersection_sources(features, start):
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


def update_last_run(features, start_time, url, layer_id, portal, user, password):

    # authenticate here as the script takes a while to run
    gis = GIS(url=portal, username=user, password=password)
    token = gis.session.auth.token
    update_feat = json.dumps([
        {
            'attributes': {
                'objectid': f['attributes']['objectid'],
                'last_run': start_time.timestamp() * 1000
            }
        } for f in features
    ])

    update_r = r.post(f'{url}/{layer_id}/updateFeatures', params={'f': 'json', 'features': update_feat, 'token': token})
    if 'updateResults' not in update_r.json():
        raise ValueError('update failed: missing update results')
    errors = [err for err in update_r.json()['updateResults'] if err['success'] is False]
    if len(errors) > 0:
        raise ValueError(f'update failed: {errors}')
    logger.info('completed last run update')


def calculate_intersections_from_sources(intersect_sources, intersect_targets, new_intersections_name, schema):
    t = []
    for source_key, source_value in intersect_sources.items():
        for target_key, target_value in intersect_targets.items():
            if target_key == source_key:
                continue
            logger.info(f'adding task for intersections on {source_key} and {target_key}')
            t.append(calculate_intersections_and_insert.s(schema, new_intersections_name, source_key, target_key))
    g = group(t)()
    g.get()
    logger.info('completed all intersection tasks')


def configure_intersection_features_table(conn, schema):
    logger.info('moving to postgres table updates')
    # drop temp backup table
    cursor = conn.cursor()
    with conn.transaction():
        cursor.execute(f'DROP TABLE IF EXISTS {schema}.intersection_features_backup_temp CASCADE;')
        logger.info(f'{schema}.intersection_features_backup_temp deleted')
        # rename backup to temp table to make space for new backup
        rename_postgres_table(conn, schema, 'intersection_features_backup',
                              'intersection_features_backup_temp')
        logger.info(
            f'{schema}.intersection_features_backup renamed to {schema}.intersection_features_backup_temp')

        # rename current table to back up table
        rename_postgres_table(conn, schema, 'intersection_features', 'intersection_features_backup')
        logger.info(f'{schema}.intersection_features renamed to {schema}.intersection_features_backup')
        # rename new intersections table to new data
        cursor.execute(
            f'CREATE TABLE {schema}.intersection_features AS SELECT * FROM {schema}.intersection_features_backup')
        logger.info(f'created {schema}.intersection_features from {schema}.intersection_features_backup')


def configure_new_intersections_table(conn, schema):
    logger.info('moving to postgres table updates')

    cursor = conn.cursor()
    with conn.transaction():
        # drop existing intersections table
        cursor.execute(f'DROP TABLE IF EXISTS {schema}.new_intersections CASCADE;')
        logger.info(f'{schema}.new_intersection deleted')
        # recreate new intersections from existing intersections
        cursor.execute(f'CREATE TABLE {schema}.new_intersections AS SELECT * FROM {schema}.intersections;')
        logger.info(f'created {schema}.new_intersections from {schema}.intersections')


def fetch_features_to_intersect(intersect_sources, docker_conn, docker_schema, insert_table, rds_conn, rds_schema,
                                wkid):
    tasks = []
    for key, value in intersect_sources.items():
        logger.info(f'adding task for fetching {key}')
        # remove existing features
        tasks.append(fetch_and_insert_intersection_features.s(key, value, wkid, docker_schema, rds_schema, insert_table))
    g = group(tasks)()
    g.get()
    logger.info(f'Removing null shapes from {docker_schema}.{insert_table}')
    # remove null shapes and unique ids
    docker_cursor = docker_conn.cursor()
    with docker_conn.transaction():
        # remove null shapes and unique ids
        docker_cursor.execute(f"DELETE FROM {docker_schema}.{insert_table} WHERE shape IS NULL OR unique_id is NULL;")
    logger.info(f'Finished populating {docker_schema}.{insert_table}')


def update_intersection_features_rds_db(docker_conn, docker_schema, rds_conn, rds_schema):
    docker_cursor = docker_conn.cursor()
    with docker_conn.transaction():
        docker_cursor.execute(f'''
                    DROP SEQUENCE IF EXISTS intersection_features_objectid_seq;
                    CREATE SEQUENCE intersection_features_objectid_seq START 1;
                    UPDATE {docker_schema}.intersection_features set objectid = nextval('intersection_features_objectid_seq')
                ''')

    copy_table_across_servers(docker_conn, docker_schema, 'intersection_features', rds_conn, rds_schema,
                              'intersection_features_dump', ['objectid', 'unique_id', 'feat_source', 'shape'],
                              ['objectid', 'unique_id', 'feat_source', 'shape'], True)
    logger.info('importing intersection_features into postgres')
    # swap the tables in the rds db
    logger.info('swapping tables')
    rotate_tables(rds_conn, rds_schema, 'intersection_features', 'intersection_features_backup',
                  'intersection_features_dump', drop_temp=False)
    # update indices
    calculate_index_for_fields(rds_conn, rds_schema, 'intersection_features',
                               ['objectid', 'unique_id', 'unique_id', 'feat_source'], True)

def run_intersections( docker_conn, docker_schema, rds_db_conn, rds_schema, s3_bucket,
                      start, wkid, intersection_source_list_url, intersection_source_view, portal, user, password):
    ############## setting intersection sources ################
    intersections = fetch_features( f'{intersection_source_view}/0/query',
                                   {'f': 'json', 'where': '1=1', 'outFields': '*', 'orderByFields': 'source_type ASC'})

    intersect_sources, intersect_targets = configure_intersection_sources(intersections, start)

    if len(intersect_sources.keys()) == 0:
        logging.info('no intersections to run')
        return

    ############## setting up intersection features ################
    configure_intersection_features_table(docker_conn, docker_schema)
    ############## fetching features ################
    # get latest features based on source
    fetch_features_to_intersect(intersect_sources, docker_conn, docker_schema, 'intersection_features',
                               rds_db_conn, rds_schema, wkid)
    # refresh the spatial index
    refresh_spatial_index(docker_conn, docker_schema, 'intersection_features')

    # run VACUUM ANALYZE to increase performance after bulk updates
    run_vacuum_analyze(docker_conn, docker_schema, 'intersection_features')
    ############## calculate intersections ################
    # setup table
    configure_new_intersections_table(docker_conn, docker_schema)
    # calculate intersections
    calculate_intersections_from_sources(intersect_sources, intersect_targets, 'new_intersections',
                                         docker_schema)
    # create the template for the new intersect
    rotate_tables(docker_conn, docker_schema, 'intersections', 'intersections_backup',
                  'new_intersections',
                  drop_temp=True)

    ############## write to csv, upload to s3, and swap in new intersections ################
    create_csv_and_upload_to_s3(docker_conn, docker_schema, 'intersections',
                                ['id_1', 'id_2', 'id_1_source', 'id_2_source', 'acre_overlap'],
                               f'intersections_{docker_schema}.csv', s3_bucket)

    logger.info('completed upload to s3')
    logger.info('importing intersections csv into postgres')
    import_s3_csv_to_postgres_table(rds_db_conn,
                                    rds_schema,
                                    ['objectid', 'id_1', 'id_2', 'id_1_source', 'id_2_source', 'acre_overlap'],
                                    'intersections_s3',
                                    s3_bucket,
                                    f'intersections_{rds_schema}.csv')
    # swap the tables in the rds db
    logger.info('swapping tables')
    rotate_tables(rds_db_conn, rds_schema, 'intersections', 'intersections_backup', 'intersections_s3',
                  drop_temp=False)
    # update indices
    calculate_index_for_fields(rds_db_conn, rds_schema, 'intersections',
                               ['objectid', 'id_1', 'id_2', 'id_1_source', 'id_2_source', 'acre_overlap'])

    ############# update intersection features table ################
    update_intersection_features_rds_db(docker_conn, docker_schema, rds_db_conn, rds_schema)

    ############# update run info on intersection sources table ################
    update_last_run(intersections, start, intersection_source_list_url, 0, portal, user, password)

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
    # URL for editing last run date
    intersection_src_url = os.getenv('INTERSECTION_SOURCES_URL')
    # public view for fetching intersection sources
    intersection_src_view_url = os.getenv('INTERSECTION_SOURCES_VIEW_URL')
    # GIS user credentials
    portal_url = os.getenv('ESRI_PORTAL_URL')
    portal_user = os.getenv('ESRI_USER')
    portal_password = os.getenv('ESRI_PW')
    ############### database connections ################
    # local docker db environment variables
    docker_db_schema = os.getenv('DOCKER_DB_SCHEMA')
    docker_pg_conn = create_db_conn_from_envs('DOCKER')

    # rds db params
    rds_db_schema = os.getenv('RDS_SCHEMA')
    rds_pg_conn = create_db_conn_from_envs('RDS')
    ############## intersections processing in docker ################
    # function that runs everything for creating new intersections in docker, uploading the results to s3, and swapping the tables on the rds instance
    try:
        run_intersections(docker_pg_conn, docker_db_schema, rds_pg_conn, rds_db_schema,
                          s3_bucket_name,
                          script_start, sr_wkid, intersection_src_url, intersection_src_view_url, portal_url, portal_user,
                          portal_password)
        logger.info(f'completed intersection processing, total runtime: {datetime.now() - script_start}')
    except Exception as e:
        logger.error(f'ERROR: error running intersections: {e}')
        sys.exit(1)
