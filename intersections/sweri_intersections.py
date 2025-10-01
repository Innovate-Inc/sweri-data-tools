import sys
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

import json
import logging
import os
from datetime import datetime
from arcgis import GIS
from celery import group
import requests as r
from dotenv import load_dotenv
from sweri_utils.download import fetch_features
from sweri_utils.sql import refresh_spatial_index, run_vacuum_analyze, connect_to_pg_db, delete_from_table, \
    create_db_conn_from_envs, truncate_and_insert, switch_autovacuum_and_triggers, delete_duplicate_records
from sweri_utils.sweri_logging import log_this
from sweri_utils.hosted import hosted_upload_and_swizzle

from intersections.tasks import calculate_intersections_and_insert, fetch_and_insert_intersection_features

@log_this
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
            'name': att['name'],
            'chunk_size': int(att['chunk_size']) if att['chunk_size'] else 1000,
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


@log_this
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


@log_this
def calculate_intersections_from_sources(intersect_sources, intersect_targets, intersections_name, schema, chunk):
    t = []
    source_ids = {}
    for source_key, source_value in intersect_sources.items():
        for target_key, target_value in intersect_targets.items():
            if target_key == source_key:
                continue
            conn = create_db_conn_from_envs()
            # delete existing intersections for this source and target
            delete_from_table(conn, schema, intersections_name,
                              f"id_1_source = '{source_key}' and id_2_source = '{target_key}'")
            # fetch object ids for the source if not already fetched
            if source_key not in source_ids:
                source_ids[source_key] = fetch_object_ids(conn, schema, source_key)
            ids = source_ids[source_key]
            conn.close()
            del conn
            # get all object ids for the intersecting features
            if len(ids) > 0:
                i = 0
                while i < len(ids):
                    # calculate intersections in chunks
                    source_object_ids = str(tuple(ids[i:i + chunk]))
                    t.append(calculate_intersections_and_insert.s(schema, intersections_name, source_key, target_key,
                                                                  source_object_ids))
                    i += chunk
    conn = create_db_conn_from_envs()
    autovacuum_tables = [intersections_name, 'intersection_features']
    switch_autovacuum_and_triggers(False, conn,  schema, autovacuum_tables)
    try:
        g = group(t)()
        g.get()
    except Exception as err:
        switch_autovacuum_and_triggers(True, conn, schema, autovacuum_tables)
        raise err
    switch_autovacuum_and_triggers(True, conn, schema, autovacuum_tables)


@log_this
def fetch_object_ids(conn, schema, source_key):
    cursor = conn.cursor()
    with conn.transaction():
        cursor.execute(f"""
            SELECT objectId
            FROM {schema}.intersection_features 
            where feat_source = '{source_key}';
        """)
        ids = cursor.fetchall()
        return tuple(i[0] for i in ids)


@log_this
def fetch_features_to_intersect(intersect_sources, conn, schema, insert_table, wkid):
    tasks = []
    for key, value in intersect_sources.items():
        # remove existing features
        tasks.append(fetch_and_insert_intersection_features.s(key, value, wkid, schema, insert_table))
    g = group(tasks)()
    g.get()
    # remove null shapes and unique ids
    docker_cursor = conn.cursor()
    with conn.transaction():
        # remove null shapes and unique ids
        docker_cursor.execute(f"DELETE FROM {schema}.{insert_table} WHERE shape IS NULL OR unique_id is NULL;")


@log_this
def run_intersections(docker_conn, docker_schema,
                      start, wkid, intersection_source_list_url, intersection_source_view, portal, user, password,
                      intersection_view, intersection_data_ids, chunk_size=5000):
    ############## setting intersection sources ################
    intersections = fetch_features(f'{intersection_source_view}/0/query',
                                   {'f': 'json', 'where': '1=1', 'outFields': '*', 'orderByFields': 'source_type ASC'})

    intersect_sources, intersect_targets = configure_intersection_sources(intersections, start)

    if len(intersect_sources.keys()) == 0:
        logging.info('no intersections to run')
        return


    ############## setting up intersection features ################
    ############## fetching features ################
    # get latest features based on source
    fetch_features_to_intersect(intersect_sources, docker_conn, docker_schema, 'intersection_features', wkid)
    # refresh the spatial index
    refresh_spatial_index(docker_conn, docker_schema, 'intersection_features')

    # run VACUUM ANALYZE to increase performance after bulk updates
    run_vacuum_analyze(docker_conn, docker_schema, 'intersection_features')
    # ############## calculate intersections ################
    calculate_intersections_from_sources(intersect_sources, intersect_targets, 'intersections',
                                         docker_schema, chunk_size)

    delete_duplicate_records(docker_schema, 'intersections', docker_conn, ['id_1', 'id_2', 'id_1_source', 'id_2_source', 'acre_overlap'], 'id_1')

    ############ hosted upload ################
    hosted_upload_and_swizzle(portal, user, password, intersection_view, intersection_data_ids, docker_schema,
                              'intersections', 0, 10000, False, [])

    # ############ update run info on intersection sources table ################
    update_last_run(intersections, start, intersection_source_list_url, 0, portal, user, password)
    # close connections
    docker_conn.close()

if __name__ == '__main__':
    logger.info('starting intersection processing')
    load_dotenv('../.env')
    script_start = datetime.now()
    sr_wkid = 4326

    # configure intersection sources
    # URL for editing last run date
    intersection_src_url = os.getenv('INTERSECTION_SOURCES_URL')
    # public view for fetching intersection sources
    intersection_src_view_url = os.getenv('INTERSECTION_SOURCES_VIEW_URL')
    # GIS user credentials
    portal_url = os.getenv('ESRI_PORTAL_URL')
    portal_user = os.getenv('ESRI_USER')
    portal_password = os.getenv('ESRI_PW')
    # views
    intersections_view_id = os.getenv('INTERSECTIONS_VIEW_ID')
    intersections_data_ids = [os.getenv('INTERSECTIONS_DATA_ID_1'), os.getenv('INTERSECTIONS_DATA_ID_2')]
    ############### database connections ################
    # local docker db environment variables
    db_schema = os.getenv('SCHEMA')
    pg_conn = connect_to_pg_db(os.getenv('DB_HOST'), int(os.getenv('DB_PORT')) if os.getenv('DB_PORT') else 5432,
                               os.getenv('DB_NAME'), os.getenv('DB_USER'), os.getenv('DB_PASSWORD'))
    ############## intersections processing in docker ################
    # function that runs everything for creating new intersections in docker
    try:
        run_intersections(pg_conn, db_schema,
                          script_start, sr_wkid, intersection_src_url, intersection_src_view_url, portal_url,
                          portal_user,
                          portal_password, intersections_view_id, intersections_data_ids)
        logger.info(f'completed intersection processing, total runtime: {datetime.now() - script_start}')
    except Exception as e:
        logger.error(f'ERROR: error running intersections: {e}')
        sys.exit(1)
