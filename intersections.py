import json
import logging
import os
from datetime import datetime
import requests as r
from dotenv import load_dotenv
from sweri_utils.conversion import create_csv_and_upload_to_s3
from sweri_utils.download import fetch_features, fetch_geojson_features
from sweri_utils.sql import connect_to_pg_db, rename_postgres_table, insert_from_db, refresh_spatial_index_analyze, \
    rotate_tables
import watchtower

logger = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', encoding='utf-8', level=logging.INFO,
                    datefmt='%Y-%m-%d %H:%M:%S')

logger.addHandler(watchtower.CloudWatchLogHandler())


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

def delete_existing_intersects(cursor, schema, table, source_key, target_key):
    delete_feat_q = f"DELETE FROM {schema}.{table} WHERE id_1_source = '{source_key}' and id_2_source = '{target_key}';"
    logger.info(f'running {delete_feat_q}')
    cursor.execute('BEGIN;')
    cursor.execute(delete_feat_q)
    cursor.execute('COMMIT;')
    logger.info(f'deleted feat_source: {source_key} and {target_key} from {schema}.{table}')


def calculate_intersections_and_insert(cursor, schema, insert_table, source_key, target_key):
    logger.info(f'beginning intersections on {source_key} and {target_key}')
    # delete existing intersections and replace with new ones
    delete_existing_intersects(cursor, schema, insert_table, source_key, target_key)
    query = f""" insert into {schema}.{insert_table} (acre_overlap, id_1, id_1_source, id_2, id_2_source)
         select ST_AREA(ST_TRANSFORM(ST_INTERSECTION(a.shape, b.shape),4326)::geography) * 0.000247105 as acre_overlap, 
         a.unique_id as id_1, 
         a.feat_source as id_1_source, 
         b.unique_id as id_2, 
         b.feat_source as id_2_source
         from {schema}.intersection_features a, {schema}.intersection_features b
         where ST_INTERSECTS (a.shape, b.shape) 
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


def insert_feature_into_db(cursor, target_table, feature, fc_name, id_field):
    if ('geometry' not in feature) or ('properties' not in feature):
        raise KeyError('missing geometry or properties')
    if id_field not in feature['properties']:
        raise KeyError(f'missing or incorrect id field: {id_field}')

    json_geom = json.dumps(feature['geometry'])
    q = f"INSERT INTO {target_table} (unique_id, feat_source, shape) VALUES ('{feature['properties'][id_field]}', '{fc_name}',ST_SetSRID(ST_MakeValid(ST_GeomFromGeoJSON('{json_geom}')), 4326));"
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
            out_feat = fetch_geojson_features(value['source'], 'SHAPE IS NOT NULL', None, None, wkid)
            for f in out_feat:
                insert_feature_into_db(cursor, f'{schema}.{insert_table}', f, key, value['id'])
        elif value['source_type'] == 'db_table':
            insert_from_db(cursor, schema, insert_table,
                           ('unique_id', 'feat_source'), value['source'],
                           # do not need to specify object id as we are using sde.net_rowid() in the insert
                           (value['id'], f"'{key}'"))
        else:
            raise ValueError('invalid source type: {}'.format(value['source_type']))


def import_s3_csv_to_postgres_table(cursor, db_schema, fields, destination_table, s3_bucket, csv_file, aws_region='us-west-2'):
    cursor.execute('BEGIN;')
    # delete existing data in destination table
    cursor.execute(f'DELETE FROM {db_schema}.{destination_table};')
    # use aws_s3 extension to insert data from the csv file in s3 into the postgres table
    cursor.execute(f"""
        SELECT aws_s3.table_import_from_s3('{db_schema}.{destination_table}', 
        '{",".join(fields)}',
        '(format csv, HEADER)',
        aws_commons.create_s3_uri('{s3_bucket}', '{csv_file}', '{aws_region}')
        );""")
    cursor.execute('COMMIT;')
    logger.info(f'completed import of {csv_file} into {db_schema}.{destination_table}')


def run_intersections(pg_cursor, conn, db_schema, s3_bucket, start, wkid):
    # psycopg2 connection, because arcsde connection is extremely slow during inserts


    # configure intersection sources
    intersection_src_url = os.getenv('INTERSECTION_SOURCES_URL')

    ############## setting intersection sources ################
    intersections = fetch_features(f'{intersection_src_url}/0/query',{'f': 'json', 'where': '1=1', 'outFields': '*'})
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
    rotate_tables(pg_cursor, db_schema, 'intersections', 'intersections_backup', 'new_intersections', drop_temp=True)

    ############## update run info on intersection sources table ################
    update_last_run(intersections, start, intersection_src_url, 0)
    ############## write to csv and upload to s3 ################
    logger.info('uploading csv to s3')
    create_csv_and_upload_to_s3(pg_cursor, db_schema, 'intersections', ['id_1', 'id_2','id_1_source','id_2_source','acre_overlap'],f'intersections_{db_schema}.csv', s3_bucket)
    logger.info('completed upload to s3')
    conn.close()


def update_intersections_rds_db(rds_cursor, rds_conn, rds_schema, s3_bucket):
    logger.info('importing csv into postgres')
    import_s3_csv_to_postgres_table(rds_cursor,
                                    rds_schema,
                                    ['objectid', 'id_1', 'id_2', 'id_1_source', 'id_2_source', 'acre_overlap'],
                                    'intersections_s3',
                                    s3_bucket,
                                    f'intersections_{rds_schema}.csv')
    # swap the tables in the rds db
    logger.info('swapping tables')
    rotate_tables(rds_pg_cursor, rds_schema, 'intersections', 'intersections_backup', 'intersections_s3',
                             drop_temp=False)
    rds_conn.close()


if __name__ == '__main__':
    logger.info('starting intersection processing')
    load_dotenv('.env')
    script_start = datetime.now()
    sr_wkid = 4326

    # s3 bucket used for intersections
    s3_bucket_name = os.getenv('S3_BUCKET')
    ############## intersections processing in docker ################
    # local docker db environment variables
    docker_db_schema = os.getenv('DOCKER_DB_SCHEMA')
    docker_db_host = os.getenv('DOCKER_DB_HOST')
    docker_db_port = int(os.getenv('DOCKER_DB_PORT'))
    docker_db_name = os.getenv('DOCKER_DB_NAME')
    docker_db_user = os.getenv('DOCKER_DB_USER')
    docker_db_password = os.getenv('DOCKER_DB_PASSWORD')

    docker_pg_cursor, docker_pg_conn = connect_to_pg_db(docker_db_host, docker_db_port, docker_db_name, docker_db_user, docker_db_password)
    # function that runs everything for creating new intersections in docker
    run_intersections(docker_pg_cursor, docker_pg_conn, docker_db_schema, s3_bucket_name, script_start, sr_wkid)

    ############## uploading results to rds db instance ################
    # rds db params
    rds_db_schema = os.getenv('RDS_SCHEMA')
    rds_db_host = os.getenv('RDS_DB_HOST')
    rds_db_port = int(os.getenv('RDS_DB_PORT'))
    rds_db_name = os.getenv('RDS_DB_NAME')
    rds_db_user = os.getenv('RDS_DB_USER')
    rds_db_password = os.getenv('RDS_DB_PASSWORD')
    rds_pg_cursor, rds_pg_conn = connect_to_pg_db(rds_db_host, rds_db_port, rds_db_name, rds_db_user, rds_db_password)
    # connect and upload
    update_intersections_rds_db(rds_pg_cursor, rds_pg_conn, rds_db_schema, s3_bucket_name)
    logger.info('completed intersection processing')
