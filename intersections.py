import os
os.environ["CRYPTOGRAPHY_OPENSSL_NO_LEGACY"] = "1"
import arcpy.da
from os import path
from arcpy.management import CreateTable, AlterField, CalculateField, CreateFeatureclass
import logging
from dotenv import load_dotenv
from sweri_utils.analysis import  layer_intersections, calculate_area_overlap_and_insert
from sweri_utils.conversion import insert_json_into_fc, insert_from_db
from sweri_utils.sql import connect_to_pg_db, rename_postgres_table
from sweri_utils.intersections import update_schema_for_intersections_insert, configure_intersection_sources
import watchtower
logger = logging.getLogger(__name__)
logging.basicConfig( format='%(asctime)s %(levelname)-8s %(message)s',filename='./intersections.log', encoding='utf-8', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
logger.addHandler(watchtower.CloudWatchLogHandler())


def calculate_intersections_and_insert(cursor, schema, insert_table, source_key, target_key):
    logger.info(f'beginning intersections on {source_key} and {target_key}')
    query = f""" insert into sweri.intersections (objectid, acre_overlap, id_1, id_1_source, id_2, id_2_source)
         select 
         sde.next_rowid('{schema}', '{insert_table}'),
         ST_AREA(ST_TRANSFORM(ST_INTERSECTION(a.shape, b.shape),4326)::geography) * 0.000247105 as acre_overlap, 
         a.unique_id as id_1, 
         a.feat_source as id_1_source, 
         b.unique_id as id_2, 
         b.feat_source as id_2_source
         from sweri.intersection_features a, sweri.intersection_features b
         where ST_INTERSECTS (a.shape, b.shape) 
         and a.feat_source = '{source_key}'
         and b.feat_source = '{target_key}';"""
    cursor.execute('BEGIN;')
    cursor.execute(query)
    cursor.execute('COMMIT;')
    logger.info(f'completed intersections on {source_key} and {target_key}, inserted into {insert_table} ')


def calculate_intersections_update_area(intersect_sources, intersect_targets, new_intersections_name, connection,
                                        schema):
    """
    calculates intersections and acreage of intersections
    :param intersect_sources: intersect sources dictionary
    :param intersect_targets: intersect targets dictionary
    :param new_intersections_table: new intersections table path
    :param new_intersections_name: new intersections table name in enterprise geodatabase
    :param connection: ArcSDESQLExecute connection
    :param schema: target schema
    :return:
    """
    for source_key, source_value in intersect_sources.items():
        for target_key, target_value in intersect_targets.items():
            if target_key == source_key:
                continue
            logger.info(f'performing intersections on {source_key} and {target_key}')
            calculate_intersections_and_insert(connection, schema, new_intersections_name, source_key, target_key)
            logger.info(f'completed intersections on {source_key} and {target_key}')


def fetch_all_features_to_intersect(intersect_sources, pg_cursor, schema, insert_table='intersection_features',
                                    wkid=3857):
    for key, value in intersect_sources.items():
        if value['source_type'] == 'url':
            insert_json_into_fc(value['source'], key, value['id'], insert_table,
                                ['SHAPE@JSON', 'unique_id', 'feat_source'], wkid)
        elif value['source_type'] == 'db_table':
            insert_from_db(pg_cursor, schema, insert_table,
                           ('objectid', 'shape', 'unique_id', 'feat_source'), value['source'],
                           # do not need to specify object id as we are using sde.net_rowid() in the insert
                           ('shape', value['id'], f"'{key}'"),
                           False)
        else:
            raise ValueError('invalid source type: {}'.format(value['source_type']))


def setup_intersection_features_table(sde_connection_file, schema, treatment_intersections):
    new_intersections_table = os.path.join(sde_connection_file, f'sweri.{schema}.new_intersections')
    if arcpy.Exists(new_intersections_table):
        connection.execute(f'DROP TABLE IF EXISTS {schema}.new_intersections')
    new_intersections_table = CreateTable(arcpy.env.workspace, f'sweri.{schema}.new_intersections',
                                          template=treatment_intersections)

    intersect_features_backup = path.join(sde_connection_file, f'sweri.{schema}.intersection_features_backup')
    intersect_features_backup_temp = path.join(sde_connection_file, f'sweri.{schema}.intersection_features_backup_temp')

    if arcpy.Exists(intersect_features_backup):
        # backup to backup temp
        arcpy.management.Rename(f'sweri.{schema}.intersection_features_backup',
                                f'sweri.{schema}.intersection_features_backup_temp')
    # current to backup
    arcpy.management.Rename(f'sweri.{schema}.intersection_features', f'sweri.{schema}.intersection_features_backup')
    # fresh copy
    fc = CreateFeatureclass(arcpy.env.workspace, f'sweri.{schema}.intersection_features',
                       # create a fresh copy
                       template=f'sweri.{schema}.intersection_features_backup',
                       spatial_reference=arcpy.SpatialReference(wkid))
    if arcpy.Exists(intersect_features_backup_temp):
        arcpy.management.Delete(intersect_features_backup_temp)
    return new_intersections_table


def swap_intersection_tables(connection, schema):
    """
    swaps new intersections and existing intersections table
    :param connection: ArcSDESQLExecute connection
    :param schema: target schema
    :return:
    """
    logger.info('moving to postgres table updates')
    # rename backup backup to temp table to make space for new backup
    rename_postgres_table(connection, schema, 'intersections_backup',
                          'intersections_backup_temp')
    logger.info(
        f'{schema}.intersections_backup renamed to {schema}.intersections_backup_temp')

    # rename current table to backup table
    rename_postgres_table(connection, schema, 'intersections', 'intersections_backup')
    logger.info(f'{schema}.intersections renamed to {schema}.intersections_backup')
    # rename new intersections table to new data
    rename_postgres_table(connection, schema, 'new_intersections', 'intersections')
    logger.info(f'{schema}.new_intersections renamed to {schema}.intersections')

    # drop temp backup table
    connection.execute(f'DROP TABLE IF EXISTS {schema}.intersections_backup_temp CASCADE;')
    logger.info(f'{schema}.intersections_backup_temp deleted')


if __name__ == '__main__':
    # Define the gdb and sde and load .env
    load_dotenv()
    sde_connection_file = os.getenv('SDE_FILE')
    arcpy.env.workspace = sde_connection_file
    arcpy.env.overwriteOutput = True
    wkid = 3857

    schema = os.getenv('SCHEMA')

    # arcsde connection
    connection = arcpy.ArcSDESQLExecute(sde_connection_file)

    # psycopg2 connection, because arcsde connection is extremely slow during inserts
    pg_cursor = connect_to_pg_db(
        os.getenv('DB_HOST'), 
        os.getenv('DB_PORT'),
        os.getenv('DB_NAME'),
        os.getenv('DB_USER'),
        os.getenv('DB_PASSWORD') 
        )


    # # create the template for the new intersect
    treatment_intersections = path.join(sde_connection_file, f'sweri.{schema}.intersections')
    logger.info('updating new_intersections')
    # create fresh intersections table
    new_intersections_table = setup_intersection_features_table(sde_connection_file, schema, treatment_intersections)
    intersection_features = os.path.join(sde_connection_file, f'sweri.{schema}.intersection_features')
    logger.info('intersection features table updated')
    postgres_target_table = path.join(sde_connection_file, f'sweri.{schema}.new_intersections')

    # configure intersection source and target dicts
    intersects = configure_intersection_sources(sde_connection_file, schema)
    intersect_sources = intersects[0]
    intersect_targets = intersects[1]

    # get all the features and put them into the intersection_features table
    fetch_all_features_to_intersect(intersect_sources, pg_cursor, schema, wkid=3857)
    # update spatial index
    arcpy.management.RebuildIndexes(sde_connection_file, "NO_SYSTEM", f"sweri.{schema}.intersection_features", "ALL")
    # run pairwise intersections and calculate acre overlap
    calculate_intersections_update_area(intersect_sources, intersect_targets,
                                        new_intersections_table, 'new_intersections', connection, schema,
                                        arcpy.env.workspace, intersection_features)

    # add Index
    try:
        arcpy.management.AddIndex(postgres_target_table, ['id_1'])
        arcpy.management.AddIndex(postgres_target_table, ['id_2'])
        arcpy.management.AddIndex(postgres_target_table, ['id_1_source'])
        arcpy.management.AddIndex(postgres_target_table, ['id_2_source'])
    except Exception as e:
        logger.error(e)
        pass

    swap_intersection_tables(connection, schema)

    logger.info(f'INTERSECTIONS UPDATES COMPLETE')
