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
import watchtower
logger = logging.getLogger(__name__)
logging.basicConfig( format='%(asctime)s %(levelname)-8s %(message)s',filename='./intersections.log', encoding='utf-8', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
logger.addHandler(watchtower.CloudWatchLogHandler())

def configure_intersection_sources(sde_connection_file, schema):
    """
    configures intersection sources fetched from intersection source list
    :param sde_connection_file: sde connection file path
    :param schema: target schema
    :return: intersection sources and intersection targets dictionaries
    """
    intersect_sources = {}
    intersect_targets = {}
    fields = ['source', 'id_source', 'uid_fields', 'use_as_target', 'source_type']
    with arcpy.da.SearchCursor(path.join(sde_connection_file, f'sweri.{schema}.intersections_source_list'),
                               field_names=fields, sql_clause=(None, "ORDER BY source_type ASC")) as source_cursor:
        for r in source_cursor:
            s = {'source': r[0], 'id': r[2],  'source_type': r[4]}
            intersect_sources[r[1]] = s
            if r[3] == 1:
                intersect_targets[r[1]] = s
    return intersect_sources, intersect_targets


def calculate_intersections_update_area(intersect_sources, intersect_targets, new_intersections_table, new_intersections_name, connection,
                                        schema, workspace, intersection_features):
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

            intersect = layer_intersections(intersection_features,
                                            source_key, target_key,
                                            f'{source_key}_{target_key}', workspace)
            add_area_and_update_intersections(intersect, new_intersections_table, new_intersections_name, source_key, target_key, connection, schema)
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


def add_area_and_update_intersections(intersect_result, intersections_table, intersections_table_name, fc_1_name, fc_2_name,
                                      connection, schema):
    """
    calculated acre overlap and maps fields to intersection table schema
    :param intersect_result: result of pairwise intersect
    :param intersections_table: intersections table path to append features
    :param intersections_table_name: intersections table name to append features
    :param fc_1_id: id field of first feature class
    :param fc_2_id: id field of second feature class
    :param fc_1_name: name of first feature class to put in intersection table
    :param fc_2_name: name of second feature class to put in intersection table
    :param connection: ArcSDESQLExecute connection
    :param schema: schema to use
    :return: None
    """

    # update the schema to match the intersections table
    update_schema_for_intersections_insert(intersect_result, fc_1_name, fc_2_name)

    # calculate geometry attributes and insert it
    calculate_area_overlap_and_insert(intersect_result,
                                      intersections_table,
                                      intersections_table_name,
                                      connection,
                                      schema,
                                      ['objectid','id_1', 'id_2', 'id_1_source', 'id_2_source', 'acre_overlap'],
                                      ['id_1', 'id_2', 'id_1_source', 'id_2_source', 'acre_overlap'],
                                      'acre_overlap')


def update_schema_for_intersections_insert(intersect_result, fc_1_name, fc_2_name):
    # update existing fields with new id column names
    AlterField(intersect_result, 'unique_id', 'id_1', 'id_1')
    AlterField(intersect_result, 'unique_id_1', 'id_2', 'id_2')
    AlterField(intersect_result, 'feat_source', 'id_1_source', 'id_1_source')
    AlterField(intersect_result, 'feat_source_1', 'id_2_source', 'id_2_source')
    # add field names
    CalculateField(intersect_result, 'id_1_source', f"'{fc_1_name}'", 'PYTHON3')
    CalculateField(intersect_result, 'id_2_source', f"'{fc_2_name}'", 'PYTHON3')


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

    # describe the new dataset and check archiving and globalids 
    desc = arcpy.Describe(postgres_target_table)
    
    if not desc.IsArchived:
        logger.info(f'enabling archiving on {postgres_target_table}')
        arcpy.management.EnableArchiving(postgres_target_table)

    if not desc.hasGlobalID:
        logger.info(f'adding GlobalIDs to {postgres_target_table}')
        arcpy.management.AddGlobalIDs(postgres_target_table)
    # swap current intersections with new intersections
    swap_intersection_tables(connection, schema)

    logger.info(f'INTERSECTIONS UPDATES COMPLETE')
