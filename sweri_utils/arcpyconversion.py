import logging
import os
import arcpy
import json
from .download import get_all_features, get_ids


def insert_json_into_fc(source, filename, id_field, insert_table,
                        insert_fields, out_sr=3857, chunk_size=500) -> None:
    """
    Inserts features fetched from a REST endpoint into an existing FeatureClass in an enterprise geodatabase
    :param source: URL or name of FeatureClass
    :param filename: name of FeatureClass
    :param id_field: id field of FeatureClass
    :param insert_table: table to insert features into
    :param insert_fields: fields to use for InsertCursor
    :param out_sr: out spatial reference WKID 
    :param chunk_size: chunk size for batch conversion to FeatureClass
    :return: None
    """
    logging.info(f'converting {filename} to fc')
    ids = get_ids(source, '1=1')
    logging.info(f'{len(ids)} to process')
    with arcpy.da.InsertCursor(insert_table, insert_fields) as c:
        for i in range(0, len(ids), chunk_size):
            capture_records(source, ids[i:i + chunk_size], [id_field], c, id_field, filename, out_sr)


def capture_records(url, ids, out_fields, cursor, id_field, source_name, out_sr=3857):
    """
    Fetch all records from an ESRI REST Service with feature access enabled and insert them into an existing FeatureClass
    :param url: REST endpoint of service to query
    :param ids: objects ids to query
    :param out_fields: out fields object for query
    :param cursor: InsertCursor cursor object
    :param id_field: FeatureClass id field
    :param source_name: FeatureClass name of features being inserted 
    :param out_sr: out spatial reference WKID
    :return: None
    """
    for rows in get_all_features(url, ids, out_sr=out_sr, out_fields=out_fields):
        for row in rows:
            try:
                if 'geometry' in row:
                    cursor.insertRow((json.dumps(row['geometry']), row['attributes'][id_field], source_name))
                else:
                    logging.info(f'missing geometry for {row}')
            except Exception as e:
                logging.info(
                    f"failed trying to insert {(json.dumps(row['geometry']), row['attributes'][id_field], source_name)}")
                logging.error(e)
                raise e


def insert_from_db_sde(cursor, schema, insert_table, insert_fields, from_table, from_fields, global_id=True):
    """
    Inserts records from one database into another in an enterprise geodatabase
    :param cursor: psycopg2 connection curosr object
    :param schema: schema to use
    :param insert_table: table to insert features into
    :param insert_fields: list of insert fields for target table
    :param from_table: table to insert features from
    :param from_fields: list of field names mapping to insert fields
    :param global_id: whether to insert global ids or not
    :return: None
    """
    q = f'''insert into {schema}.{insert_table} ({','.join(insert_fields)}) select sde.next_rowid('{schema}','{insert_table}'),{'sde.next_globalid(),' if global_id else ''}{','.join(from_fields)} from {schema}.{from_table};'''
    logging.info(q)
    arcpy.AddMessage(q)
    cursor.execute('BEGIN;')
    cursor.execute(q)
    cursor.execute('COMMIT;')
    logging.info(f'completed {q}')

def insert_from_db_sde(cursor, schema, insert_table, insert_fields, from_table, from_fields, global_id=True):
    """
    Inserts records from one database into another in an enterprise geodatabase
    :param cursor: psycopg2 connection curosr object
    :param schema: schema to use
    :param insert_table: table to insert features into
    :param insert_fields: list of insert fields for target table
    :param from_table: table to insert features from
    :param from_fields: list of field names mapping to insert fields
    :param global_id: whether to insert global ids or not
    :return: None
    """
    q = f'''insert into {schema}.{insert_table} ({','.join(insert_fields)}) select sde.next_rowid('{schema}','{insert_table}'),{'sde.next_globalid(),' if global_id else ''}{','.join(from_fields)} from {schema}.{from_table};'''
    logging.info(q)
    arcpy.AddMessage(q)
    cursor.execute('BEGIN;')
    cursor.execute(q)
    cursor.execute('COMMIT;')
    logging.info(f'completed {q}')


def reproject(fc, target_projection, output_gdb_path):
    """
    Reprojects FeatureClass to target projection
    :param fc: arcpy FeatureClass
    :param target_projection: target arcpy.SpatialReference object
    :param output_gdb_path: path to output gdb
    :return: path to reprojected feature class
    """
    proj_fc = os.path.join(output_gdb_path, fc + '_reprojected')
    arcpy.management.Project(fc, proj_fc, target_projection)
    return proj_fc


def array_to_dict(keys, values):
    """
    shallow array to dictionary converter
    :param keys: names of keys to assign in new object
    :param values: list of values in same order as keys
    :return: new dict
    """
    x = dict()
    for i, f in enumerate(keys):
        x[f] = values[i]
    return x