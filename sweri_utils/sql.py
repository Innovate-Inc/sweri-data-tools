import logging

import psycopg2


def rename_postgres_table(cursor, schema, old_table_name, new_table_name):
    """
    renames PostgreSQL table using arcpy.ArcSDESQLExecute connection
    :param connection: the ArcSDESQLExecute connection object
    :param schema: target schema
    :param old_table_name: existing table name
    :param new_table_name: new table name
    :return: results of executed SQL
    """
    cursor.execute(f'BEGIN;')
    cursor.execute(f'ALTER TABLE {schema}.{old_table_name} RENAME TO {new_table_name};')
    cursor.execute(f'COMMIT;')


def connect_to_pg_db(db_host, db_port, db_name, db_user, db_password):
    conn = psycopg2.connect(
        host=db_host,
        # port=db_port,
        dbname=db_name,
        user=db_user,
        password=db_password
    )

    return conn.cursor(), conn


def insert_from_db(cursor, schema, insert_table, insert_fields, from_table, from_fields, from_shape='shape', to_shape='shape'):
    """
    Inserts records from one database into another in an enterprise geodatabase
    :param to_shape:
    :param from_shape:
    :param cursor: psycopg2 connection curosr object
    :param schema: schema to use
    :param insert_table: table to insert features into
    :param insert_fields: list of insert fields for target table
    :param from_table: table to insert features from
    :param from_fields: list of field names mapping to insert fields
    :return: None
    """
    q = f'''insert into {schema}.{insert_table} ({to_shape}, {','.join(insert_fields)}) select ST_TRANSFORM({from_shape}, 4326), {','.join(from_fields)} from {schema}.{from_table};'''
    logging.info(q)
    cursor.execute('BEGIN;')
    cursor.execute(q)
    cursor.execute('COMMIT;')
    logging.info(f'completed {q}')


def pg_copy_to_csv(cursor, schema, table, filename, columns):
    with open(filename, 'w') as f:
        cursor.copy_expert(f'COPY (SELECT row_number() OVER () AS objectid, {",".join(columns)} FROM {schema}.{table}) TO STDOUT WITH CSV HEADER', f)
    return f

def refresh_spatial_index_analyze(cursor, schema, table):
    logging.info(f'refreshing spatial index on {schema}.{table}')
    cursor.execute('BEGIN;')
    cursor.execute(f'DROP INDEX IF EXISTS {table}_shape_idx;')
    # recreate index
    cursor.execute(f'CREATE INDEX {table}_shape_idx ON {schema}.{table} USING GIST (shape);')
    cursor.execute('COMMIT;')
    # run VACUUM ANALYZE to increase performance after bulk updates
    cursor.execute(f'VACUUM ANALYZE {schema}.{table};')
    logging.info(f'refreshed spatial index on {schema}.{table}')
