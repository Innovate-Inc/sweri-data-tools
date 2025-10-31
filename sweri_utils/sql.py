import logging
import os
from typing import TextIO
import psycopg
from .sweri_logging import log_this

def rename_postgres_table(conn: psycopg.Connection, schema: str, old_table_name: str, new_table_name: str) -> None:
    """
    Renames a PostgreSQL table within a specified schema.

    :param conn: The database connection.
    :param schema: The schema where the table is located.
    :param old_table_name: The current name of the table to be renamed.
    :param new_table_name: The new name for the table.
    :return: None
    """
    cursor = conn.cursor()
    with conn.transaction():
        cursor.execute(f'ALTER TABLE {schema}.{old_table_name} RENAME TO {new_table_name};')

def postgres_create_index(conn, schema, table_name, column_to_index):
    """
    Creates an index on a specified column in a PostgreSQL table.

    :param conn: The database connection.
    :param schema: The schema where the table is located.
    :param table_name: The name of the table to create the index on.
    :param column_to_index: The name of the column to index.
    :return: None
    """
    cursor = conn.cursor()
    with conn.transaction():
        cursor.execute(f'CREATE INDEX ON {schema}.{table_name} ({column_to_index});')


def connect_to_pg_db(db_host: str, db_port: int, db_name: str, db_user: str, db_password: str) -> psycopg.Connection:
    """
    Establishes a connection to a PostgreSQL database using the provided credentials.

    :param db_host: The hostname of the PostgreSQL server.
    :param db_port: The port number on which the PostgreSQL server is listening.
    :param db_name: The name of the database to connect to.
    :param db_user: The username to use for authentication.
    :param db_password: The password to use for authentication.
    :param autocommit: If True, enables autocommit mode for the connection.
    """
    conn = psycopg.connect(
        host=db_host,
        port=db_port,
        dbname=db_name,
        user=db_user,
        password=db_password,
        autocommit=True
    )

    return conn


def insert_from_db(
        conn: psycopg.Connection,
        schema: str,
        insert_table: str,
        insert_fields: list[str],
        from_table: str,
        from_fields: list[str],
        from_shape: str = 'shape',
        to_shape: str = 'shape',
        wkid: int = 4326
) -> None:
    """
    Inserts records from one table into another in a PostgreSQL database.

    :param conn: The database connection.
    :param schema: The schema where the tables are located.
    :param insert_table: The name of the table to insert records into.
    :param insert_fields: A list of field names to insert into the target table.
    :param from_table: The name of the table to copy records from.
    :param from_fields: A list of field names to copy from the source table.
    :param from_shape: The geometry column in the source table.
    :param to_shape: The geometry column in the target table.
    :param wkid: The spatial reference ID to use for the geometry transformation.
    :return: None
    """
    q = f'''INSERT INTO {schema}.{insert_table} (objectid, {to_shape}, {','.join(insert_fields)}) SELECT sde.next_rowid('{schema}', '{insert_table}'),ST_MakeValid(ST_TRANSFORM({from_shape}, {wkid})), {','.join(from_fields)} FROM {schema}.{from_table};'''
    logging.info(q)
    cursor = conn.cursor()
    with conn.transaction():
        cursor.execute(q)
    logging.info(f'Completed {q}')


def pg_copy_to_csv(conn: psycopg.Connection, schema: str, table: str, filename: str, columns: list[str]) -> TextIO:
    """
    Copies data from a PostgreSQL table to a CSV file.

    :param conn: The database connection.
    :param schema: The schema where the table is located.
    :param table: The name of the table to copy data from.
    :param filename: The name of the CSV file to write the data to.
    :param columns: A list of column names to include in the CSV file.
    :return: The file object of the written CSV file.
    """
    with open(filename, 'w') as f:
        cursor = conn.cursor()
        with conn.transaction():
            with cursor.copy(
                f'COPY (SELECT row_number() OVER () AS objectid, {",".join(columns)} FROM {schema}.{table}) TO STDOUT WITH CSV HEADER') as copy:
                    while data := copy.read():
                        f.write(data.tobytes().decode('utf-8'))
    return f


def refresh_spatial_index(conn: psycopg.Connection, schema: str, table: str) -> None:
    """
    Refreshes the spatial index on a specified table in a PostgreSQL database.

    :param conn: The database connection.
    :param schema: The schema where the table is located.
    :param table: The name of the table for which the spatial index will be refreshed.
    :return: None
    """

    cursor = conn.cursor()
    with conn.transaction():
        logging.info(f'Refreshing spatial index on {schema}.{table}')
        cursor.execute(f'CREATE INDEX ON {schema}.{table} USING GIST (shape);')
        logging.info(f'Refreshed spatial index on {schema}.{table}')


def rotate_tables(conn: psycopg.Connection, schema: str, main_table_name: str, backup_table_name: str,
                  new_table_name: str, drop_temp: bool = True) -> None:
    """
    Rotates tables in a PostgreSQL database by renaming them.

    This function performs the following steps:
    1. Renames the backup table to a temporary table.
    2. Renames the main table to the backup table.
    3. Renames the new table to the main table.
    4. Optionally drops the temporary table or renames it to the new table.

    :param conn: The database connection.
    :param schema: The schema where the tables are located.
    :param main_table_name: The current name of the main table.
    :param backup_table_name: The current name of the backup table.
    :param new_table_name: The name of the new table to be rotated in.
    :param drop_temp: If True, the temporary table will be dropped. If False, it will be renamed to the new table.
    :return: None
    """


    logging.info('Moving to PostgreSQL table updates')
    drop_temp_table(conn, schema, backup_table_name)

    rename_postgres_table(conn, schema, backup_table_name, f'{backup_table_name}_temp')
    logging.info(f'{schema}.{backup_table_name} renamed to {schema}.{backup_table_name}_temp')

    rename_postgres_table(conn, schema, main_table_name, backup_table_name)
    logging.info(f'{schema}.{main_table_name} renamed to {schema}.{backup_table_name}')

    rename_postgres_table(conn, schema, new_table_name, main_table_name)
    logging.info(f'{schema}.{new_table_name} renamed to {schema}.{main_table_name}')

    if drop_temp:
        drop_temp_table(conn, schema, backup_table_name)
    else:
        rename_postgres_table(conn, schema, f'{backup_table_name}_temp', new_table_name)
        logging.info(f'{schema}.{backup_table_name}_temp renamed to {schema}.{new_table_name}')


def drop_temp_table(conn: psycopg.Connection, schema: str, backup_table_name: str) -> None:
    """
    Drops a temporary backup table in a PostgreSQL database.

    :param conn: The database connection.
    :param schema: The schema where the table is located.
    :param backup_table_name: The name of the backup table to be dropped.
    :return: None
    """
    cursor = conn.cursor()
    with conn.transaction():
        cursor.execute(f'DROP TABLE IF EXISTS {schema}.{backup_table_name}_temp CASCADE;')
        logging.info(f'{schema}.{backup_table_name}_temp deleted')


def fetch_and_order_columns(cursor: psycopg.Cursor, schema: str, table: str) -> list[str]:
    """
    Fetches and orders the columns of a specified table in a PostgreSQL database.

    :param cursor: The database cursor to execute the SQL commands.
    :param schema: The schema where the table is located.
    :param table: The name of the table to fetch columns from.
    :return: A list of column names ordered alphabetically.
    """
    columns_query = f"SELECT column_name FROM information_schema.columns WHERE table_schema = '{schema}' AND table_name = '{table}'"
    cursor.execute(columns_query)
    columns = [row[0] for row in cursor.fetchall()]
    columns.sort()
    return columns


def copy_table_across_servers(from_conn: psycopg.Connection, from_schema: str, from_table: str, to_conn: psycopg.Connection,
                              to_schema: str, to_table: str, from_columns: list[str], to_columns: list[str], delete_to_rows: bool = False, where=None) -> None:
    """
    Copies a table from one PostgreSQL server to another.

    :param from_conn: The connection for the source database.
    :param from_schema: The schema of the source table.
    :param from_table: The name of the source table.
    :param to_conn: The connection for the destination database.
    :param to_schema: The schema of the destination table.
    :param to_table: The name of the destination table.
    :param from_columns: A list of column names to copy from the source table.
    :param to_columns: A list of column names to copy to the destination table.
    :param delete_to_rows: If True, deletes all rows in the destination table before copying.
    :param where: Optional WHERE clause to filter the rows to be copied.
    :return: None
    """

    from_copy = f"COPY (SELECT {','.join(from_columns)} FROM {from_schema}.{from_table}) TO STDOUT (FORMAT BINARY)"
    if where is not None:
        from_copy = f"COPY (SELECT {','.join(from_columns)} FROM {from_schema}.{from_table} WHERE {where}) TO STDOUT (FORMAT BINARY)"
    logging.info(f'Running {from_copy}')
    from_cursor = from_conn.cursor()
    to_cursor = to_conn.cursor()
    with from_conn.transaction():
        with from_cursor.copy(from_copy) as out_copy:
            with to_conn.transaction():
                if delete_to_rows:
                    delete_q = f"DELETE FROM {to_schema}.{to_table}"
                    if where:
                        delete_q += f" WHERE {where}"
                    else:
                        logging.warning(f'Deleting all features from {to_schema}.{to_table}')
                    logging.info(f'Running {delete_q}')

                    to_cursor.execute(delete_q)
                to_copy = f"COPY {to_schema}.{to_table} ({','.join(to_columns)}) FROM STDIN (FORMAT BINARY)"
                to_cursor.execute('BEGIN;')
                with to_cursor.copy(to_copy) as in_copy:
                    for data in out_copy:
                        in_copy.write(data)

    logging.info(f'Copied {from_schema}.{from_table} ({to_columns}) from out cursor to {to_schema}.{to_table} via in-cursor')


def delete_from_table(conn: psycopg.Connection, schema: str, table: str, where: str) -> None:
    """
    Deletes records from a specified table in a PostgreSQL database based on a condition.

    :param conn: The database connection to execute the SQL commands.
    :param schema: The schema where the table is located.
    :param table: The name of the table from which records will be deleted.
    :param where: The condition to specify which records to delete.
    :return: None
    """
    cursor = conn.cursor()
    with conn.transaction():
        delete_feat_q = f"DELETE FROM {schema}.{table} WHERE {where};"
        logging.info(f'Running {delete_feat_q}')
        cursor.execute(delete_feat_q)
        logging.info(f'Deleted from {schema}.{table} where {where}')


def run_vacuum_analyze(connection: psycopg.Connection, schema: str, table: str) -> None:
    """
    Runs VACUUM ANALYZE on a specified table in a PostgreSQL database to optimize performance.

    :param connection: The database connection object.
    :param schema: The schema where the table is located.
    :param table: The name of the table to be vacuumed and analyzed.
    :return: None
    """

    cursor = connection.cursor()
    cursor.execute(f'VACUUM ANALYZE {schema}.{table};')


def calculate_index_for_fields(conn: psycopg.Connection, schema: str, table: str, fields: list[str], spatial = False) -> None:
    """
    Calculates the index for specified fields in a PostgreSQL table.

    :param conn: The database connection.
    :param schema: The schema where the table is located.
    :param table: The name of the table to calculate the index for.
    :param fields: A list of field names to calculate the index for.
    :param spatial: Boolean indicating whether to refresh spatial index.
    :return: None
    """

    for field in fields:
        postgres_create_index(conn, schema, table, field)
    if spatial:
        refresh_spatial_index(conn, schema, table)

def add_column(conn: psycopg.Connection, schema: str, table: str, column_name: str, column_type: str) -> None:
    """
    Adds a new column to a specified table in a PostgreSQL database.

    :param cursor: The database cursor to execute the SQL commands.
    :param schema: The schema where the table is located.
    :param table: The name of the table to add the column to.
    :param column_name: The name of the new column to be added.
    :param column_type: The data type of the new column.
    :return: None
    """
    cursor = conn.cursor()
    with conn.transaction():
        cursor.execute(f'ALTER TABLE {schema}.{table} ADD COLUMN {column_name} {column_type};')

def limit_update(conn, schema, table, update_command, limit=150000):
    """
    Limits the number of rows in a PostgreSQL table to a specified limit.

    :param conn: The database connection object.
    :param schema: The schema where the table is located.
    :param table: The name of the table to limit rows in.
    :param update_command: The main update command to be run.
    :param limit: The maximum number of rows to keep in the table.
    :return: None
    """
    cursor = conn.cursor()
    current_id = 0
    # pbar = tqdm.tqdm()
    while True:
        with conn.transaction():
            max_id = cursor.execute(f"""
                        select max(objectid) as max_id from (
                          select objectid from {schema}.{table}
                           where objectid > {current_id} group by objectid limit {limit}
                          ) subquery
                    """).fetchone()[0]
            if max_id is None:
                break
            cursor.execute(f"""
                {update_command}
                {'AND' if 'where' in update_command.lower() else 'WHERE'} 
                {current_id} < objectid and objectid <= {max_id}
            """)
            current_id = max_id
            # pbar.update(limit)


@log_this
def revert_multi_to_poly(conn, schema, table):
    """
    Reverts a multi-part geometry to a single-part geometry in a PostgreSQL table where there is only one polygon.
    :param conn: The database connection object.
    :param schema: The schema where the table is located.
    :param table: The name of the table to revert geometries in.
    :param field: The name of the geometry field to be reverted.
    :return: None
    """
    cursor = conn.cursor()
    with conn.transaction():
        cursor.execute(f"""
            WITH polygons AS (
              SELECT
                objectid,
                (ST_Dump(shape)).geom::geometry(Polygon,4326) AS geom
              FROM {schema}.{table}
              WHERE ST_NumGeometries(shape) = 1
                AND ST_GeometryType(shape) = 'ST_MultiPolygon'
            )
            UPDATE {schema}.{table} AS t
            SET shape = p.geom
            FROM polygons AS p
            WHERE t.objectid = p.objectid;
        """)

@log_this
def makevalid_shapes(conn, schema, table, shape_field, resolution=0.000000001):
    """
     Makes shapes invalid to PostGIS valid using ST_MakeValid().
     After inital MakeValid, targets shapes valid to PostGIS but
     invalid to ESRI by snapping them to a fine grid to emulate
     ESRI’s feature class resolution.

     :param conn: Database connection object.
     :param schema: Schema where the table is located.
     :param table: Name of the table to fix geometries in.
     :param shape_field: Name of the geometry field to fix.
     :param resolution: Grid snapping resolution (default 1e-9).
     """
    cursor = conn.cursor()
    with conn.transaction():
        cursor.execute(f'''
        
            UPDATE {schema}.{table}
            SET {shape_field} = ST_MakeValid({shape_field}, 'method=structure')
            WHERE NOT ST_IsValid({shape_field});
            
        ''')

        cursor.execute(f'''

            UPDATE {schema}.{table}
            SET {shape_field} =
                            ST_MakeValid( 
                                ST_SnapToGrid({shape_field}, {resolution})  -- Snap to ESRI grid
                            , 'method=structure'
                            )
            WHERE NOT ST_IsValid(ST_SnapToGrid({shape_field}, {resolution})); 

        ''')

@log_this
def remove_zero_area_polygons(conn, schema, table):
    cursor = conn.cursor()
    with conn.transaction():
        cursor.execute(f'''

            DELETE FROM {schema}.{table}
            WHERE ST_Area(shape) = 0;

        ''')

@log_this
def extract_geometry_collections(conn, schema, table,  resolution=0.000000001):
    """
    Extracts geometry collections from a PostGIS table.
    ST_CollectionExtract : Extracts geometry collections to polygon
    ST_SnapToGrid : Emulates ESRI feature class resolution(use 0 resolution to disable)
    ST_UnaryUnion : Dissolves geometry preventing MakeValid turning the geometry back into a geometry collection
    ST_MakeValid : ensures geometry validity

     :param conn: Database connection object.
     :param schema: Schema where the table is located.
     :param table: Name of the table to fix geometries in.
     :param resolution: Grid snapping resolution (default 1e-9).
    """
    cursor = conn.cursor()
    with conn.transaction():
        cursor.execute(f'''

            UPDATE {schema}.{table}
            SET shape =
                ST_MakeValid(
                    ST_UnaryUnion(
                        ST_SnapToGrid(
                          ST_CollectionExtract(shape, 3),   
                          {resolution}
                        )                                    
                  ),
                  'method=structure'
                )
            WHERE ST_GeometryType(shape) = 'ST_GeometryCollection';

        ''')

@log_this
def remove_blank_strings(conn, schema, treatment_index, fields_for_removal):
    cursor = conn.cursor()
    with conn.transaction():
        for field in fields_for_removal:
            cursor.execute(f'''

                UPDATE {schema}.{treatment_index}
                SET {field} = NULLIF({field}, '');

            ''')

@log_this
def create_db_conn_from_envs():
    docker_db_host = os.getenv('DB_HOST')
    docker_db_port = int(os.getenv('DB_PORT'))
    docker_db_name = os.getenv('DB_NAME')
    docker_db_user = os.getenv('DB_USER')
    docker_db_password = os.getenv('DB_PASSWORD')
    return connect_to_pg_db(docker_db_host, docker_db_port, docker_db_name, docker_db_user, docker_db_password)