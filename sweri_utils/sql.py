import logging
from typing import TextIO
import psycopg


def rename_postgres_table(cursor: psycopg.Cursor, schema: str, old_table_name: str, new_table_name: str) -> None:
    """
    Renames a PostgreSQL table within a specified schema.

    :param cursor: The database cursor to execute the SQL commands.
    :param schema: The schema where the table is located.
    :param old_table_name: The current name of the table to be renamed.
    :param new_table_name: The new name for the table.
    :return: None
    """
    cursor.execute('BEGIN;')
    cursor.execute(f'ALTER TABLE {schema}.{old_table_name} RENAME TO {new_table_name};')
    cursor.execute('COMMIT;')

def postgres_create_index(cursor, schema, table_name, column_to_index):
    cursor.execute('BEGIN;')
    cursor.execute(f'CREATE INDEX ON {schema}.{table_name} ({column_to_index});')
    cursor.execute('COMMIT;')


def connect_to_pg_db(db_host: str, db_port: int, db_name: str, db_user: str, db_password: str) -> tuple:
    """
    Establishes a connection to a PostgreSQL database using the provided credentials.

    :param db_host: The hostname of the PostgreSQL server.
    :param db_port: The port number on which the PostgreSQL server is listening.
    :param db_name: The name of the database to connect to.
    :param db_user: The username to use for authentication.
    :param db_password: The password to use for authentication.
    :return: A tuple containing the database cursor and connection objects.
    """
    conn = psycopg.connect(
        host=db_host,
        port=db_port,
        dbname=db_name,
        user=db_user,
        password=db_password
    )

    return conn.cursor(), conn


def insert_from_db(
        cursor: psycopg.Cursor,
        schema: str,
        insert_table: str,
        insert_fields: list[str],
        from_table: str,
        from_fields: list[str],
        from_shape: str = 'shape',
        to_shape: str = 'shape',
        wkid: int = 3857
) -> None:
    """
    Inserts records from one table into another in a PostgreSQL database.

    :param cursor: The database cursor to execute the SQL commands.
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
    q = f'''INSERT INTO {schema}.{insert_table} ({to_shape}, {','.join(insert_fields)}) SELECT ST_MakeValid(ST_TRANSFORM({from_shape}, {wkid})), {','.join(from_fields)} FROM {schema}.{from_table};'''
    logging.info(q)
    cursor.execute('BEGIN;')
    cursor.execute(q)
    cursor.execute('COMMIT;')
    logging.info(f'Completed {q}')


def pg_copy_to_csv(cursor: psycopg.Cursor, schema: str, table: str, filename: str, columns: list[str]) -> TextIO:
    """
    Copies data from a PostgreSQL table to a CSV file.

    :param cursor: The database cursor to execute the SQL commands.
    :param schema: The schema where the table is located.
    :param table: The name of the table to copy data from.
    :param filename: The name of the CSV file to write the data to.
    :param columns: A list of column names to include in the CSV file.
    :return: The file object of the written CSV file.
    """
    with open(filename, 'w') as f:
        with cursor.copy(
            f'COPY (SELECT row_number() OVER () AS objectid, {",".join(columns)} FROM {schema}.{table}) TO STDOUT WITH CSV HEADER') as copy:
                while data := copy.read():
                    f.write(data.tobytes().decode('utf-8'))
    return f


def refresh_spatial_index(cursor: psycopg.Cursor, schema: str, table: str) -> None:
    """
    Refreshes the spatial index on a specified table in a PostgreSQL database.

    :param cursor: The database cursor to execute the SQL commands.
    :param schema: The schema where the table is located.
    :param table: The name of the table for which the spatial index will be refreshed.
    :return: None
    """
    logging.info(f'Refreshing spatial index on {schema}.{table}')
    cursor.execute('BEGIN;')
    cursor.execute(f'CREATE INDEX ON {schema}.{table} USING GIST (shape);')
    cursor.execute('COMMIT;')
    logging.info(f'Refreshed spatial index on {schema}.{table}')


def rotate_tables(cursor: psycopg.Cursor, schema: str, main_table_name: str, backup_table_name: str,
                  new_table_name: str, drop_temp: bool = True) -> None:
    """
    Rotates tables in a PostgreSQL database by renaming them.

    This function performs the following steps:
    1. Renames the backup table to a temporary table.
    2. Renames the main table to the backup table.
    3. Renames the new table to the main table.
    4. Optionally drops the temporary table or renames it to the new table.

    :param cursor: The database cursor to execute the SQL commands.
    :param schema: The schema where the tables are located.
    :param main_table_name: The current name of the main table.
    :param backup_table_name: The current name of the backup table.
    :param new_table_name: The name of the new table to be rotated in.
    :param drop_temp: If True, the temporary table will be dropped. If False, it will be renamed to the new table.
    :return: None
    """
    logging.info('Moving to PostgreSQL table updates')
    drop_temp_table(cursor, schema, backup_table_name)

    rename_postgres_table(cursor, schema, backup_table_name, f'{backup_table_name}_temp')
    logging.info(f'{schema}.{backup_table_name} renamed to {schema}.{backup_table_name}_temp')

    rename_postgres_table(cursor, schema, main_table_name, backup_table_name)
    logging.info(f'{schema}.{main_table_name} renamed to {schema}.{backup_table_name}')

    rename_postgres_table(cursor, schema, new_table_name, main_table_name)
    logging.info(f'{schema}.{new_table_name} renamed to {schema}.{main_table_name}')

    if drop_temp:
        drop_temp_table(cursor, schema, backup_table_name)
    else:
        rename_postgres_table(cursor, schema, f'{backup_table_name}_temp', new_table_name)
        logging.info(f'{schema}.{backup_table_name}_temp renamed to {schema}.{new_table_name}')


def drop_temp_table(cursor: psycopg.Cursor, schema: str, backup_table_name: str) -> None:
    """
    Drops a temporary backup table in a PostgreSQL database.

    :param cursor: The database cursor to execute the SQL commands.
    :param schema: The schema where the table is located.
    :param backup_table_name: The name of the backup table to be dropped.
    :return: None
    """
    cursor.execute('BEGIN;')
    cursor.execute(f'DROP TABLE IF EXISTS {schema}.{backup_table_name}_temp CASCADE;')
    cursor.execute('COMMIT;')
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


def copy_table_across_servers(from_cursor: psycopg.Cursor, from_schema: str, from_table: str, to_cursor: psycopg.Cursor,
                              to_schema: str, to_table: str, from_columns: list[str], to_columns: list[str], delete_to_rows: bool = False, where=None) -> None:
    """
    Copies a table from one PostgreSQL server to another.

    :param from_cursor: The cursor for the source database.
    :param from_schema: The schema of the source table.
    :param from_table: The name of the source table.
    :param to_cursor: The cursor for the destination database.
    :param to_schema: The schema of the destination table.
    :param to_table: The name of the destination table.
    :param from_columns: A list of column names to copy from the source table.
    :param to_columns: A list of column names to copy to the destination table.
    :param delete_to_rows: If True, deletes all rows in the destination table before copying.
    :return: None
    """

    from_copy = f"COPY (SELECT {','.join(from_columns)} FROM {from_schema}.{from_table}) TO STDOUT (FORMAT BINARY)"
    if where is not None:
        from_copy = f"COPY (SELECT {','.join(from_columns)} FROM {from_schema}.{from_table} WHERE {where}) TO STDOUT (FORMAT BINARY)"
    logging.info(f'Running {from_copy}')

    with from_cursor.copy(from_copy) as out_copy:
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
        to_cursor.execute('COMMIT;')

    logging.info(f'Copied {from_schema}.{from_table} ({to_columns}) from out cursor to {to_schema}.{to_table} via in-cursor')


def delete_from_table(cursor: psycopg.Cursor, schema: str, table: str, where: str) -> None:
    """
    Deletes records from a specified table in a PostgreSQL database based on a condition.

    :param cursor: The database cursor to execute the SQL commands.
    :param schema: The schema where the table is located.
    :param table: The name of the table from which records will be deleted.
    :param where: The condition to specify which records to delete.
    :return: None
    """
    delete_feat_q = f"DELETE FROM {schema}.{table} WHERE {where};"
    logging.info(f'Running {delete_feat_q}')
    cursor.execute('BEGIN;')
    cursor.execute(delete_feat_q)
    cursor.execute('COMMIT;')
    logging.info(f'Deleted from {schema}.{table} where {where}')


def run_vacuum_analyze(connection: psycopg.Connection, cursor: psycopg.Cursor, schema: str, table: str) -> None:
    """
    Runs VACUUM ANALYZE on a specified table in a PostgreSQL database to optimize performance.

    :param connection: The database connection object.
    :param cursor: The database cursor to execute the SQL commands.
    :param schema: The schema where the table is located.
    :param table: The name of the table to be vacuumed and analyzed.
    :return: None
    """
    connection.autocommit = True
    cursor.execute(f'VACUUM ANALYZE {schema}.{table};')
    connection.autocommit = False


def postgres_create_index(cursor: psycopg.Cursor, schema: str, table_name: str, column_to_index: str) -> None:
    """
    Creates an index on a specified column in a PostgreSQL table.

    :param cursor: The database cursor to execute the SQL commands.
    :param schema: The schema where the table is located.
    :param table_name: The name of the table to create the index on.
    :param column_to_index: The name of the column to create the index on.
    :return: None
    """
    cursor.execute('BEGIN;')
    cursor.execute(f'CREATE INDEX ON {schema}.{table_name} ({column_to_index});')
    cursor.execute('COMMIT;')

def calculate_index_for_fields(cursor: psycopg.Cursor, schema: str, table: str, fields: list[str], spatial = False) -> None:
    """
    Calculates the index for specified fields in a PostgreSQL table.

    :param cursor: The database cursor to execute the SQL commands.
    :param schema: The schema where the table is located.
    :param table: The name of the table to calculate the index for.
    :param fields: A list of field names to calculate the index for.
    :param spatial: Boolean indicating whether to refresh spatial index.
    :return: None
    """
    for field in fields:
        postgres_create_index(cursor, schema, table, field)
    if spatial:
        refresh_spatial_index(cursor, schema, table)