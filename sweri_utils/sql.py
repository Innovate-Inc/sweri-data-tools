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
    cursor.execute(f'BEGIN;')
    cursor.execute(f'ALTER TABLE {schema}.{old_table_name} RENAME TO {new_table_name};')
    cursor.execute(f'COMMIT;')

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
        to_shape: str = 'shape'
) -> None:
    """
    Inserts records from one database into another in an enterprise geodatabase
    :param to_shape:
    :param from_shape:
    :param cursor: psycopg2 connection cursor object
    :param schema: schema to use
    :param insert_table: table to insert features into
    :param insert_fields: list of insert fields for target table
    :param from_table: table to insert features from
    :param from_fields: list of field names mapping to insert fields
    :return: None
    """
    q = f'''insert into {schema}.{insert_table} ({to_shape}, {','.join(insert_fields)}) select ST_TRANSFORM(ST_MakeValid({from_shape}), 4326), {','.join(from_fields)} from {schema}.{from_table};'''
    logging.info(q)
    cursor.execute('BEGIN;')
    cursor.execute(q)
    cursor.execute('COMMIT;')
    logging.info(f'completed {q}')


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

    This function drops the existing spatial index (if any) and recreates it.

    :param cursor: The database cursor to execute the SQL commands.
    :param schema: The schema where the table is located.
    :param table: The name of the table for which the spatial index will be refreshed.
    :return: None
    """
    logging.info(f'refreshing spatial index on {schema}.{table}')
    cursor.execute('BEGIN;')
    cursor.execute(f'DROP INDEX IF EXISTS {table}_shape_idx;')
    # recreate index
    cursor.execute(f'CREATE INDEX {table}_shape_idx ON {schema}.{table} USING GIST (shape);')
    cursor.execute('COMMIT;')
    logging.info(f'refreshed spatial index on {schema}.{table}')


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
    logging.info('moving to postgres table updates')
    # rename backup  to temp table to make space for new backup
    rename_postgres_table(cursor, schema, backup_table_name,
                          f'{backup_table_name}_temp')
    logging.info(f'{schema}.{backup_table_name} renamed to {schema}.{backup_table_name}_temp')

    # rename current table to backup table
    rename_postgres_table(cursor, schema, main_table_name, backup_table_name)
    logging.info(f'{schema}.{main_table_name} renamed to {schema}.{backup_table_name}')

    # rename new intersections table to new data
    rename_postgres_table(cursor, schema, new_table_name, main_table_name)
    logging.info(f'{schema}.{new_table_name} renamed to {schema}.{main_table_name}')

    # drop (default) or swap out temp table with new table
    if drop_temp:
        # drop temp backup table
        cursor.execute('BEGIN;')
        cursor.execute(f'DROP TABLE IF EXISTS {schema}.{backup_table_name}_temp CASCADE;')
        cursor.execute('COMMIT;')
        logging.info(f'{schema}.{backup_table_name}_temp deleted')
    else:
        # cycle temp backup into new table
        rename_postgres_table(cursor, schema, f'{backup_table_name}_temp', new_table_name)
        logging.info(f'{schema}.{backup_table_name}_temp renamed to {schema}.{new_table_name}')


def copy_table_across_servers(from_cursor: psycopg.Cursor, from_schema: str, from_table: str, to_cursor: psycopg.Cursor,
                              to_schema: str, to_table: str) -> None:
    """
    Copies a table from one PostgreSQL server to another.

    :param from_cursor: The cursor for the source database.
    :param from_schema: The schema of the source table.
    :param from_table: The name of the source table.
    :param to_cursor: The cursor for the destination database.
    :param to_schema: The schema of the destination table.
    :param to_table: The name of the destination table.
    :return: None
    """
    logging.info(f'copying {from_schema}.{from_table} from out cursor to {to_schema}.{to_table} via in-cursor')
    with from_cursor.copy(f"COPY (SELECT * FROM {from_schema}.{from_table}) TO STDOUT (FORMAT BINARY)") as out_copy:
        to_cursor.execute(f"DELETE FROM {to_schema}.{to_table};")
        with to_cursor.copy(f"COPY {to_schema}.{to_table} FROM STDIN (FORMAT BINARY)") as in_copy:
            for data in out_copy:
                in_copy.write(data)
    logging.info(f'copied {from_schema}.{from_table} from out cursor to {to_schema}.{to_table} via in-cursor')


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
    logging.info(f'running {delete_feat_q}')
    cursor.execute('BEGIN;')
    cursor.execute(delete_feat_q)
    cursor.execute('COMMIT;')
    logging.info(f'deleted from {schema}.{table} where {where}')


def run_vacuum_analyze(connection, cursor, schema, table):
    connection.autocommit = True
    cursor.execute(f'VACUUM ANALYZE {schema}.{table};')
    connection.autocommit = False