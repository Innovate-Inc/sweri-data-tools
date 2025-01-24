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

    return conn.cursor()



