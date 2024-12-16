import os

from dotenv import load_dotenv

from sweri_utils.sql import connect_to_pg_db

if __name__ == '__main__':
    # Define the gdb and sde and load .env
    load_dotenv()

    wkid = 3857

    schema = os.getenv('SCHEMA')
    # psycopg2 connection, because arcsde connection is extremely slow during inserts
    pg_cursor = connect_to_pg_db(
        os.getenv('DB_HOST'),
        os.getenv('DB_PORT'),
        os.getenv('DB_NAME'),
        os.getenv('DB_USER'),
        os.getenv('DB_PASSWORD')
    )
     # fetch all features and create featureclass from intersection features
    