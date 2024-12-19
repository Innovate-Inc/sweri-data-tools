import os

from dotenv import load_dotenv
from osgeo import ogr

from sweri_utils.download import fetch_features
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

    print(help(ogr))
    # create the template for the new intersect
    treatment_intersections = os.path.join(os.getenv('SDE_FILE'), f'sweri.{schema}.intersections')
    # fetch_features()
    print(pg_cursor.execute('select * from information_schema.tables'))
    print('did something')
     # fetch all features and create featureclass from intersection features
    