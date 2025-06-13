import os

from sweri_utils.download import service_to_postgres
from sweri_utils.sql import convert_poly_to_multi, connect_to_pg_db

if __name__ == "__main__":
    # get params
    url = os.environ.get('STATE_DATA_URL')
    schema = os.environ.get('SCHEMA')
    table = 'state_data'

    ogr_db_string = f"PG:dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')} port={os.getenv('DB_PORT')} host={os.getenv('DB_HOST')}"
    conn = connect_to_pg_db(os.getenv('DB_HOST'), os.getenv('DB_PORT'), os.getenv('DB_NAME'),
                            os.getenv('DB_USER'), os.getenv('DB_PASSWORD'))


    # download and insert features
    service_to_postgres(url, "DataCategory = 'State'", 4326, ogr_db_string, 'sweri',
                        table, conn, 250,
                        {"geometryPrecision": "6"})
    # convert polygons to multi-polygons
    convert_poly_to_multi(conn, schema, table)
    
    # todo: delete or fix lines and points
