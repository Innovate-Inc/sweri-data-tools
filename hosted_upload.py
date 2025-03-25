from arcgis.gis import GIS
from dotenv import load_dotenv
import os
from arcgis.features import FeatureLayer, FeatureLayerCollection
from osgeo import ogr
from sqlalchemy import create_engine, Table, MetaData, select, func
import geopandas
import logging
import pandas as pd
import json

def return_db_connection_url(db_host: str, db_port: int, db_name: str, db_user: str, db_password: str) :
    return f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

def postgres_query_to_gdf(pg_query, pg_con, geom_field = 'shape'):
    gdf = geopandas.GeoDataFrame.from_postgis(pg_query, pg_con, geom_col=geom_field)

    if gdf.empty:
        return None, None

    max_objectid = gdf.iloc[-1]['objectid']  # set max_objectid to the max or last record in the gdf for objectid column (can do by -1 index most likely)
    gdf.drop(columns=['objectid', 'gdb_geomattr_data'], inplace=True, errors='ignore')

    return gdf, max_objectid

def gdf_to_features(gdf):

    sdf = pd.DataFrame.spatial.from_geodataframe(gdf)
    features_to_add = sdf.spatial.to_featureset().features

    return features_to_add


def postgres_chunk_query(schema, table, chunk_size, objectid = 0):

    query = f"""
        SELECT * FROM {schema}.{table}
        WHERE
        objectid > {objectid}
        ORDER BY objectid ASC
        limit {chunk_size};
    """
    return query

if __name__ == '__main__':
    load_dotenv('.env')

    logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', filename='./hosted_upload.log',
                        encoding='utf-8', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')

    gis = GIS("https://gis.reshapewildfire.org/arcgis", os.getenv("ESRI_USER"), os.getenv("ESRI_PW"))
    target_schema = os.getenv('DOCKER_DB_SCHEMA')
    target_table = 'treatment_index'

    hosted_feature_id = '1557c605657b413886df816a8796fce8'
    hosted_item = gis.content.get(hosted_feature_id)
    hosted_feature_layer = hosted_item.layers[0]
    # Change view to look at newly updated hosted feature layer

    # Create 2 hosted feature layers in the test folder
    hosted_feature_layer.delete_features(where = '1=1')

    db_connection_url = return_db_connection_url(os.getenv('DOCKER_DB_HOST'), os.getenv('DOCKER_DB_PORT'), os.getenv('DOCKER_DB_NAME'),
                                                 os.getenv('DOCKER_DB_USER'), os.getenv('DOCKER_DB_PASSWORD'))
    con = create_engine(db_connection_url)

    logging.info(f"{target_table}, {target_schema}")

    current_objectid = 0
    chunk_size = 10

    while True:
        sql_query = postgres_chunk_query(target_schema, target_table, chunk_size, current_objectid)
        features_gdf, current_objectid = postgres_query_to_gdf(sql_query, con, geom_field = 'shape')

        if current_objectid is None:
            break

        features = gdf_to_features(features_gdf)
        additions = hosted_feature_layer.edit_features(adds=features)

        print(additions)
        logging.info(current_objectid)
        if "'success': False" in additions:
            logging.info(additions)
