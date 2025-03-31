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
import math

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

def postgis_table_to_hosted_feature_class(feature_layer, db_con, schema, table, chunk, current_objectid):
    while True:
        sql_query = postgres_chunk_query(schema, table, chunk, current_objectid)
        features_gdf, current_objectid = postgres_query_to_gdf(sql_query, db_con, geom_field = 'shape')

        if current_objectid is None:
            break

        features = gdf_to_features(features_gdf)

        date_fields = features_gdf.select_dtypes(include=['datetime64[ns]', 'datetime']).columns.tolist()

        #null dates get set to nan and break the feature class
        for feature in features:
            for field in date_fields:
                if math.isnan(feature.attributes[field]):
                    feature.attributes[field] = None

        additions = feature_layer.edit_features(adds=features)

        logging.info(additions)
        logging.info(current_objectid)

if __name__ == '__main__':
    load_dotenv('.env')

    logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', filename='./hosted_upload.log',
                        encoding='utf-8', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')

    gis = GIS("https://gis.reshapewildfire.org/arcgis", os.getenv("ESRI_USER"), os.getenv("ESRI_PW"))
    target_schema = os.getenv('DOCKER_DB_SCHEMA')
    target_table = 'treatment_index'

    hosted_feature_id = '1bb527c9800b4519b5c2d1923c1d6870'
    hosted_item = gis.content.get(hosted_feature_id)
    hosted_feature_layer = hosted_item.layers[0]
    # Change view to look at newly updated hosted feature layer

    # Create 2 hosted feature layers in the test folder
    hosted_feature_layer.manager.truncate()

    db_connection_url = return_db_connection_url(os.getenv('DOCKER_DB_HOST'), os.getenv('DOCKER_DB_PORT'), os.getenv('DOCKER_DB_NAME'),
                                                 os.getenv('DOCKER_DB_USER'), os.getenv('DOCKER_DB_PASSWORD'))
    con = create_engine(db_connection_url)

    logging.info(f"{target_table}, {target_schema}")

    start_objectid = 0
    chunk_size = 1000

    postgis_table_to_hosted_feature_class(hosted_feature_layer, con, target_schema, target_table, chunk_size, start_objectid)


