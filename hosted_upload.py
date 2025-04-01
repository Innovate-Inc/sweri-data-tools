from arcgis.gis import GIS
from dotenv import load_dotenv
import os
from arcgis.features import FeatureLayer, FeatureLayerCollection
from sqlalchemy import create_engine
import geopandas
import logging
import pandas as pd
import math
from sweri_utils.swizzle import swizzle_service

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

def main():
    load_dotenv('.env')

    logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', filename='./hosted_upload.log',
                        encoding='utf-8', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')

    db_connection_url = return_db_connection_url(os.getenv('DOCKER_DB_HOST'), os.getenv('DOCKER_DB_PORT'),
                                                 os.getenv('DOCKER_DB_NAME'),
                                                 os.getenv('DOCKER_DB_USER'), os.getenv('DOCKER_DB_PASSWORD'))
    con = create_engine(db_connection_url)
    target_schema = os.getenv('DOCKER_DB_SCHEMA')
    target_table = 'treatment_index'
    gis = GIS("https://gis.reshapewildfire.org/arcgis", os.getenv("ESRI_USER"), os.getenv("ESRI_PW"))
    token = gis.session.auth.token

    hosted_feature_ids = ['1bb527c9800b4519b5c2d1923c1d6870','27164729c8bc48cd81e9308e691560ac']

    view_id = '6b819ca96ceb4d0fb31465932344515f'
    view_item = gis.content.get(view_id)
    view_flc = FeatureLayerCollection.fromitem(view_item)

    if len(view_flc.manager.properties.get('adminServiceInfo').get('serviceSource')) ==  1:
        current_data_source_id = view_flc.manager.properties.get('adminServiceInfo').get('serviceSource')[0].get('serviceItemId')
    else:
        raise ValueError(f"{len(view_flc.manager.properties.get('adminServiceInfo').get('serviceSource'))} sources returned, 1 expected")

    new_data_source_id = next(id for id in hosted_feature_ids if id != current_data_source_id)
    new_source_item = gis.content.get(new_data_source_id)

    if(len(new_source_item.layers)) == 1:
        new_source_feature_layer = new_source_item.layers[0]
    else:
        raise ValueError(f"{len(new_source_item.layers)} sources returned, 1 expected")

    new_source_feature_layer.manager.truncate()

    start_objectid = 0
    chunk_size = 1000

    postgis_table_to_hosted_feature_class(new_source_feature_layer, con, target_schema, target_table, chunk_size, start_objectid)

    swizzle_service('https://gis.reshapewildfire.org/', view_item.name, new_source_feature_layer.name, token)

if __name__ == '__main__':
    main()



