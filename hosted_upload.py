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
from sweri_utils.download import retry

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    filename='./hosted_upload.log',
    encoding='utf-8',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S'
)

def return_db_connection_url(db_host: str, db_port: int, db_name: str, db_user: str, db_password: str) :
    return f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

def get_view_data_source_id(view):
    view_flc = FeatureLayerCollection.fromitem(view)

    if len(view_flc.manager.properties.get('adminServiceInfo').get('serviceSource')) ==  1:
        source_id = view_flc.manager.properties.get('adminServiceInfo').get('serviceSource')[0].get('serviceItemId')
    else:
        raise ValueError(f"{len(view_flc.manager.properties.get('adminServiceInfo').get('serviceSource'))} sources returned, 1 expected")

    return source_id

def gdf_to_features(gdf):

    sdf = pd.DataFrame.spatial.from_geodataframe(gdf)
    features_to_add = sdf.spatial.to_featureset().features

    return features_to_add

def build_postgis_chunk_query (schema, table, chunk_size, objectid = 0):

    query = f"""
        SELECT * FROM {schema}.{table}
        WHERE
        objectid > {objectid}
        ORDER BY objectid ASC
        limit {chunk_size};
    """
    return query

def postgis_query_to_gdf(pg_query, pg_con, geom_field = 'shape'):
    gdf = geopandas.GeoDataFrame.from_postgis(pg_query, pg_con, geom_col=geom_field)

    if gdf.empty:
        return None, None

    # set max_objectid to the max or last record in the gdf for objectid column
    max_objectid = gdf.iloc[-1]['objectid']
    gdf.drop(columns=['objectid', 'gdb_geomattr_data'], inplace=True, errors='ignore')

    return gdf, max_objectid

def retry_upload(func, *args, **kwargs):
    chunk = kwargs.get('chunk_size', args[3])
    if chunk > 1:
        new_chunk = chunk/2
        func(chunk_size=new_chunk, *args, **kwargs)
    else:
        raise Exception('Upload Tries Exceeded')

@retry(retries=2, on_failure=retry_upload)
def upload_chunk_to_feature_layer (feature_layer, schema, table, chunk_size, current_objectid, db_con, date_fields):
    sql_query = build_postgis_chunk_query(schema, table, chunk_size, current_objectid)
    features_gdf, current_objectid = postgis_query_to_gdf(sql_query, db_con, geom_field='shape')

    if current_objectid is None:
        return None

    features = gdf_to_features(features_gdf)

    # null dates get set to nan and break the feature class
    for feature in features:
        for field in date_fields:
            if math.isnan(feature.attributes[field]):
                feature.attributes[field] = None

    additions = feature_layer.edit_features(adds=features)

    print(additions)
    logging.info(current_objectid)

    return current_objectid



def load_postgis_to_feature_layer(feature_layer, db_con, schema, table, chunk_size, current_objectid):

    #pull 1 feature to scrape date fields
    sql_query = build_postgis_chunk_query(schema, table, 1, current_objectid)
    features_gdf, unused_id = postgis_query_to_gdf(sql_query, db_con, geom_field='shape')
    date_fields = features_gdf.select_dtypes(include=['datetime64[ns]', 'datetime']).columns.tolist()

    while True:
        current_objectid = upload_chunk_to_feature_layer(feature_layer, schema, table, chunk_size, current_objectid, db_con, date_fields)
        if current_objectid is None:
            break


def refresh_feature_data_and_swap_view_source(gis_con, view_id, source_feature_layer_ids, con, schema, table, chunk_size, start_objectid):

    token = gis_con.session.auth.token

    view_item = gis.content.get(view_id)

    current_data_source_id = get_view_data_source_id(view_item)

    new_data_source_id = next(id for id in source_feature_layer_ids if id != current_data_source_id)
    new_source_item = gis.content.get(new_data_source_id)

    if(len(new_source_item.layers)) == 1:
        new_source_feature_layer = new_source_item.layers[0]
    else:
        raise ValueError(f"{len(new_source_item.layers)} sources returned, 1 expected")

    new_source_feature_layer.manager.truncate()

    load_postgis_to_feature_layer(new_source_feature_layer, con, schema, table, chunk_size, start_objectid)
    swizzle_service('https://gis.reshapewildfire.org/', view_item.name, new_source_item.name, token)

if __name__ == '__main__':
    load_dotenv('.env')

    db_connection_url = return_db_connection_url(os.getenv('DOCKER_DB_HOST'), os.getenv('DOCKER_DB_PORT'),
                                                 os.getenv('DOCKER_DB_NAME'),
                                                 os.getenv('DOCKER_DB_USER'), os.getenv('DOCKER_DB_PASSWORD'))
    pg_con = create_engine(db_connection_url)
    postgis_schema = os.getenv('DOCKER_DB_SCHEMA')
    postgis_table = os.getenv('DOCKER_DB_TABLE')
    start_objectid = 0
    chunk = 1000

    gis = GIS("https://gis.reshapewildfire.org/arcgis", os.getenv("ESRI_USER"), os.getenv("ESRI_PW"))

    hosted_feature_ids = ['1bb527c9800b4519b5c2d1923c1d6870', '27164729c8bc48cd81e9308e691560ac']
    view_item_id = '6b819ca96ceb4d0fb31465932344515f'

    # For polygon layer
    refresh_feature_data_and_swap_view_source(gis, view_item_id, hosted_feature_ids, pg_con, postgis_schema, postgis_table, chunk, start_objectid)


