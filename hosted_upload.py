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
from sweri_utils.sql import connect_to_pg_db
from datetime import datetime

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    filename='./hosted_upload.log',
    encoding='utf-8',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S'
)

def get_view_data_source_id(view):
    view_flc = FeatureLayerCollection.fromitem(view)

    if len(view_flc.manager.properties.get('adminServiceInfo').get('serviceSource')) ==  1:
        service_sources = view_flc.manager.properties.get('adminServiceInfo').get('serviceSource')
        source_id = next(iter(service_sources)).get('serviceItemId')

    else:
        raise ValueError(f"{len(view_flc.manager.properties.get('adminServiceInfo').get('serviceSource'))} sources returned, 1 expected")

    return source_id

def gdf_to_features(gdf):

    # Step 1: Convert GeoDataFrame to SEDF
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
    chunk = args[3]
    if chunk > 1:
        new_chunk = int(chunk/2)
        new_args = list(args)
        new_args[3] = new_chunk
        return func(*new_args, **kwargs)
    else:
        raise Exception('Upload Tries Exceeded')

@retry(retries=1, on_failure=retry_upload)
def upload_chunk_to_feature_layer (feature_layer, schema, table, chunk_size, current_objectid, db_con):
    sql_query = build_postgis_chunk_query(schema, table, chunk_size, current_objectid)
    features_gdf, current_objectid = postgis_query_to_gdf(sql_query, db_con, geom_field='shape')

    if current_objectid is None:
        return None

    features = gdf_to_features(features_gdf)

    print(datetime.now())

    for feature in features:
        for key, value in feature.attributes.items():
            if isinstance(value, float) and math.isnan(value):
                feature.attributes[key] = None

    print(datetime.now())

    additions = feature_layer.edit_features(adds=features)

    print(additions)
    logging.info(current_objectid)

    return current_objectid



def load_postgis_to_feature_layer(feature_layer, sqla_engine, schema, table, chunk_size, current_objectid):

    while True:
        current_objectid = upload_chunk_to_feature_layer(feature_layer, schema, table, chunk_size, current_objectid, sqla_engine)
        if current_objectid is None:
            break


def refresh_feature_data_and_swap_view_source(gis_con, view_id, source_feature_layer_ids, psycopg_con, schema, table, chunk_size, start_objectid):

    sql_engine = create_engine("postgresql+psycopg://", creator=lambda: psycopg_con)

    view_item = gis.content.get(view_id)

    current_data_source_id = get_view_data_source_id(view_item)

    new_data_source_id = next(id for id in source_feature_layer_ids if id != current_data_source_id)
    new_source_item = gis.content.get(new_data_source_id)

    if(len(new_source_item.layers)) == 1:
        new_source_feature_layer = next(iter(new_source_item.layers))
    else:
        raise ValueError(f"{len(new_source_item.layers)} sources returned, 1 expected")

    new_source_feature_layer.manager.truncate()


    load_postgis_to_feature_layer(new_source_feature_layer, sql_engine, schema, table, chunk_size, start_objectid)

    #refreshing old references before swizzle service
    view_item = gis.content.get(view_id)
    new_source_item = gis.content.get(new_data_source_id)
    token = gis_con.session.auth.token

    swizzle_service('https://gis.reshapewildfire.org/', view_item.name, new_source_item.name, token)

if __name__ == '__main__':
    load_dotenv('.env')

    cur, conn = connect_to_pg_db(os.getenv('RDS_DB_HOST'), os.getenv('RDS_DB_PORT'), os.getenv('RDS_DB_NAME'), os.getenv('RDS_DB_USER'), os.getenv('RDS_DB_PASSWORD'))

    postgis_schema = os.getenv('RDS_DB_SCHEMA')
    postgis_table = os.getenv('RDS_DB_TABLE')
    start_objectid = 0
    chunk = 1000

    gis = GIS("https://gis.reshapewildfire.org/arcgis", os.getenv("ESRI_USER"), os.getenv("ESRI_PW"))

    treatment_index_data_ids = [os.getenv('TREATMENT_INDEX_DATA_ID_1'), os.getenv('TREATMENT_INDEX_DATA_ID_2')]
    treatment_index_view_id = os.getenv('TREATMENT_INDEX_VIEW_ID')

    # Treatment Index Polygons
    refresh_feature_data_and_swap_view_source(gis, treatment_index_view_id, treatment_index_data_ids, conn, postgis_schema, postgis_table, chunk, start_objectid)

    treatment_index_points_data_ids = [os.getenv('TREATMENT_INDEX_POINTS_DATA_ID_1'), os.getenv('TREATMENT_INDEX_POINTS_DATA_ID_2')]
    treatment_index_points_view_id = os.getenv('TREATMENT_INDEX_POINTS_VIEW_ID')

    # Treatment Index Points
    refresh_feature_data_and_swap_view_source(gis, treatment_index_points_view_id, treatment_index_points_data_ids, conn, postgis_schema, postgis_table, chunk, start_objectid)

    daily_progression_data_ids = [os.getenv('TREATMENT_INDEX_POINTS_DATA_ID_1'), os.getenv('TREATMENT_INDEX_POINTS_DATA_ID_2')]
    daily_progression_view_id = os.getenv('TREATMENT_INDEX_POINTS_VIEW_ID')

    # Daily Progression
    refresh_feature_data_and_swap_view_source(gis, daily_progression_view_id, daily_progression_data_ids, conn, postgis_schema, postgis_table, chunk, start_objectid)
