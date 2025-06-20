from arcgis.gis import GIS
from dotenv import load_dotenv
import os
from arcgis.features import  FeatureLayerCollection, GeoAccessor, GeoSeriesAccessor, FeatureSet

from sqlalchemy import create_engine
import geopandas
import logging
import pandas as pd
import math
import watchtower

from sweri_utils.swizzle import swizzle_service
from sweri_utils.download import retry
from sweri_utils.sql import connect_to_pg_db

#logger setup
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.propagate = False

file_handler = logging.FileHandler('../hosted_upload.log', encoding='utf-8')
file_formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
file_handler.setFormatter(file_formatter)
file_handler.setLevel(logging.INFO)

logger.addHandler(file_handler)
logger.addHandler(watchtower.CloudWatchLogHandler())

def get_view_data_source_id(view):
    view_flc = FeatureLayerCollection.fromitem(view)

    if len(view_flc.manager.properties.get('adminServiceInfo').get('serviceSource')) ==  1:
        service_sources = view_flc.manager.properties.get('adminServiceInfo').get('serviceSource')
        source_id = next(iter(service_sources)).get('serviceItemId')

    else:
        raise ValueError(f"{len(view_flc.manager.properties.get('adminServiceInfo').get('serviceSource'))} sources returned, 1 expected")

    return source_id

def gdf_to_features(gdf):

    # Convert GeoDataFrame to SEDF to featureset
    sdf = GeoAccessor.from_geodataframe(gdf)
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

    for feature in features:
        for key, value in feature.attributes.items():
            if isinstance(value, float) and math.isnan(value):
                feature.attributes[key] = None

    additions = feature_layer.edit_features(adds=features)

    logging.info(additions)
    logging.info(current_objectid)
    return current_objectid

def load_postgis_to_feature_layer(feature_layer, sqla_engine, schema, table, chunk_size, current_objectid):

    while True:
        current_objectid = upload_chunk_to_feature_layer(feature_layer, schema, table, chunk_size, current_objectid, sqla_engine)
        if current_objectid is None:
            break


def hosted_upload_and_swizzle(gis_con, view_id, source_feature_layer_ids, psycopg_con, schema, table, chunk_size, start_objectid):
    sql_engine = create_engine("postgresql+psycopg://", creator=lambda: psycopg_con)

    view_item = gis_con.content.get(view_id)

    current_data_source_id = get_view_data_source_id(view_item)

    new_data_source_id = next(id for id in source_feature_layer_ids if id != current_data_source_id)
    new_source_item = gis_con.content.get(new_data_source_id)

    if(len(new_source_item.layers)) == 1:
        new_source_feature_layer = next(iter(new_source_item.layers))
    else:
        raise ValueError(f"{len(new_source_item.layers)} sources returned, 1 expected")

    new_source_feature_layer.manager.truncate()

    logging.info(f'beginning update for {new_source_item.name} from {schema}.{table}')


    load_postgis_to_feature_layer(new_source_feature_layer, sql_engine, schema, table, chunk_size, start_objectid)

    #refreshing old references before swizzle service
    view_item = gis_con.content.get(view_id)
    new_source_item = gis_con.content.get(new_data_source_id)
    token = gis_con.session.auth.token

    logging.info(f'swapping {view_item.name} to data source {new_source_item.name}')
    swizzle_service('https://gis.reshapewildfire.org/', view_item.name, new_source_item.name, token)

if __name__ == '__main__':
    load_dotenv()

    cur, conn = connect_to_pg_db(os.getenv('RDS_DB_HOST'), os.getenv('RDS_DB_PORT'), os.getenv('RDS_DB_NAME'), os.getenv('RDS_DB_USER'), os.getenv('RDS_DB_PASSWORD'))

    postgis_schema = os.getenv('RDS_DB_SCHEMA')
    start_objectid = 0
    chunk = 1000

    gis = GIS("https://gis.reshapewildfire.org/arcgis", os.getenv("ESRI_USER"), os.getenv("ESRI_PW"), expiration=120)

    # Treatment Index Points
    treatment_index_points_data_ids = [os.getenv('TREATMENT_INDEX_POINTS_DATA_ID_1'),
                                       os.getenv('TREATMENT_INDEX_POINTS_DATA_ID_2')]
    treatment_index_points_view_id = os.getenv('TREATMENT_INDEX_POINTS_VIEW_ID')
    treatment_index_points_table = 'treatment_index_points'

    hosted_upload_and_swizzle(gis, treatment_index_points_view_id, treatment_index_points_data_ids,
                                              conn, postgis_schema, treatment_index_points_table, chunk, start_objectid)

    #Refresh gis since it lasts 1 hour
    gis = GIS("https://gis.reshapewildfire.org/arcgis", os.getenv("ESRI_USER"), os.getenv("ESRI_PW"), expiration=120)

    # Daily Progression
    daily_progression_data_ids = [os.getenv('DAILY_PROGRESSION_DATA_ID_1'), os.getenv('DAILY_PROGRESSION_DATA_ID_2')]
    daily_progression_view_id = os.getenv('DAILY_PROGRESSION_VIEW_ID')
    daily_progression_table = 'daily_progression'

    hosted_upload_and_swizzle(gis, daily_progression_view_id, daily_progression_data_ids,
                                              conn, postgis_schema, daily_progression_table, chunk, start_objectid)

    # Refresh gis since it lasts 1 hour
    gis = GIS("https://gis.reshapewildfire.org/arcgis", os.getenv("ESRI_USER"), os.getenv("ESRI_PW"), expiration=120)

    # Treatment Index Polygons
    treatment_index_data_ids = [os.getenv('TREATMENT_INDEX_DATA_ID_1'), os.getenv('TREATMENT_INDEX_DATA_ID_2')]
    treatment_index_view_id = os.getenv('TREATMENT_INDEX_VIEW_ID')
    treatment_index_table = 'treatment_index'

    hosted_upload_and_swizzle(gis, treatment_index_view_id, treatment_index_data_ids, conn, postgis_schema, treatment_index_table, chunk, start_objectid)

