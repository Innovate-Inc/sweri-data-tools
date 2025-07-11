import geopandas
import math
from arcgis.features import FeatureLayerCollection, GeoAccessor
from sqlalchemy import create_engine

from .swizzle import swizzle_service
from .download import retry
from .sweri_logging import log_this, logging
from arcgis.gis import GIS


@log_this
def get_view_data_source_id(view):
    view_flc = FeatureLayerCollection.fromitem(view)

    if len(view_flc.manager.properties.get('adminServiceInfo').get('serviceSource')) == 1:
        service_sources = view_flc.manager.properties.get('adminServiceInfo').get('serviceSource')
        source_id = next(iter(service_sources)).get('serviceItemId')

    else:
        raise ValueError(
            f"{len(view_flc.manager.properties.get('adminServiceInfo').get('serviceSource'))} sources returned, 1 expected")

    return source_id


def gdf_to_features(gdf):
    # Convert GeoDataFrame to SEDF to featureset
    sdf = GeoAccessor.from_geodataframe(gdf)
    features_to_add = sdf.spatial.to_featureset().features

    return features_to_add


def build_postgis_chunk_query(schema, table, chunk_size, objectid=0):
    query = f"""
        SELECT * FROM {schema}.{table}
        WHERE
        objectid > {objectid}
        ORDER BY objectid ASC
        limit {chunk_size};
    """
    return query


def postgis_query_to_gdf(pg_query, pg_con, geom_field='shape'):
    gdf = geopandas.GeoDataFrame.from_postgis(pg_query, pg_con, geom_col=geom_field)

    if gdf.empty:
        return None, None

    # set max_objectid to the max or last record in the gdf for objectid column
    max_objectid = gdf.iloc[-1]['objectid']
    gdf.drop(columns=['objectid', 'gdb_geomattr_data'], inplace=True, errors='ignore')

    return gdf, max_objectid


@log_this
def retry_upload(func, *args, **kwargs):
    chunk = args[3]
    if chunk > 1:
        new_chunk = int(chunk / 2)
        new_args = list(args)
        new_args[3] = new_chunk
        return func(*new_args, **kwargs)
    else:
        raise Exception('Upload Tries Exceeded')


@retry(retries=1, on_failure=retry_upload)
@log_this
def upload_chunk_to_feature_layer(feature_layer, schema, table, chunk_size, current_objectid, db_con):
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

    return current_objectid


@log_this
def load_postgis_to_feature_layer(feature_layer, sqla_engine, schema, table, chunk_size, current_objectid):
    while True:
        current_objectid = upload_chunk_to_feature_layer(feature_layer, schema, table, chunk_size, current_objectid,
                                                         sqla_engine)
        if current_objectid is None:
            break

def refresh_gis(gis_url, gis_user, gis_password):
    gis = GIS(gis_url,gis_user, gis_password)
    return gis

@log_this
def hosted_upload_and_swizzle(gis_url, gis_user, gis_password, view_id, source_feature_layer_ids, psycopg_con, schema, table, chunk_size,
                              start_objectid):
    gis_con = refresh_gis(gis_url, gis_user, gis_password)
    sql_engine = create_engine("postgresql+psycopg://", creator=lambda: psycopg_con)

    view_item = gis_con.content.get(view_id)

    current_data_source_id = get_view_data_source_id(view_item)

    new_data_source_id = next(id for id in source_feature_layer_ids if id != current_data_source_id)
    new_source_item = gis_con.content.get(new_data_source_id)

    if (len(new_source_item.layers)) == 1:
        new_source_feature_layer = next(iter(new_source_item.layers))
    else:
        raise ValueError(f"{len(new_source_item.layers)} sources returned, 1 expected")

    new_source_feature_layer.manager.truncate()

    load_postgis_to_feature_layer(new_source_feature_layer, sql_engine, schema, table, chunk_size, start_objectid)

    # refreshing old references before swizzle service
    view_item = gis_con.content.get(view_id)
    new_source_item = gis_con.content.get(new_data_source_id)
    token = gis_con.session.auth.token

    swizzle_service('https://gis.reshapewildfire.org/', view_item.name, new_source_item.name, token)
