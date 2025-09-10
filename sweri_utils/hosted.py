import geopandas
import math
from arcgis import gis
from arcgis.features import FeatureLayerCollection, GeoAccessor
from celery import group
from sqlalchemy import create_engine
from .sql import create_db_conn_from_envs
from .swizzle import swizzle_service
from .download import retry
from .sweri_logging import log_this, logging
from arcgis.gis import GIS
from worker import app



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

def gdf_to_feature_set(gdf):
    # Convert GeoDataFrame to SEDF to featureset
    sdf = GeoAccessor.from_geodataframe(gdf)
    feature_set = sdf.spatial.to_featureset()

    return feature_set

def build_postgis_chunk_query(schema, table, object_ids):

    object_ids_str = ", ".join(str(i) for i in object_ids)

    query = f"""
        SELECT * FROM {schema}.{table}
        WHERE
        objectid IN ({object_ids_str});
    """
    return query



def postgis_query_to_gdf(pg_query, sql_engine, geom_field='shape'):
    with sql_engine.begin() as conn:
        gdf = geopandas.read_postgis(pg_query, conn, geom_col=geom_field)

    if not gdf.empty:
        gdf.drop(columns=['objectid', 'gdb_geomattr_data'], inplace=True, errors='ignore')

    return gdf


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


def refresh_gis(gis_url, gis_user, gis_password):
    gis = GIS(gis_url,gis_user, gis_password)
    return gis

def get_object_id_chunks(conn, schema, table, where, chunk_size=500):
    ids = get_object_ids(conn, schema, table, where)

    if chunk_size == 1:
        for id in ids:
            yield [id]
        return

    for i in range(0, len(ids), chunk_size):
        yield ids[i:i + chunk_size]

@log_this
def hosted_upload_and_swizzle(gis_url, gis_user, gis_password, view_id, source_feature_layer_ids, schema, table, max_points_before_single_geom_chunk, chunk_size):
    # setup new layer connection
    gis_con = refresh_gis(gis_url, gis_user, gis_password)
    view_item = gis_con.content.get(view_id)
    current_data_source_id = get_view_data_source_id(view_item)

    new_data_source_id = next(id for id in source_feature_layer_ids if id != current_data_source_id)
    new_source_feature_layer = get_feature_layer_from_item(gis_url, gis_user, gis_password, new_data_source_id)
    new_source_feature_layer.manager.truncate()
    conn = create_db_conn_from_envs()
    # list of tasks
    t = []
    # for chunk_ids in get_object_id_chunks(conn, schema, table, f'ST_NPoints(shape) > {max_points_before_single_geom_chunk}', 1):
    #     t.append(upload_chunk_to_feature_layer.s(gis_url, gis_user, gis_password, new_data_source_id, schema, table,
    #                                              chunk_ids))

    for chunk_ids in get_object_id_chunks(conn, schema, table, f'ST_NPoints(shape) <= {max_points_before_single_geom_chunk}', chunk_size):
        t.append(upload_chunk_to_feature_layer.s(gis_url, gis_user, gis_password, new_data_source_id, schema, table,
                                                 chunk_ids))

    g = group(t)()
    g.get()
    # refreshing old references before swizzle service
    view_item = gis_con.content.get(view_id)
    new_source_item = gis_con.content.get(new_data_source_id)
    token = gis_con.session.auth.token

    swizzle_service('https://gis.reshapewildfire.org/', view_item.name, new_source_item.name, token)


def get_feature_layer_from_item(gis_url, gis_user, gis_password,  new_data_source_id):
    gis_con = refresh_gis(gis_url, gis_user, gis_password)
    new_source_item = gis_con.content.get(new_data_source_id)
    if (len(new_source_item.layers)) == 1:
        new_source_feature_layer = next(iter(new_source_item.layers))
    else:
        raise ValueError(f"{len(new_source_item.layers)} sources returned, 1 expected")

    return new_source_feature_layer

def get_object_ids(conn, schema, table, where = '1=1'):
    q = f"SELECT objectid FROM {schema}.{table} WHERE {where};"
    cursor = conn.cursor()
    try:
        with conn.transaction():
            cursor.execute(q)
            results = cursor.fetchall()
            ids = [r[0] for r in results]

    except Exception as err:
        logging.error(f'error getting objectids: {err}, {q}')
        raise err
    return ids


from worker import app

@app.task()
def upload_chunk_to_feature_layer(gis_url, gis_user, gis_password, new_source_id, schema, table, object_ids):
    feature_layer = get_feature_layer_from_item(gis_url, gis_user, gis_password, new_source_id)
    conn = create_db_conn_from_envs()
    sql_engine = create_engine("postgresql+psycopg://", creator=lambda: conn)
    sql_query = build_postgis_chunk_query(schema, table, object_ids)
    features_gdf = postgis_query_to_gdf(sql_query, sql_engine, geom_field='shape')

    features = gdf_to_features(features_gdf)

    for feature in features:
        for key, value in feature.attributes.items():
            if isinstance(value, float) and math.isnan(value):
                feature.attributes[key] = None

    feature_layer.edit_features(adds=features)
    conn.close()

# def upload_chunk_to_feature_layer(gis_url, gis_user, gis_password, new_source_id, schema, table, object_ids):
#
#
#     feature_layer = get_feature_layer_from_item(gis_url, gis_user, gis_password, new_source_id)
#
#     conn = create_db_conn_from_envs()
#     sql_engine = create_engine("postgresql+psycopg://", creator=lambda: conn)
#     sql_query = build_postgis_chunk_query(schema, table, object_ids)
#     features_gdf = postgis_query_to_gdf(sql_query, sql_engine, geom_field='shape')
#
#     fs = gdf_to_feature_set(features_gdf)
#
#     result = feature_layer.append(
#         edits=fs,
#         upload_format="featureCollection"
#     )
#
#     print(result)
#
#     conn.close()