import logging
import json
import os
from time import sleep
import requests
from osgeo.gdal import VectorTranslateOptions, VectorTranslate
from datetime import datetime
from .sweri_logging import log_this

try:
    import arcpy
except ModuleNotFoundError:
    logging.warning('Missing arcpy, some functions will not work')

def get_fields(service_url):
    """
    Function for fetching fields from a service
    :param service_url: REST endpoint of service
    :return: Esri JSON fields object
    """
    params = {
        'properties': None,
        'f': 'json'
    }
    r = requests.get(service_url, params=params)
    r_json = r.json()
    if 'fields' not in r_json:
        print('fields ', json.dumps(r_json))
        raise KeyError('fields')
    return r_json['fields']

def retry(retries, on_failure):
    """
    wrapper function for retrying a function a specified number of times, with a callback function for failed tries
    :param retries: number of retries
    :param on_failure: callback function for retrying failed tries, after exceeding the number of retries
    :return: function
    """

    def inner(func):
        def wrapper(*args, **kwargs):
            current_retry = 0
            while True:
                current_retry += 1
                try:
                    r = func(*args, **kwargs)
                    return r
                except Exception as e:
                    if current_retry <= retries:
                        current_retry += 1
                        sleep(current_retry ** 2 * 10)
                    else:
                        try:
                            on_failure(func, *args, **kwargs)
                        except Exception as e:
                            raise Exception(f'Retries exceeded: {e}')

        return wrapper

    return inner


def fetch_failure(func, *args, **kwargs):
    """
    When primary request fails, starts requesting smaller and smaller chunks of features
    :param func: original failing function to retry
    :param args: arguments passed to func
    :param kwargs: keyword arguments passed to func
    :return: None
    """
    logging.info('fetch_failure, retrying')
    params = kwargs.get('params', args[1])
    # check if objectids and count them otherwise
    current_limit = len(params['objectIds'].split(',')) if 'objectIds' in params else 1000
    current_limit = current_limit if 'limit' not in params else params['limit']
    if current_limit > 1:
        new_limit = round(current_limit / 2)
        params.update(
            {
                'limit': new_limit,
                'offset': 0
            }
        )
        logging.info(f'fetching with {params}')
        func(*args, **kwargs)
        params['offset'] = new_limit
        func(*args, **kwargs)
    else:
        raise Exception('Fetch failures retries exceeded')


@retry(retries=2, on_failure=fetch_failure)
def get_ids(service_url, where='1=1', geometry=None, geometry_type=None):
    """
    Function for fetching Object IDs from a service
    :param service_url: REST endpoint of feature service
    :param where: where clause for query
    :param geometry: optional geometry for query
    :param geometry_type: geometry type
    :return: list of Object IDs
    """
    geom_map = {
        'polygon': 'esriGeometryPolygon',
        'point': 'esriGeometryPoint',
        'extent': 'esriGeometryEnvelope',
        'multipoint': 'esriGeometryMultipoint',
        'polyline': 'esriGeometryPolyline'
    }

    count_params = {
        'where': where,
        'returnIdsOnly': 'true',
        'f': 'json'  # Specify the response format
    }

    if geometry and geometry_type in geom_map:
        count_params = {
            **count_params,
            'geometry': geometry,
            'geometryType': geom_map[geometry_type],
            'spatialRel': 'esriSpatialRelIntersects'
        }

    response = requests.post(service_url + "/query", data=count_params)
    data = response.json()
    if 'error' in data or response.status_code != 200:
        raise Exception(f'Failed to fetch object Ids, {response.status_code}: {data}')
    if 'objectIds' not in data or data.get('objectIds') is None:
        raise Exception('objectIds are missing from request')
    return data.get('objectIds')


def get_query_params_chunk(ids, out_sr=3857, out_fields=None, chunk_size=2000, format='json'):
    """
    Function for creating query parameters for a feature service
    :param ids: list of Object IDs
    :param out_sr: output spatial reference
    :param out_fields: out fields for query
    :param chunk_size: check size for batch feature request
    :return: query parameters
    """
    if out_fields is None:
        out_fields = ['*']
    start = 0
    str_ids = [str(i) for i in ids]
    while True:
        # create comma-separated string of ids from chunk of id list
        id_list = ','.join(str_ids[start:start + chunk_size])
        # if no ids, break
        if id_list == '':
            break
        # query params
        params = {'f': format, 'outSR': 4326 if format == 'geojson' else out_sr, 'outFields': ','.join(out_fields), 'returnGeometry': 'true',
                  'objectIds': id_list}
        start += chunk_size
        yield params


def get_all_features(url, ids, out_sr=3857, out_fields=None, chunk_size=2000, format='json', return_full_response=False):
    """
    Fetches all features from a feature service using object ids
    :param url: REST endpoint of feature service
    :param ids: list of Object IDs
    :param out_sr: output spatial reference
    :param out_fields: out fields for query
    :param chunk_size: check size for batch feature request
    :param format: format of the response, either 'json' or 'geojson'
    :param return_full_response: if True, returns the full response, else returns only the features
    :return: None
    """
    logging.info(f'getting all features for {url}')
    total = 0
    for params in get_query_params_chunk(ids, out_sr, out_fields, chunk_size, format):  # This loops through the service and compiles the output into all_features
        try:
            # fetch features and update total
            r = fetch_features(url + '/query', params, return_full_response)
            total += len(r) if not return_full_response else len(r['features'])
            yield r
            logging.info(f'{total} of {len(ids)} fetched')
        except Exception as e:
            logging.error(e.args[0])
            raise e
    if total != len(ids):
        logging.warning(f'missing features: {total} of {len(ids)} collected')


@retry(retries=2, on_failure=fetch_failure)
def fetch_features(url, params, return_full_response=False):
    """
    get features from a feature service
    :param url: REST endpoint of feature service
    :param params: query parameters
    :param return_full_response: if True, returns the full response, else returns only the features
    :return: JSON features
    """
    try:
        r = requests.post(url, data=params)
        r_json = r.json()

        if return_full_response:
            return r_json

        if 'features' not in r_json:
            raise KeyError(f'Error: {r.content} With Params: {json.dumps(params)}')
        return r_json['features']
    except Exception as e:
        raise e

def fetch_geojson_features(service_url, where, geometry=None, geom_type=None, out_sr=4326,
                                    out_fields=None, chunk_size=100):
    ids = get_ids(service_url, where, geometry, geom_type)
    out_features = []
    # get all features
    for f in get_all_features(service_url, ids, out_sr, out_fields, chunk_size, 'geojson'):
        out_features += f

    if len(out_features) == 0:
        raise Exception(f'No features fetched for ids: {ids}')
    return out_features


@log_this
def service_to_postgres(service_url, where_clause, wkid, ogr_db_string, schema, destination_table, conn, chunk_size = 70):
    """
    service_to_postgres allows the capture of records from services that break other methods
    this method is much slower, and should be used when other methods are exhauseted
    :param service_url: REST endpoint of feature service
    :param where_clause:  where clause for filtering features to fetch
    :param wkid: out spatial reference WKID
    :param ogr_db_string: OGR connection string for Postgres
    :param schema: destination postgres schema
    :param destination_table: destination postgres table
    :param conn: psycopg2 connection object
    :param chunk_size: check size for batch feature request
    """

    cursor = conn.cursor()
    # create a buffer table to hold the data without copying data
    # this is a workaround for the fact that vector translate does not support overwriting
    # the destination table if it is already in use
    with conn.transaction():
        # create table has to be commited to be used in another transaction
        cursor.execute(f'''
                   DROP TABLE IF EXISTS {schema}.{destination_table}_buffer;
                   CREATE TABLE {schema}.{destination_table}_buffer (LIKE {schema}.{destination_table} INCLUDING ALL);
               ''')

    #fetches all ids that will be added
    ids = get_ids(service_url, where=where_clause)

    for r in get_all_features(service_url, ids, wkid, out_fields=['*'], chunk_size=chunk_size, format='json', return_full_response=True):

        options = VectorTranslateOptions(format='PostgreSQL',
                                         accessMode='append',
                                         geometryType=['POLYGON', 'PROMOTE_TO_MULTI'],
                                         layerName=f'{schema}.{destination_table}_buffer')
        # commit chunks to database in
        _ = VectorTranslate(destNameOrDestDS=ogr_db_string, srcDS=f"ESRIJSON:{json.dumps(r)}", options=options)
        del _

    # copy data from buffer table to destination table
    with conn.transaction():
        cursor.execute(f'''TRUNCATE {schema}.{destination_table};''')
        cursor.execute(f'''INSERT INTO {schema}.{destination_table} (SELECT * FROM {schema}.{destination_table}_buffer);''')
        cursor.execute(f'''DROP TABLE {schema}.{destination_table}_buffer;''')

########################### arcpy required for below functions ###########################

def fetch_and_create_featureclass(service_url, where, gdb, fc_name, geometry=None, geom_type=None, out_sr=3857,
                                  out_fields=None, chunk_size = 2000):
    """
    fetches and converts ESRI JSON features to a FeatureClass in a geodatabase
    :param service_url: REST endpoint of Esri service with feature access enabled
    :param where: where clause for filtering features to fetch
    :param gdb: output geodatabase for new FeatureClass
    :param fc_name: name of new FeatureClass
    :param geometry: geometry to use for query, if any
    :param geom_type: type of geometry used for query, required only if geometry is provided
    :param out_sr: out spatial reference WKID
    :param out_fields: out fields for query
    :return: new FeatureClass
    """
    # get count
    ids = get_ids(service_url, where, geometry, geom_type)

    # if len(ids) > 10k reject request
    # at this time this function is only use for gp download tool so this is the simplest way to reject requests
    if len(ids) > 10000:
        raise Exception(f'Feature count {len(ids)} exceeds 10,000, please refine your query')

    out_features = []
    # get all features
    for f in get_all_features(service_url, ids, out_sr, out_fields, chunk_size):
        out_features += f
    if len(out_features) == 0:
        raise Exception(f'No features fetched for ids: {ids}')
    # save errors out if there is no geometry or attrbutes object
    fields = get_fields(service_url)
    fset = {'features': out_features, 'fields': fields, 'spatial_reference': {'wkid': out_sr}}
    # create a json file from the features

    with open(f'{fc_name}.json', 'w') as f:
        f.write(json.dumps(fset, separators=(',', ':')))

    arcpy.AddMessage(f'Converting features to FeatureClass {datetime.now()}')
    out_fc = os.path.join(gdb, fc_name)
    if arcpy.Exists(out_fc):
        arcpy.management.Delete(out_fc)
    out_fc = arcpy.conversion.JSONToFeatures(f.name, out_fc)
    # define the projection, without this FeaturesToJSON fails
    arcpy.management.DefineProjection(out_fc, arcpy.SpatialReference(out_sr))
    # delete JSON
    os.remove(f.name)
    return out_fc
