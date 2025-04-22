import logging
import json
import os
from io import StringIO
from time import sleep
import requests
from osgeo.gdal import VectorTranslateOptions, VectorTranslate
from tempfile import NamedTemporaryFile

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
                            raise Exception('Retries exceeded')

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


def get_all_features(url, ids, out_sr=3857, out_fields=None, chunk_size=2000, format='json'):
    """
    Fetches all features from a feature service using object ids
    :param url: REST endpoint of feature service
    :param ids: list of Object IDs
    :param out_sr: output spatial reference
    :param out_fields: out fields for query
    :param chunk_size: check size for batch feature request
    :return: None
    """
    logging.info(f'getting all features for {url}')
    if out_fields is None:
        out_fields = ['*']
    start = 0
    total = 0
    str_ids = [str(i) for i in ids]
    while True:  # This loops through the service and compiles the output into all_features
        try:
            # create comma-separated string of ids from chunk of id list
            id_list = ','.join(str_ids[start:start + chunk_size])
            # if no ids, break
            if id_list == '':
                break
            # query params
            params = {'f': format, 'outSR': 4326 if format == 'geojson' else out_sr, 'outFields': ','.join(out_fields), 'returnGeometry': 'true',
                      'objectIds': id_list}

            start += chunk_size
            # fetch features and update total
            r = fetch_features(url + '/query', params)
            total += len(r)
            yield r
            logging.info(f'{total} of {len(ids)} fetched')
        except Exception as e:
            logging.error(e.args[0])
            raise e
    if total != len(ids):
        logging.warning(f'missing features: {total} of {len(ids)} collected')


@retry(retries=2, on_failure=fetch_failure)
def fetch_features(url, params):
    """
    get features from a feature service
    :param url: REST endpoint of feature service
    :param params: query parameters
    :return: JSON features
    """
    try:
        r = requests.post(url, data=params)
        r_json = r.json()
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
        json.dump(fset, f)
    out_fc = os.path.join(gdb, fc_name)

    json_to_gdb(f'{fc_name}.json', gdb, fc_name, 'overwrite')

    # delete JSON
    os.remove(f.name)
    return out_fc


def service_to_postgres(service_url, where_clause, wkid, ogr_db_string, schema, destination_table, cursor, call_insert_function,chunk_size = 70):
    """
    service_to_postgres allows the capture of records from services that break other methods
    this method is much slower, and should be used when other methods are exhauseted
    :param service_url: REST endpoint of feature service
    :param where_clause:  where clause for filtering features to fetch
    :param wkid: out spatial reference WKID
    :param ogr_db_string: OGR connection string for Postgres
    :param schema: destination postgres schema
    :param destination_table: destination postgres table
    :param cursor: psycopg2 cursor
    :param sde_file: sde_file connected to target schema
    :param call_insert_function: calls insert function passed in to insert from additions table to destination table
    :param chunk_size: check size for batch feature request
    """
    #clear data from the destination table
    #destination table must be in place and have the proper schema
    cursor.execute(f'TRUNCATE {schema}.{destination_table}')
    cursor.execute('COMMIT;')

    # service_fl = arcgis.features.FeatureLayer(service_url)
    # service_additions_postgres = os.path.join(sde_file, f'{database}.{schema}.{destination_table}_additions')

    #fetches all ids that will be added
    ids = get_ids(service_url, where=where_clause)
    # str_ids = [str(i) for i in ids]
    # start = 0
    # total_len = len(ids)

    # overwrite to create table on first run then append
    access_mode = 'overwrite'
    for features in get_all_features(service_url, ids, wkid, out_fields=['*'], chunk_size=chunk_size, format='geojson'):
        # todo: remove _additions from here
        json_to_postgres(json.dumps(features), ogr_db_string, f'{destination_table}_additions', access_mode)
        access_mode = 'append'
        # count, fc = query_by_id_and_save_to_fc(id_list, service_fl, service_additions_postgres, wkid, f'{destination_table}_additions')

    # todo: expand try to writing each chunk to the database?
    # if (count > 0):
    #     try:
    #         #insert additions to destination table
    #         call_insert_function(cursor, schema)
    #
    #     except Exception as e:
    #         logging.error(e.args[0])
    #         raise e

    # todo: fix logging here
    # logging.info(f'{start+chunk_size} of {total_len} records inserted into {destination_table}')
    # start+=chunk_size


def json_to_postgres(query_url, ogr_db_string, table_name, access_mode='overwrite'):
    """
    Function to convert JSON to Postgres table
    :param json_string: JSON string to convert
    :param ogr_db_string: OGR connection string for Postgres
    :param table_name: name of the table to create in Postgres
    :return: None
    """

    options = VectorTranslateOptions(format='PostgreSQL',
                                     accessMode=access_mode,
                                     layerName=table_name)

    pass
    # with NamedTemporaryFile(suffix='.geojson') as f:
    #     f.write(json_string.encode('utf-8'))
    #     VectorTranslate(destNameOrDestDS=ogr_db_string, srcDS=f"GEOJSON:{f.name}", options=options)

def json_to_gdb(json_string, gdb, table_name, access_mode='overwrite'):
    """
    Function to convert JSON to Postgres table
    :param json_string: JSON string to convert
    :param ogr_db_string: OGR connection string for Postgres
    :param table_name: name of the table to create in Postgres
    :return: None
    """

    options = VectorTranslateOptions(format='FileGDB',
                                     accessMode=access_mode,
                                     layerName=table_name)

    with StringIO(json_string) as f:
        VectorTranslate(destNameOrDestDS=gdb, srcDS=f, options=options)
