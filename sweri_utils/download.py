import logging
import json
import os
from time import sleep
import requests
from osgeo.gdal import VectorTranslateOptions, VectorTranslate
from datetime import datetime
from .logging import log_this

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
def service_to_postgres(service_url, where_clause, wkid, ogr_db_string, schema, destination_table, conn, overwrite_destination=False, chunk_size = 70):
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
    # todo: we dont need this if we use overwrite below
    cursor = conn.cursor()
    with conn.transaction():
        cursor.execute(f'TRUNCATE {schema}.{destination_table}')

        #fetches all ids that will be added
        ids = get_ids(service_url, where=where_clause)

        # overwrite to create table on first run then append
        access_mode = 'overwrite' if overwrite_destination else 'append'
        for r in get_all_features(service_url, ids, wkid, out_fields=['*'], chunk_size=chunk_size, format='json', return_full_response=True):
            # convert epoch time to iso format for esriFieldTypeDate fields
            date_fields = [x['name'] for x in r['fields'] if x['type'] == 'esriFieldTypeDate']
            for f in r['features']:
                for d in date_fields:
                    if d in f['attributes'] and f['attributes'][d] is not None:
                        f['attributes'][d] = int(f['attributes'][d]) / 1000
                        f['attributes'][d] = datetime.fromtimestamp(f['attributes'][d]).isoformat()

            options = VectorTranslateOptions(format='PostgreSQL',
                                             accessMode=access_mode,
                                             geometryType=['POLYGON', 'PROMOTE_TO_MULTI'],
                                             layerName=destination_table)

            VectorTranslate(destNameOrDestDS=ogr_db_string, srcDS=f"ESRIJSON:{json.dumps(r)}", options=options)

            # access mode is always append after the first run
            access_mode = 'append'

        conn.commit()
