import json
from osgeo.gdal import VectorTranslate, VectorTranslateOptions
from .download import  fetch_features
from worker import app

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

@app.task()
def download_and_insert_service_chunk(ids, url, out_sr, chunk_size, schema, destination_table, ogr_db_string, out_fields):
    params = get_query_params_chunk(ids, out_sr, out_fields, chunk_size, format="json")
    r = fetch_features(url + '/query', params)
    options = VectorTranslateOptions(format='PostgreSQL',
                                     accessMode='append',
                                     geometryType=['POLYGON', 'PROMOTE_TO_MULTI'],
                                     layerName=f'{schema}.{destination_table}_buffer')
    # commit chunks to database in
    _ = VectorTranslate(destNameOrDestDS=ogr_db_string, srcDS=f"ESRIJSON:{json.dumps(r)}", options=options)
    del _