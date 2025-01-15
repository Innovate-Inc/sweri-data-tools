import os
import  requests as r
from dotenv import load_dotenv
# from osgeo import ogr
from sweri_utils.sql import connect_to_pg_db

def get_intersection_features(url, layer_id=0):
    intersection_r = r.get(url + f'/{layer_id}/query',
                           params={'f': 'json', 'where': '1=1', 'outFields': '*'})
    intersections_json = intersection_r.json()
    if 'features' not in intersections_json:
        raise KeyError('features')
    return intersections_json['features']

def configure_intersection_sources(features, coded_vals):
    intersect_sources = {}
    intersect_targets = {}

    for f in features:
        att = f['attributes']
        s = {'source': att['source'], 'id': att['uid_fields'], 'source_type': att['source_type']}
        if att['name'] in coded_vals:
            s['name'] = coded_vals[att['name']]
        intersect_sources[att['id_source']] = s
        if att['use_as_target'] == 1:
            intersect_targets[att['id_source']] = s
    return intersect_sources, intersect_targets


def query_coded_value_domain(url, layer):
    domain_r = r.get(url + f'/queryDomains',
                      params={'f': 'json', 'layers': layer})
    d_json = domain_r.json()
    if 'domains' not in d_json or ('domains' in d_json and len(d_json['domains']) == 0):
        raise ValueError('missing domains')
    if 'codedValues' not in d_json['domains'][0]:
        raise ValueError('missing coded values or incorrect domain type')
    cv = d_json['domains'][0]['codedValues']
    return {c['code']: c['name'] for c in cv}

if __name__ == '__main__':
    # Define the gdb and sde and load .env
    load_dotenv()
    wkid = 3857
    intersection_src_url = os.getenv('INTERSECTION_SOURCES_URL')
    # setup intersection source features
    intersections = get_intersection_features(intersection_src_url)
    # handle coded value domains
    cvs = query_coded_value_domain(intersection_src_url, 0)
    # configure intersection sources
    sources = configure_intersection_sources(intersections, cvs)
    # get db schema
    schema = os.getenv('SCHEMA')
    # psycopg2 connection, because arcsde connection is extremely slow during inserts
    pg_cursor = connect_to_pg_db(
        os.getenv('DB_HOST'),
        os.getenv('DB_PORT'),
        os.getenv('DB_NAME'),
        os.getenv('DB_USER'),
        os.getenv('DB_PASSWORD')
    )

    # create the template for the new intersect
    treatment_intersections = os.path.join(os.getenv('SDE_FILE'), f'sweri.{schema}.intersections')
    print(pg_cursor.execute('select * from information_schema.tables'))
    print('did something')
     # fetch all features and create featureclass from intersection features
    