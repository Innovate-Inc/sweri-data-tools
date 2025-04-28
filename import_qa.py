import geopandas
from arcgis.apps.storymap.story_content import requests
from dotenv import load_dotenv
import os

os.environ["CRYPTOGRAPHY_OPENSSL_NO_LEGACY"] = "1"
import math
import logging
import datetime
import geopandas as gpd
import pandas as pd

from sweri_utils.sql import rename_postgres_table, connect_to_pg_db
from sweri_utils.download import fetch_and_create_featureclass, fetch_features

logger = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', filename='./import_qa.log', encoding='utf-8',
                    level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')

def get_feature_count(cursor, schema, treatment_index, source_database):
    cursor.execute(f"SELECT count(*) FROM {schema}.{treatment_index} where identifier_database = '{source_database}';")
    feature_count = cur.fetchone()[0]
    return feature_count

def get_sample_size(population_size, proportion = .5, margin_of_error = .05):
    # Uses Cochran's formula to calculate sample size
    # z=1.96 sets confidence to 95%
    z = 1.96
    p = proportion
    e = margin_of_error
    n0 = ((z**2) * p * (1-p)) / (e**2)
    n = n0 / (1 + ((n0-1) / population_size))

    return math.ceil(n)

def return_comparison(comparison_feature, sweri_feature, source_database, iterator_offset=0):
    field_equal = {}
    value_compare = {}
    iterator = 0 + iterator_offset

    value_compare[source_database] = 'sweri'

    if iterator_offset > 0:
        field_equal['id'] = sweri_feature[0]
        value_compare['id'] = sweri_feature[0]

    for key in comparison_feature['attributes']:
        field_equal[key] = comparison_feature['attributes'][key] == sweri_feature[iterator]
        value_compare[comparison_feature['attributes'][key]] = sweri_feature[iterator]

        iterator += 1

    if sweri_feature[-1] is None or comparison_feature.get('geometry') is None:
        pass
    else:
        geom_equal = arcpy.AsShape(comparison_feature['geometry'], True).equals(sweri_feature[-1])
        field_equal['geom'] = geom_equal
        value_compare['geom'] = geom_equal

    return field_equal, value_compare

def compare_features(comparison_feature, sweri_feature, fields):
    total_compare = True

    for i, f in enumerate(fields):
        try:
            sweri_value = sweri_feature[i]
            comp_value = comparison_feature['attributes'][f]

            if isinstance(sweri_value, float):
                total_compare = total_compare and round(comp_value, 2) == round(sweri_value, 2)
            elif isinstance(sweri_value, str) and isinstance(comp_value, str):
                total_compare = total_compare and comp_value.strip() == sweri_value.strip()
            else:
                total_compare = total_compare and comp_value == sweri_value

        except (TypeError, AttributeError) as e:
            print(f"Comparison error: {e}")
            total_compare = False


    if sweri_feature[-1] is not None:
        total_compare = total_compare and arcpy.AsShape(comparison_feature['geometry'], True).equals(sweri_feature[-1])

    return total_compare


def get_comparison_ids(cur, identifier_database, treatment_index, schema, sample_size):
    cur.execute(f'''
        SELECT unique_id
        FROM {schema}.{treatment_index}
        tablesample system (1)
	    WHERE identifier_database = '{identifier_database}'
        AND
        shape IS NOT NULL
        limit {sample_size};
    ''')

    id_rows = cur.fetchall()
    comparison_ids = [str(id_row[0]) for id_row in id_rows]
    return comparison_ids

def log_comparison_results(source_database, same, different):
    logging.info(f'{source_database} comparison complete')
    logging.info(f'same: {same}')
    logging.info(f'different: {different}')
    if different >= 1:
        logging.warning(f'{different} features from {source_database} did not match')
    else:
        logging.info(f'all {same} sweri {source_database} features matched source {source_database} features')


def prepare_feature_for_comparison(target_feature, date_field, wkid):
    if target_feature['attributes'][date_field] is not None:
        target_feature['attributes'][date_field] = datetime.datetime(1970, 1, 1) + datetime.timedelta(
            seconds=(target_feature['attributes'][date_field] / 1000))

    geometry = target_feature.get('geometry')
    if geometry:
        geometry['spatialReference'] = {'wkid': wkid}
    else:
        logging.debug(f"Missing geometry in feature: {target_feature}")

    return target_feature

def postgis_query_to_gdf(pg_query, pg_con, geom_field = 'shape'):
    gdf = geopandas.GeoDataFrame.from_postgis(pg_query, pg_con, geom_col=geom_field)

    if gdf.empty:
        return None

    # set max_objectid to the max or last record in the gdf for objectid column
    return gdf

def service_to_gdf(where_clause, service_fields, service_url, wkid):
    fields_str = ",".join(service_fields)

    params = {'f': 'geojson', 'outSR': 4326, 'outFields': fields_str, 'returnGeometry': 'true', 'where': where_clause}

    service_features = fetch_features(service_url + '/query', params)

    gdf = gpd.GeoDataFrame.from_features(service_features, crs="EPSG:4326")

    gdf = gdf.to_crs(epsg=wkid)

    return gdf

def return_sweri_pg_query(sweri_fields, schema, treatment_index, source_database, ids):
    sql_sweri_fields = ', '.join(f"{field}" for field in sweri_fields)
    id_list = ', '.join(f"'{i}'" for i in ids)

    sweri_pg_query = f"SELECT {sql_sweri_fields}, shape FROM {schema}.{treatment_index} WHERE identifier_database = '{source_database}' AND unique_id IN ({id_list})"

    return sweri_pg_query

def return_service_where_clause(source_database, ids):
    if source_database == 'FACTS Hazardous Fuels':
        id_list = ', '.join(f"'{i}'" for i in ids)
        service_where_clause = f"activity_cn in ({id_list})"

    elif source_database == 'FACTS Common Attributes':
        id_list = ', '.join(f"'{i}'" for i in ids)
        service_where_clause = f"event_cn in ({id_list})"

    elif source_database == 'NFPORS':
        service_where_clause = ' OR '.join(
            f"(nfporsfid = '{nfporsfid}' AND trt_id = '{trt_id}')"
            for id_value in ids
            for nfporsfid, trt_id in [id_value.split('-', 1)]
        )

    return service_where_clause

def compare_gdfs(service_gdf, sweri_gdf, comparison_field_map, id_map):
    same = 0
    different = 0
    service_id_field, sweri_id_field = id_map

    service_gdf.set_index(service_id_field)
    sweri_gdf.set_index(sweri_id_field)

    for index, sweri_row in sweri_gdf.iterrows():

        match = True
        if index in service_gdf.index:

            service_row = service_gdf.loc[index]
            for field_pair in comparison_field_map:

                service_field, sweri_field = field_pair
                service_value = service_row[service_field]
                sweri_value = sweri_row[sweri_field]

                # Need to solve different format datetime values here

                if service_value != sweri_value:
                    print('No Match')
                    match = False
                    print(service_value, sweri_value)
                else:
                    print('Match')
                    print(service_value, sweri_value)

        if match:
            same +=1
        else:
            different +=1

    print(same)
    print(different)



def hazardous_fuels_sample(treatment_index_fc, cursor, pg_con, treatment_index, schema, service_url):
    haz_fields = [ 'ACTIVITY_SUB_UNIT_NAME', 'DATE_COMPLETED', 'GIS_ACRES', 'TREATMENT_TYPE', 'CAT_NM',
                  'FUND_CODE', 'COST_PER_UOM', 'UOM', 'STATE_ABBR', 'ACTIVITY', 'ACTIVITY_CN']
    sweri_haz_fields = ['name', 'actual_completion_date', 'acres', 'type', 'category', 'fund_code',
                        'cost_per_uom', 'uom', 'state', 'activity', 'unique_id', 'SHAPE@']
    source_database = 'FACTS Hazardous Fuels'
    date_field = 'DATE_COMPLETED'

    feature_count = get_feature_count(cursor, schema, treatment_index, source_database)
    sample_size = get_sample_size(feature_count)

    ids = get_comparison_ids(cursor, source_database, treatment_index, schema, sample_size)

    if ids:
        id_list = ', '.join(f"'{i}'" for i in ids)
        sweri_pg_query = f"SELECT * FROM {schema}.{treatment_index} WHERE identifier_database = 'FACTS Hazardous Fuels' AND unique_id IN ({id_list})"
    else:
        logging.info('No ids returned for FACTS Hazardous Fuels comparison, moving to next process')
        return

    logging.info('Running Hazardous Fuels sample comparison')
    logging.info(f'size: {feature_count}, sample size: {sample_size}')
    compare_sweri_to_service(treatment_index_fc, sweri_haz_fields, pg_con, sweri_pg_query, haz_fields, service_url,
                             date_field, source_database)


def nfpors_sample(treatment_index_fc, cursor, pg_con, treatment_index, schema, service_url):
    nfpors_fields = ['trt_nm', 'act_comp_dt', 'gis_acres', 'type_name', 'cat_nm', 'st_abbr']
    sweri_nfpors_fields = ['name', 'actual_completion_date', 'acres', 'type', 'category', 'state', 'unique_id', 'SHAPE@']
    source_database = 'NFPORS'

    feature_count = get_feature_count(cursor, schema, treatment_index, source_database)
    sample_size = get_sample_size(feature_count)

    ids = get_comparison_ids(cursor, source_database, treatment_index, schema, sample_size)
    if ids:
        id_list = ', '.join(f"'{i}'" for i in ids)
        sweri_pg_query = f"SELECT {sweri_nfpors_fields} FROM {schema}.{treatment_index} WHERE identifier_database = 'NFPORS' AND unique_id IN ({id_list})"
    else:
        logging.info('No ids returned for NFPORS comparison, moving to next process')
        return

    date_field = 'act_comp_dt'

    # need to offset NFPORS by 1 to ignore the id field since we are splitting it apart
    iterator_offset = 1

    logging.info('Running NFPORS sample comparison')
    logging.info(f'size: {feature_count}, sample size: {sample_size}')
    compare_sweri_to_service(treatment_index_fc, sweri_nfpors_fields, pg_con, sweri_pg_query, nfpors_fields, service_url,
                             date_field, source_database, iterator_offset)
def compare_sweri_to_service(cursor, schema, treatment_index, comparison_field_map, pg_con, service_url, source_database, id_map, wkid=3857):

    feature_count = get_feature_count(cursor, schema, treatment_index, source_database)
    sample_size = get_sample_size(feature_count)

    ids = get_comparison_ids(cursor, source_database, treatment_index, schema, sample_size)

    service_fields, sweri_fields = zip(*comparison_field_map)

    service_fields = list(service_fields)
    sweri_fields = list(sweri_fields)


    if ids:
        sweri_pg_query = return_sweri_pg_query(sweri_fields, schema, treatment_index, source_database, ids)
        service_where_clause = return_service_where_clause(source_database, ids)

        logging.info(f'Running {source_database} sample comparison')
        logging.info(f'size: {feature_count}, sample size: {sample_size}')

    else:
        logging.info(f'No ids returned for {source_database} comparison, moving to next process')
        return

    sweri_gdf = postgis_query_to_gdf(sweri_pg_query, pg_con, geom_field='shape')
    service_gdf = service_to_gdf(service_where_clause, service_fields, service_url, wkid)

    compare_gdfs(service_gdf, sweri_gdf, comparison_field_map , id_map)
    same = 0
    different = 0
    features_equal = False

    for row in sweri_fc_cursor:

        if service_feature is None or len(service_feature) == 0:
            logging.warning(f'No feature returned for {row[-2]} in {source_database}')
            different += 1
            continue

        elif len(service_feature) > 1:
            logging.warning(
                f'more than one feature returned for {row[-2]} in {source_database}, skipping comparison')
            continue

        target_feature = service_feature[0]

        prepared_feature = prepare_feature_for_comparison(target_feature, date_field, wkid)

        features_equal = compare_features(prepared_feature, row, service_fields)

        if features_equal:
            same += 1
        else:
            field_equality, value_comparison = return_comparison(prepared_feature, row, source_database, iterator_offset)
            logging.warning(field_equality)
            logging.warning(value_comparison)
            different += 1

    log_comparison_results(source_database, same, different)

def common_attributes_sample(treatment_index_fc, cursor, pg_con, treatment_index, schema, service_url):
    comparison_field_map = [('NAME', 'name'), ('DATE_COMPLETED', 'actual_completion_date'), ('GIS_ACRES', 'acres'), ('NFPORS_TREATMENT', 'type'),
    ('NFPORS_CATEGORY', 'category'), ('STATE_ABBR', 'state'), ('FUND_CODES', 'fund_code'), ('COST_PER_UNIT', 'cost_per_uom'),
    ('UOM', 'uom'), ('ACTIVITY', 'activity'), ('EVENT_CN', 'unique_id')]
    id_map = ('EVENT_CN', 'unique_id')

    source_database = 'FACTS Common Attributes'

    compare_sweri_to_service(cursor, schema, treatment_index, comparison_field_map, pg_con, service_url, source_database, id_map)

if __name__ == '__main__':
    load_dotenv()

    cur, conn = connect_to_pg_db(os.getenv('DOCKER_DB_HOST'), os.getenv('DOCKER_DB_PORT'), os.getenv('DOCKER_DB_NAME'), os.getenv('DOCKER_DB_USER'),
                                 os.getenv('DOCKER_DB_PASSWORD'))

    sde_connection_file = os.getenv('SDE_FILE')
    target_schema = os.getenv('SCHEMA')
    treatment_index_table = 'treatment_index'
    hazardous_fuels_url = os.getenv('HAZARDOUS_FUELS_URL')
    nfpors_url = os.getenv('NFPORS_URL')
    common_attributes_url = os.getenv('COMMON_ATTRIBUTES_URL')
    treatment_index_sweri_fc = os.path.join(sde_connection_file, f"{target_schema}.{treatment_index_table}")

    logging.info('new run')
    logging.info('______________________________________')

    common_attributes_sample(treatment_index_sweri_fc, cur, conn, treatment_index_table, target_schema, common_attributes_url)
    hazardous_fuels_sample(treatment_index_sweri_fc, cur, conn, treatment_index_table, target_schema, hazardous_fuels_url)
    nfpors_sample(treatment_index_sweri_fc, cur, conn, treatment_index_table, target_schema, nfpors_url)

    conn.close()
