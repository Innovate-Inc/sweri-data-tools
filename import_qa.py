import geopandas
from dotenv import load_dotenv
import os

os.environ["CRYPTOGRAPHY_OPENSSL_NO_LEGACY"] = "1"
import math
import logging
import geopandas as gpd
import pandas as pd

from sweri_utils.sql import connect_to_pg_db
from sweri_utils.download import fetch_features

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

file_handler = logging.FileHandler('./import_qa.log', encoding='utf-8')
file_handler.setFormatter(formatter)

if not logger.handlers:
    logger.addHandler(file_handler)

def flatten_list(source_list):
    flat_list = []

    for item in source_list:
        if isinstance(item, (list, tuple)):
            flat_list.extend(item)
        else:
            flat_list.append(item)
    return flat_list

def get_feature_count(cursor, schema, treatment_index, source_database):
    cursor.execute(f"SELECT count(*) FROM {schema}.{treatment_index} where identifier_database = '{source_database}';")
    feature_count = cur.fetchone()[0]
    return feature_count


def get_sample_size(population_size, proportion=.5, margin_of_error=.05):
    # Uses Cochran's formula to calculate sample size
    # z=1.96 sets confidence to 95%
    z = 1.96
    p = proportion
    e = margin_of_error
    n0 = ((z ** 2) * p * (1 - p)) / (e ** 2)
    n = n0 / (1 + ((n0 - 1) / population_size))

    return math.ceil(n)


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


def postgis_query_to_gdf(pg_query, pg_con, geom_field='shape'):
    gdf = geopandas.GeoDataFrame.from_postgis(pg_query, pg_con, geom_col=geom_field)

    if gdf.empty:
        return None

    return gdf


def service_to_gdf(where_clause, service_fields, service_url, wkid):
    fields_str = ",".join(service_fields)
    params = {'f': 'geojson', 'outSR': 4326, 'outFields': fields_str, 'returnGeometry': 'true', 'where': where_clause}

    service_features = fetch_features(service_url + '/query', params)

    gdf = gpd.GeoDataFrame.from_features(service_features, crs="EPSG:4326")
    gdf = gdf.to_crs(epsg=wkid)

    return gdf


def prepare_gdfs_for_compare(service_gdf, sweri_gdf, service_date_field):
    # convert date from ms to datetime if needed
    if isinstance(service_gdf[service_date_field].dropna().iloc[0], (int, float)):
        service_gdf.loc[:, service_date_field] = pd.to_datetime(service_gdf[service_date_field], unit='ms')

    # strip strings
    service_gdf = service_gdf.map(lambda x: x.strip() if isinstance(x, str) else x)
    sweri_gdf = sweri_gdf.map(lambda x: x.strip() if isinstance(x, str) else x)

    # Standardize float typing and rounding
    for col in service_gdf.select_dtypes(include=['float', 'float64']).columns:
        service_gdf[col] = service_gdf[col].round(3).astype('float64')

    for col in sweri_gdf.select_dtypes(include=['float', 'float64']).columns:
        sweri_gdf[col] = sweri_gdf[col].round(3).astype('float64')

    return service_gdf, sweri_gdf


def compare_gdfs(service_gdf, sweri_gdf, comparison_field_map, id_map):
    same = 0
    different = 0
    service_id_field, sweri_id_field = id_map

    # Set index to id fields
    service_gdf = service_gdf.set_index(service_id_field)
    sweri_gdf = sweri_gdf.set_index(sweri_id_field)

    # Remove ids from comparison map since they are indexes now
    fields_to_compare = [pair for pair in comparison_field_map if pair != id_map]

    for index, sweri_row in sweri_gdf.iterrows():

        if index not in service_gdf.index:
            logger.info(f'No feature returned for {index}')
            different += 1
            continue

        service_row = service_gdf.loc[index]

        # Create 2 series that have the same column names for direct .compare for each row
        service_vals = pd.Series(
            {sweri_field: service_row[service_field] for service_field, sweri_field in fields_to_compare})
        sweri_vals = sweri_row[[sweri_field for service_field, sweri_field in fields_to_compare]]

        diff = service_vals.compare(sweri_vals, result_names=('service', 'sweri'))

        # Add geom compare

        if diff.empty:
            same += 1
        else:
            different += 1
            logger.info(f"No Match for ID: {index}\n{diff.to_string(justify='left')}")
            logger.info('-'*40)

    logger.info(f'same: {same}')
    logger.info(f'different: {different}')


def return_sample_gdfs(cursor, schema, treatment_index, pg_con, service_url, source_database, comparison_field_map,
                       wkid=3857):
    feature_count = get_feature_count(cursor, schema, treatment_index, source_database)
    sample_size = get_sample_size(feature_count)

    ids = get_comparison_ids(cursor, source_database, treatment_index, schema, sample_size)

    service_fields, sweri_fields = zip(*comparison_field_map)

    service_fields = flatten_list(list(service_fields))
    sweri_fields = flatten_list(list(sweri_fields))

    if ids:
        service_where_clause = return_service_where_clause(source_database, ids)
        sweri_pg_query = return_sweri_pg_query(sweri_fields, schema, treatment_index, source_database, ids)

        logger.info(f'Running {source_database} sample comparison')
        logger.info(f'size: {feature_count}, sample size: {sample_size}')

    else:
        logger.info(f'No ids returned for {source_database} comparison, moving to next process')
        return None, None

    service_gdf = service_to_gdf(service_where_clause, service_fields, service_url, wkid)
    sweri_gdf = postgis_query_to_gdf(sweri_pg_query, pg_con, geom_field='shape')

    return service_gdf, sweri_gdf


def common_attributes_sample(cursor, pg_con, treatment_index, schema, service_url):
    source_database = 'FACTS Common Attributes'
    comparison_field_map = [
        ('NAME', 'name'),
        ('DATE_COMPLETED', 'actual_completion_date'),
        ('GIS_ACRES', 'acres'),
        ('NFPORS_TREATMENT', 'type'),
        ('NFPORS_CATEGORY', 'category'),
        ('STATE_ABBR', 'state'),
        ('FUND_CODES', 'fund_code'),
        ('COST_PER_UNIT', 'cost_per_uom'),
        ('UOM', 'uom'),
        ('ACTIVITY', 'activity'),
        ('EVENT_CN', 'unique_id')
    ]
    id_map = ('EVENT_CN', 'unique_id')
    service_date_field = 'DATE_COMPLETED'

    service_gdf, sweri_gdf = return_sample_gdfs(cursor, schema, treatment_index, pg_con, service_url, source_database,
                                                comparison_field_map)

    if service_gdf is not None and sweri_gdf is not None:
        service_gdf, sweri_gdf = prepare_gdfs_for_compare(service_gdf, sweri_gdf, service_date_field)
        compare_gdfs(service_gdf, sweri_gdf, comparison_field_map, id_map)


def hazardous_fuels_sample(cursor, pg_con, treatment_index, schema, service_url):
    source_database = 'FACTS Hazardous Fuels'
    comparison_field_map = [
        ('ACTIVITY_SUB_UNIT_NAME', 'name'),
        ('DATE_COMPLETED', 'actual_completion_date'),
        ('GIS_ACRES', 'acres'),
        ('TREATMENT_TYPE', 'type'),
        ('CAT_NM', 'category'),
        ('FUND_CODE', 'fund_code'),
        ('COST_PER_UOM', 'cost_per_uom'),
        ('UOM', 'uom'),
        ('STATE_ABBR', 'state'),
        ('ACTIVITY', 'activity'),
        ('ACTIVITY_CN', 'unique_id')
    ]
    id_map = ('ACTIVITY_CN', 'unique_id')
    service_date_field = 'DATE_COMPLETED'

    service_gdf, sweri_gdf = return_sample_gdfs(cursor, schema, treatment_index, pg_con, service_url, source_database,
                                                comparison_field_map)

    if service_gdf is not None and sweri_gdf is not None:
        service_gdf, sweri_gdf = prepare_gdfs_for_compare(service_gdf, sweri_gdf, service_date_field)
        compare_gdfs(service_gdf, sweri_gdf, comparison_field_map, id_map)

def nfpors_sample(cursor, pg_con, treatment_index, schema, service_url):
    source_database = 'NFPORS'
    comparison_field_map = [
        ('trt_nm', 'name'),
        ('act_comp_dt', 'actual_completion_date'),
        ('gis_acres', 'acres'),
        ('type_name', 'type'),
        ('cat_nm', 'category'),
        ('st_abbr', 'state'),
        (('nfporsfid', 'trt_id'), 'unique_id')
    ]
    id_map = (('nfporsfid', 'trt_id'), 'unique_id')
    service_date_field = 'act_comp_dt'

    service_gdf, sweri_gdf = return_sample_gdfs(cursor, schema, treatment_index, pg_con, service_url, source_database,
                                                comparison_field_map)

    if service_gdf is not None and sweri_gdf is not None:
        # Merging NFPORS columns to match sweri id merge before compare
        service_gdf['merged_id'] = service_gdf['nfporsfid'].astype(str) + '-' + service_gdf['trt_id'].astype(str)
        service_gdf = service_gdf.drop(columns=['nfporsfid', 'trt_id'])

        updated_comparison_field_map = [
            ('trt_nm', 'name'),
            ('act_comp_dt', 'actual_completion_date'),
            ('gis_acres', 'acres'),
            ('type_name', 'type'),
            ('cat_nm', 'category'),
            ('st_abbr', 'state'),
            ('merged_id', 'unique_id')
        ]
        updated_id_map =  ('merged_id', 'unique_id')

        service_gdf, sweri_gdf = prepare_gdfs_for_compare(service_gdf, sweri_gdf, service_date_field)
        compare_gdfs(service_gdf, sweri_gdf, updated_comparison_field_map, updated_id_map)


if __name__ == '__main__':
    load_dotenv()

    cur, conn = connect_to_pg_db(os.getenv('DOCKER_DB_HOST'), os.getenv('DOCKER_DB_PORT'), os.getenv('DOCKER_DB_NAME'),
                                 os.getenv('DOCKER_DB_USER'),
                                 os.getenv('DOCKER_DB_PASSWORD'))

    target_schema = os.getenv('SCHEMA')
    treatment_index_table = 'treatment_index'
    hazardous_fuels_url = os.getenv('HAZARDOUS_FUELS_URL')
    nfpors_url = os.getenv('NFPORS_URL')
    common_attributes_url = os.getenv('COMMON_ATTRIBUTES_URL')

    logger.info('new run')
    logger.info('______________________________________')

    nfpors_sample(cur, conn, treatment_index_table, target_schema, nfpors_url)

    common_attributes_sample(cur, conn, treatment_index_table, target_schema,
                             common_attributes_url)
    hazardous_fuels_sample(cur, conn, treatment_index_table, target_schema,
                           hazardous_fuels_url)

    conn.close()
