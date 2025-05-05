import geopandas
from dotenv import load_dotenv
import os

os.environ["CRYPTOGRAPHY_OPENSSL_NO_LEGACY"] = "1"
import math
import logging
import geopandas as gpd
import pandas as pd
import numpy as np

from sweri_utils.sql import connect_to_pg_db
from sweri_utils.download import fetch_geojson_features, get_ids, get_all_features

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

    sweri_pg_query = f"SELECT shape, {sql_sweri_fields} FROM {schema}.{treatment_index} WHERE identifier_database = '{source_database}' AND unique_id IN ({id_list})"

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
    gdf = gdf.rename(columns={'shape': 'geometry'})
    gdf.set_geometry('geometry', inplace=True)
    gdf.set_crs(epsg=3857, inplace=True)
    gdf_reprojected = gdf.to_crs(epsg=4326)

    return gdf_reprojected


def service_to_gdf(where_clause, service_fields, service_url, wkid):

    service_features = fetch_geojson_features(service_url, where_clause, out_fields=service_fields, out_sr=4326, chunk_size=70)


    gdf = gpd.GeoDataFrame.from_features(service_features)


    gdf = gdf.set_crs(epsg=4326, inplace=True)

    return gdf


def prepare_gdfs_for_compare(service_gdf, sweri_gdf, service_date_field):
    # convert date from ms to datetime if needed
    if isinstance(service_gdf[service_date_field].dropna().iloc[0], (int, float, np.integer, np.floating)):
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
    service_id_field, sweri_id_field = id_map

    rename_map = {service_field: sweri_field for service_field, sweri_field in comparison_field_map}
    service_gdf = service_gdf.rename(columns=rename_map)

    # Set index to id fields after rename
    service_gdf = service_gdf.set_index(sweri_id_field)
    sweri_gdf = sweri_gdf.set_index(sweri_id_field)

    service_gdf = service_gdf.sort_index()
    sweri_gdf = sweri_gdf.sort_index()

    service_gdf_no_geom = service_gdf.drop(columns='geometry', errors='ignore')
    sweri_gdf_no_geom = sweri_gdf.drop(columns='geometry', errors='ignore')

    # Compare all but the geoms
    diff = service_gdf_no_geom.compare(sweri_gdf_no_geom, result_names=('service', 'sweri'))
    logger.info('Attribute Difference: ')
    logger.info(diff)

    # Compare the geoms
    geom_matches = service_gdf.geometry.geom_equals_exact(sweri_gdf.geometry, tolerance=10)
    geom_mismatch_indices = geom_matches[~geom_matches].index

    logger.info('Geom Mismatches:')
    logger.info(geom_mismatch_indices)




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
    logger.info('-' * 40)

    common_attributes_sample(cur, conn, treatment_index_table, target_schema,
                             common_attributes_url)
    nfpors_sample(cur, conn, treatment_index_table, target_schema, nfpors_url)
    hazardous_fuels_sample(cur, conn, treatment_index_table, target_schema,
                           hazardous_fuels_url)

    conn.close()
