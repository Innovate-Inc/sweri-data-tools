import os
import shutil

from sweri_utils.s3 import upload_to_s3
from sweri_utils.swizzle import swizzle_service

os.environ["CRYPTOGRAPHY_OPENSSL_NO_LEGACY"]="1"
from dotenv import load_dotenv
from celery import group

from sweri_utils.sql import connect_to_pg_db, revert_multi_to_poly, makevalid_shapes, \
    extract_geometry_collections, remove_zero_area_polygons, remove_blank_strings, trim_whitespace
from sweri_utils.files import pg_table_to_gdb, create_zip
from sweri_utils.error_flagging import flag_duplicates, flag_high_cost, flag_uom_outliers, flag_duplicate_ids, flag_spatial_errors
from sweri_utils.sweri_logging import logging, log_this
from sweri_utils.hosted import refresh_gis, hosted_upload_and_swizzle
from treatment_index.tasks import ifprs_download_and_insert, common_attributes_download_and_insert, \
    hazardous_fuels_download_and_insert, nfpors_download_and_insert, state_data_download_and_insert

logger = logging.getLogger(__name__)

@log_this
def fund_source_updates(conn, schema, treatment_index):
    #IFPRS Processing is handled seperate since it does not have a fund code
    cursor = conn.cursor()
    with conn.transaction():
        cursor.execute(f'''
                UPDATE {schema}.{treatment_index}
                SET fund_source = 'Multiple'
                WHERE position(',' in fund_code) > 0;
        ''')

        cursor.execute(f'''
                UPDATE {schema}.{treatment_index}
                SET fund_source = 'No Funding Code'
                WHERE fund_code is null
                AND
                fund_source is null;
            ''')

        cursor.execute(f'''
                UPDATE {schema}.{treatment_index} ti
                SET fund_source = lt.fund_source
                FROM {schema}.fund_source_lookup lt
                WHERE ti.fund_code = lt.fund_code
                AND ti.fund_source IS null;
            ''')

        # Fund source consolidation from IFPRS fundsourcecategory
        # fund_source populated and overwritten since IFPRS has no fund_code
        cursor.execute(f'''
                UPDATE {schema}.{treatment_index} ti
                SET fund_source = lt.fund_source
                FROM {schema}.fund_source_lookup lt
                WHERE ti.fund_source = lt.fund_code
                AND ti.identifier_database = 'IFPRS';
            ''')
        # Set IFPRS entries that didn't get consolidated to 'Other'
        cursor.execute(f'''
                UPDATE {schema}.{treatment_index} ti
                SET fund_source = 'Other'
                WHERE ti.identifier_database = 'IFPRS'
                AND ti.fund_source NOT IN 
                (SELECT lt.fund_source
                FROM {schema}.fund_source_lookup lt);
            ''')

        cursor.execute(f'''
                UPDATE {schema}.{treatment_index}
                SET fund_source = 'Other'
                WHERE fund_source IS null
                AND
                fund_code IS NOT null;
            ''')

@log_this
def correct_biomass_removal_typo(conn, schema, treatment_index):
    cursor = conn.cursor()
    with conn.transaction():
        cursor.execute(f'''
            UPDATE {schema}.{treatment_index}
            SET type = 'Biomass Removal'
            WHERE 
            type = 'Biomass Removall'
        ''')

@log_this
def update_total_cost(conn, schema, treatment_index):
    cursor = conn.cursor()
    with conn.transaction():
        cursor.execute(f'''
            UPDATE {schema}.{treatment_index}
            SET total_cost = 
                CASE
                    WHEN uom = 'EACH' THEN cost_per_uom
                    WHEN uom = 'ACRES' THEN cost_per_uom * acres
                    WHEN uom = 'MILES' THEN cost_per_uom * (acres / 640)
                    ELSE total_cost
                END
        ''')

@log_this
def update_treatment_points(conn, schema, treatment_index):
    cursor = conn.cursor()
    with conn.transaction():
        cursor.execute(f'truncate table {schema}.{treatment_index}_points;')
        cursor.execute(
         f'''
            insert into {schema}.{treatment_index}_points (shape, objectid, unique_id, name, state, acres, treatment_date, 
            status, identifier_database, date_current, 
             activity_code, activity, method, equipment, category, type, twig_category, agency, 
            fund_source, fund_code, total_cost, cost_per_uom, uom, error)
            select ST_Centroid(shape), 
            sde.next_rowid('{schema}', '{treatment_index}_points'), 
            unique_id, name, state, acres, treatment_date, status, identifier_database, date_current, 
             activity_code, activity, method, equipment, category, type, twig_category, agency, 
            fund_source, fund_code, total_cost, cost_per_uom, uom, error
            from {schema}.{treatment_index}
        ''')

@log_this
def add_twig_category(conn, schema):
    common_attributes_twig_category(conn, schema)
    facts_nfpors_twig_category(conn, schema)
    state_data_twig_category(conn, schema)

@log_this
def common_attributes_twig_category(conn, schema):
    cursor = conn.cursor()
    with conn.transaction():
        cursor.execute(f'''
            UPDATE {schema}.treatment_index ti
            SET twig_category = tc.twig_category
            FROM
            {schema}.twig_category_lookup tc
            WHERE
            ti.identifier_database = 'FACTS Common Attributes'
            AND
            ti.activity = tc.activity
            AND
            ti.method = tc.method
            AND
            ti.equipment = tc.equipment;
        ''')

@log_this
def facts_nfpors_twig_category(conn, schema):
    cursor = conn.cursor()
    with conn.transaction():
        cursor.execute(f'''
            UPDATE {schema}.treatment_index ti
            SET twig_category = tc.twig_category
            FROM
            {schema}.twig_category_lookup tc
            WHERE(
                ti.identifier_database = 'NFPORS'
                OR
                ti.identifier_database = 'FACTS Hazardous Fuels'
                )     
            AND
            ti.type = tc.type;
        ''')

def state_data_twig_category(conn, schema):
    cursor = conn.cursor()
    with conn.transaction():
        cursor.execute(f'''
            UPDATE {schema}.treatment_index ti
            SET twig_category = tc.twig_category
            FROM
            {schema}.twig_category_lookup tc
            WHERE ti.identifier_database = 'NASF'
            AND
            ti.category = tc.category;
        ''')

@log_this
def update_state_abbr(conn, schema, treatment_index):
    cursor = conn.cursor()
    with conn.transaction():
        cursor.execute(f'''
        UPDATE {schema}.{treatment_index} ti
        SET state = s.stusps
        FROM {schema}.states s
        WHERE ti.state = s.name;
        ''')

@log_this
def simplify_large_polygons(conn, schema, table, points_cutoff, tolerance, resolution=0.000000001):
    """
    Ensures OGC-compliant geometries meet Esri geometry specifications by simplifying
    and validating overly complex shapes. The purpose of this function is twofold:

    1. To reduce vertex count and ensure successful upload to a hosted feature class
    2. To ensure the output geometry is ESRI compliant, and does not fail ESRI repair geometry alogrithm

    Highly complex OGC geometries (specifically those exceeding a defined vertex threshold)
    can often fail to convert or upload successfully to Esri environments. This function
    targets these oversized geometries and applies a sequence of PostGIS operations to
    reduce their vertex count and enforce Esri-compliant topology and resolution.

    ST_SimplifyPreserveTopology : Simplifies shapes while preserving topology (shells and holes)
    ST_SnapToGrid : Emulates ESRI feature class resolution(use 0 resolution to disable)
    ST_MakeValid : Fixes invalid polygons and multipolygons, method structure
    ST_UnaryUnion : Unions overlapping geometries into single shapes

    :param conn: postgres connection object
    :param schema: postgres schema name
    :param table: postgres table name
    :param points_cutoff: number of points above which shapes will be simplified
    :param tolerance: simplification tolerance distance
    :param resolution: grid snapping resolution (default 1e-9)
    """

    cursor = conn.cursor()
    with conn.transaction():
        cursor.execute(f'''
        
            UPDATE {schema}.{table}
			set shape = 
                    ST_UnaryUnion(      -- combines overlapping or touching geometries into single shapes
                      ST_MakeValid(     -- ensures shape validity for successful union
                        ST_SnapToGrid(  -- snaps to grid to emulate ESRI resolutoin
                          ST_SimplifyPreserveTopology(shape, {tolerance}), -- simplify, but preserve topology (holes, boundaries)
                          {resolution}  -- set to resolution of feature class
                        ), 'method=structure' -- stucture makevalid prevents overlaps from being interpreted as holes
                      )
                    ),
              error = CASE
                        WHEN error IS NULL THEN 'MODIFIED_SHAPE'
                        ELSE error || ';MODIFIED_SHAPE'
                      END
            WHERE ST_NPoints(shape) > {points_cutoff}; -- all shapes with more than points_cutoff points will be simplified
        ''')

@log_this
def swizzle_view(esri_root_url, esri_gis_url, esri_gis_user, esri_gis_password, esri_view_id, esri_ti_points_data_source):
    gis_con = refresh_gis(esri_gis_url, esri_gis_user, esri_gis_password)
    token = gis_con.session.auth.token
    swizzle_service(esri_root_url, gis_con.content.get(esri_view_id).name, esri_ti_points_data_source, token)

@log_this
def s3_gdb_update(ogr_db_conn_string, schema, table, bucket, obj_name, fc_name, wkid, query=None, work_dir=None, geom_col='shape'):
    gdb_path = pg_table_to_gdb(ogr_db_conn_string, schema, table, fc_name, wkid)
    zip_path = create_zip(gdb_path, table, out_dir=work_dir)
    upload_to_s3(bucket, zip_path, obj_name)

    if gdb_path and os.path.exists(gdb_path):
        shutil.rmtree(gdb_path)
    if zip_path and os.path.exists(zip_path):
        os.remove(zip_path)

def run_treatment_index(conn, schema, table, ogr_db_conn_string, wkid, facts_haz_fuels_gdb_url, nfpors_service_url,
                        ifprs_service_url, state_data_url, gis_root_url, api_gis_url, api_gis_user, api_gis_password, ti_view_id,
                        ti_data_ids, additional_poly_view_ids, ti_points_view_id, ti_points_data_ids,
                        additional_point_views_ids,bucket, s3_obj_name, ti_points_table='treatment_index_points',
                        facts_haz_fuels_fc_name='Actv_HazFuelTrt_PL', haz_fuels_table='facts_hazardous_fuels',
                        facts_haz_gdb_path='Actv_HazFuelTrt_PL.gdb', fields_for_cleanup=['type', 'fund_source'],
                        max_poly_size_before_simplify=10000, simplify_tol=0.000009, fc_res=0.000000001, chunk_size=500):

    # Truncate the table before inserting new data
    pg_cursor = conn.cursor()
    with conn.transaction():
        pg_cursor.execute(f'''TRUNCATE TABLE {schema}.{table}''')
        pg_cursor.execute('COMMIT;')

    # FACTS Common Attributes
    t = []
    t.append(hazardous_fuels_download_and_insert.s(haz_fuels_table, facts_haz_fuels_gdb_url, facts_haz_gdb_path, wkid, facts_haz_fuels_fc_name, schema, table, ogr_db_conn_string))
    t.append(common_attributes_download_and_insert.s(wkid, ogr_db_conn_string, schema, table, haz_fuels_table))
    t.append(nfpors_download_and_insert.s(schema, table))
    t.append(ifprs_download_and_insert.s(schema, table, wkid, ifprs_service_url, ogr_db_conn_string))
    t.append(state_data_download_and_insert.s(state_data_url, wkid, schema, table, ogr_db_conn_string))
    g = group(t)()
    g.get()

    # Modify treatment index in place
    remove_blank_strings(conn, schema, table, fields_for_cleanup)
    trim_whitespace(conn, schema, table, 'agency')
    fund_source_updates(conn, schema, table)
    update_total_cost(conn, schema, table)
    correct_biomass_removal_typo(conn, schema, table)
    add_twig_category(conn, schema)
    update_state_abbr(conn, schema, table)
    flag_duplicate_ids(conn, schema, table)
    flag_high_cost(conn, schema, table)
    flag_duplicates(conn, schema, table)
    flag_uom_outliers(conn, schema, table)
    revert_multi_to_poly(conn, schema, table)
    simplify_large_polygons(conn, schema, table, max_poly_size_before_simplify, simplify_tol, fc_res)
    makevalid_shapes(conn, schema, table, 'shape', fc_res)
    extract_geometry_collections(conn, schema, table, fc_res)
    remove_zero_area_polygons(conn, schema, table)
    flag_spatial_errors(conn, schema, table)

    # update treatment points
    update_treatment_points(conn, schema, table)
    # treatment index
    treatment_index_data_source = hosted_upload_and_swizzle(gis_root_url, api_gis_url, api_gis_user, api_gis_password, ti_view_id,
                                               ti_data_ids, schema,
                                               table, max_poly_size_before_simplify, chunk_size)

    if additional_poly_view_ids:
        for polygon_view_id in additional_poly_view_ids:
            swizzle_view(gis_root_url, api_gis_url, api_gis_user, api_gis_password, polygon_view_id, treatment_index_data_source)

    # treatment index points
    treatment_index_points_data_source = hosted_upload_and_swizzle(gis_root_url, api_gis_url, api_gis_user, api_gis_password,
                                                      ti_points_view_id, ti_points_data_ids,
                                                      schema,
                                                      ti_points_table, max_poly_size_before_simplify, chunk_size)

    if additional_point_views_ids:
        for point_view_id in additional_point_views_ids:
            swizzle_view(gis_root_url, api_gis_url, api_gis_user, api_gis_password, point_view_id, treatment_index_points_data_source)

    s3_gdb_update(ogr_db_conn_string, schema, table, bucket, s3_obj_name, fc_name=table, wkid=wkid)

    conn.close()

if __name__ == "__main__":
    load_dotenv()

    out_wkid = 4326

    target_schema = os.getenv('SCHEMA')
    exluded_ids = os.getenv('EXCLUSION_IDS')
    facts_haz_gdb_url = os.getenv('FACTS_GDB_URL')
    ifprs_url = os.getenv('IFPRS_URL')
    facts_haz_gdb = 'Actv_HazFuelTrt_PL.gdb'
    facts_haz_fc_name = 'Actv_HazFuelTrt_PL'
    hazardous_fuels_table = 'facts_hazardous_fuels'
    nfpors_url = os.getenv('NFPORS_URL')
    state_data_url = os.getenv('STATE_DATA_URL')

    #This is the final table
    insert_table = 'treatment_index'
    points_table = 'treatment_index_points'
    fields_to_clean = ['type', 'fund_source']

    pg_conn = connect_to_pg_db(os.getenv('DB_HOST'), os.getenv('DB_PORT'), os.getenv('DB_NAME'),
                                 os.getenv('DB_USER'), os.getenv('DB_PASSWORD'))

    ogr_db_string = f"PG:dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')} port={os.getenv('DB_PORT')} host={os.getenv('DB_HOST')}"

    # Hosted upload variables
    root_url = os.getenv('ESRI_ROOT_URL')
    gis_url = os.getenv("ESRI_PORTAL_URL")
    gis_user = os.getenv("ESRI_USER")
    gis_password = os.getenv("ESRI_PW")

    treatment_index_view_id = os.getenv('TREATMENT_INDEX_VIEW_ID')
    treatment_index_data_ids = [os.getenv('TREATMENT_INDEX_DATA_ID_1'), os.getenv('TREATMENT_INDEX_DATA_ID_2')]
    additional_polygon_view_ids = [os.getenv('TREATMENT_INDEX_AGENCY_VIEW_ID'), os.getenv('TREATMENT_INDEX_CATEGORY_VIEW_ID')]

    treatment_index_points_view_id = os.getenv('TREATMENT_INDEX_POINTS_VIEW_ID')
    additional_point_view_ids = [os.getenv('TREATMENT_INDEX_AGENCY_POINTS_VIEW_ID'),os.getenv('TREATMENT_INDEX_CATEGORY_POINTS_VIEW_ID')]
    treatment_index_points_data_ids = [os.getenv('TREATMENT_INDEX_POINTS_DATA_ID_1'), os.getenv('TREATMENT_INDEX_POINTS_DATA_ID_2')]

    treatment_index_points_table = 'treatment_index_points'

    chunk = 500
    max_points_before_simplify = 10000
    simplify_tolerance = 0.000009  # ESPG:4326 degrees
    fc_resolution = 0.000000001 # ESPG:4326 degrees
    start_objectid = 0

    s3_bucket = os.getenv('S3_BUCKET')
    s3_obj_name = os.getenv('S3_OBJECT_NAME')

    run_treatment_index(pg_conn, target_schema, insert_table, ogr_db_string, out_wkid, facts_haz_gdb_url, nfpors_url,
                        ifprs_url, state_data_url,  root_url, gis_url, gis_user, gis_password, treatment_index_view_id,
                        treatment_index_data_ids, additional_polygon_view_ids, treatment_index_points_view_id,
                        treatment_index_points_data_ids, additional_point_view_ids, s3_bucket, s3_obj_name)