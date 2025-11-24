import os

from sweri_utils.common_attributes import common_attributes_type_filter
from sweri_utils.swizzle import swizzle_service

os.environ["CRYPTOGRAPHY_OPENSSL_NO_LEGACY"]="1"
from dotenv import load_dotenv
import re
from celery import group

from sweri_utils.sql import connect_to_pg_db, postgres_create_index, add_column, revert_multi_to_poly, makevalid_shapes, \
    extract_geometry_collections, remove_zero_area_polygons, remove_blank_strings, trim_whitespace
from sweri_utils.download import service_to_postgres, get_ids, prep_buffer_table, get_query_params_chunk, \
    swap_buffer_table
from sweri_utils.files import gdb_to_postgres, download_file_from_url, extract_and_remove_zip_file
from sweri_utils.error_flagging import flag_duplicates, flag_high_cost, flag_uom_outliers, flag_duplicate_ids, flag_spatial_errors
from sweri_utils.sweri_logging import logging, log_this
from sweri_utils.hosted import hosted_upload_and_swizzle, refresh_gis
from sweri_utils.tasks import service_chunk_to_postgres, common_attributes_processing

logger = logging.getLogger(__name__)

@log_this
def update_nfpors(nfpors_url, conn, schema, wkid, ogr_db_string, chunk_size=40):
    where = create_nfpors_where_clause()
    destination_table = 'nfpors'
    out_fields = ['*']

    prep_buffer_table(schema, destination_table)
    try:
        ids = get_ids(nfpors_url, where=where)
        t = []
        for params in get_query_params_chunk(ids, wkid, out_fields, chunk_size):
            t.append(service_chunk_to_postgres.s(nfpors_url, params, schema, destination_table, ogr_db_string))
        g = group(t)()
        g.get()

        swap_buffer_table(schema, destination_table)
    except Exception as e:
        logger.error(f'Error downloading NFPORS: {e}... continuing')
        pass

@log_this
def update_ifprs(conn, schema, wkid, service_url, ogr_db_string, chunk_size=70):
    where = '''
    (Class IN ('Actual Treatment','Estimated Treatment')) AND ((completiondate > DATE '1984-01-01 00:00:00')
    OR (completiondate IS NULL AND initiationdate > DATE '1984-01-01 00:00:00')
    OR (completiondate IS NULL AND initiationdate IS NULL AND createdondate > DATE '1984-01-01 00:00:00'))
'''

    destination_table = 'ifprs'
    out_fields = ['*']

    prep_buffer_table(schema, destination_table)
    try:
        ids = get_ids(service_url, where=where)
        t = []
        for params in get_query_params_chunk(ids, wkid, out_fields, chunk_size):
            t.append(service_chunk_to_postgres.s(service_url, params, schema, destination_table, ogr_db_string))
        g = group(t)()
        g.get()

        #add a check here before swap
        swap_buffer_table(schema, destination_table)
    except Exception as e:
        logger.error(f'Error downloading IFPRS: {e}... continuing')
        pass
    # service_to_postgres(service_url, where, wkid, ogr_db_string, schema, destination_table, conn, 100)

def create_nfpors_where_clause():
    #some ids break download, those will be excluded
    exclusion_ids = os.getenv('EXCLUSION_IDS')
    exlusion_ids_tuple = tuple(exclusion_ids.split(",")) if len(exclusion_ids) > 0 else tuple()

    where_clause = f'''
        (act_comp_dt > DATE '1984-01-01' 
        OR (act_comp_dt IS NULL AND plan_int_dt > DATE '1984-01-01') 
        OR (act_comp_dt IS NULL AND plan_int_dt IS NULL AND col_date > DATE '1984-01-01'))
    '''
    if len(exlusion_ids_tuple) > 0:
        where_clause += f' and objectid not in ({",".join(exlusion_ids_tuple)})'

    return where_clause

@log_this
def ifprs_insert(conn, schema, treatment_index):
    cursor = conn.cursor()
    with conn.transaction():
        cursor.execute(f'''
        INSERT INTO {schema}.{treatment_index} (
    
            objectid, 
            name,  date_current, 
            acres, type, category, fund_source,
            identifier_database, unique_id,
            state, status,
            total_cost, twig_category,
            agency, shape
        )
        SELECT
    
            sde.next_rowid('{schema}', '{treatment_index}'),
            name AS name, lastmodifieddate AS date_current,
            calculatedarea AS acres, type AS type, category AS category,
            fundingsourcecategory as fund_source, 'IFPRS' AS identifier_database, id AS unique_id,
            state AS state, 
            CASE WHEN class = 'Estimated Treatment' AND status IS NULL THEN 'Planned' ELSE status END AS status, 
            estimatedtotalcost as total_cost, category as twig_category, 
            agency as agency, shape as shape
    
        FROM {schema}.ifprs
        WHERE {schema}.ifprs.shape IS NOT NULL;
        ''')

@log_this
def ifprs_treatment_date(conn, schema, treatment_index):

    cursor = conn.cursor()
    with conn.transaction():
        cursor.execute(f'''
            UPDATE {schema}.{treatment_index} t
            SET treatment_date = i.completiondate
            FROM {schema}.ifprs i
            WHERE t.identifier_database = 'IFPRS'
            AND t.treatment_date is null
            AND t.unique_id = i.id::text
            AND i.completiondate IS NOT NULL;
        ''')
        cursor.execute(f'''
            UPDATE {schema}.{treatment_index} t
            SET treatment_date = i.originalinitiationdate
            FROM {schema}.ifprs i
            WHERE t.identifier_database = 'IFPRS'
            AND t.treatment_date is null
            AND t.unique_id = i.id::text
            AND i.originalinitiationdate IS NOT NULL;
        ''')

        cursor.execute(f'''
            UPDATE {schema}.{treatment_index} t
            SET treatment_date = i.createdondate
            FROM {schema}.ifprs i
            WHERE t.identifier_database = 'IFPRS'
            AND t.treatment_date is null
            AND t.unique_id = i.id::text
            AND i.createdondate IS NOT NULL;
        ''')

@log_this
def ifprs_status_consolidation(conn, schema, treatment_index):
    cursor = conn.cursor()
    with conn.transaction():
        cursor.execute(f'''
        
            UPDATE {schema}.{treatment_index}
            SET status =
                CASE 
                    WHEN status IN (
                        'Draft',
                        'Approved (Local)',
                        'Approved (Regional)',
                        'Ready for Approval',
                        'Approved (Department)',
                        'Approved (Agency)',
                        'Approved',
                        'Not Started'
                    ) THEN 'Planned'
                    WHEN status IN ('UnApproval Requested', 'Cancelled') THEN 'Other'
                    ELSE status
                END
            WHERE identifier_database = 'IFPRS';

                  ''')


def nfpors_insert(conn, schema, treatment_index):
    cursor = conn.cursor()
    with conn.transaction():
        cursor.execute(f'''
                       
        INSERT INTO {schema}.{treatment_index} (
    
            objectid, 
            name,  date_current,
            acres, type, category, fund_code, 
            identifier_database, unique_id,
            state, agency, shape
        )
        SELECT
    
            sde.next_rowid('{schema}', '{treatment_index}'),
            trt_nm AS name, modifiedon AS date_current,
            gis_acres AS acres, type_name AS type, cat_nm AS category, isbil as fund_code,
            'NFPORS' AS identifier_database, CONCAT(nfporsfid,'-',trt_id) AS unique_id,
            st_abbr AS state, agency as agency, shape
    
        FROM {schema}.nfpors
        WHERE {schema}.nfpors.shape IS NOT NULL;
        ''')

@log_this
def hazardous_fuels_insert(conn, schema, treatment_index, facts_haz_table):
    cursor = conn.cursor()
    with conn.transaction():
        cursor.execute(f'''
                   
        INSERT INTO {schema}.{treatment_index}(
    
            objectid, name, 
            date_current, acres,    
            type, category, fund_code, cost_per_uom,
            identifier_database, unique_id,
            uom, state, activity, activity_code, treatment_date,
            status, method, equipment, agency,
            shape
    
        )
        SELECT
    
            sde.next_rowid('{schema}', '{treatment_index}'), activity_sub_unit_name AS name,
            etl_modified_date_haz AS date_current, gis_acres AS acres,
            treatment_type AS type, cat_nm AS category, fund_code AS fund_code, cost_per_uom AS cost_per_uom,
            'FACTS Hazardous Fuels' AS identifier_database, activity_cn AS unique_id,
            uom AS uom, state_abbr AS state, activity AS activity, activity_code as activity_code, 
            CASE WHEN date_completed IS NULL THEN date_planned ELSE date_completed END AS treatment_date,
            CASE WHEN date_completed IS NULL THEN 'Planned' ELSE 'Completed' END AS status, 
            method AS method, equipment AS equipment,
            'USFS' AS agency, shape
            
        FROM {schema}.{facts_haz_table}
        WHERE {schema}.{facts_haz_table}.shape IS NOT NULL;
        
        ''')

def remove_wildfire_non_treatment(conn, schema, treatment_index):
    cursor = conn.cursor()
    with conn.transaction():
        cursor.execute(f'''             
            DELETE FROM {schema}.{treatment_index} 
            WHERE category = 'Wildfire Non-Treatment';
        ''')

@log_this
def hazardous_fuels_date_filtering(conn, schema, facts_haz_table):
    cursor = conn.cursor()
    with conn.transaction():
        cursor.execute(f'''             
            DELETE FROM {schema}.{facts_haz_table} WHERE
            date_completed < '1984-1-1'::date
            OR
            (date_completed is null AND date_planned < '1984-1-1'::date);
        ''')

@log_this
def nfpors_fund_code(conn, schema, treatment_index):
    cursor = conn.cursor()
    with conn.transaction():
        cursor.execute(f'''   
            UPDATE {schema}.{treatment_index}
            SET fund_code = null
            WHERE fund_code = 'No';        
        ''')

        cursor.execute(f'''   
            UPDATE {schema}.{treatment_index}
            SET fund_code = 'BIL'
            WHERE fund_code = 'Yes';
        ''')

@log_this
def nfpors_treatment_date_and_status(conn, schema,treatment_index):
    cursor = conn.cursor()
    with conn.transaction():
        cursor.execute(f'''
            UPDATE {schema}.{treatment_index} t
            SET treatment_date = n.act_comp_dt,
            status = 'Completed'
            FROM {schema}.nfpors n
            WHERE t.identifier_database = 'NFPORS'
            AND t.treatment_date IS NULL
            AND t.unique_id = CONCAT(n.nfporsfid,'-',n.trt_id)
            AND n.act_comp_dt IS NOT NULL;
        ''')
        cursor.execute(f'''
            UPDATE {schema}.{treatment_index} t
            SET treatment_date = n.plan_int_dt,
            status = 'Planned'
            FROM {schema}.nfpors n
            WHERE t.identifier_database = 'NFPORS'
            AND t.treatment_date is null
            AND t.unique_id = CONCAT(n.nfporsfid,'-',n.trt_id)
            AND n.plan_int_dt IS NOT NULL;
        ''')

        cursor.execute(f'''
            UPDATE {schema}.{treatment_index} t
            SET treatment_date = n.col_date,
            status = 'Other'
            FROM {schema}.nfpors n
            WHERE t.identifier_database = 'NFPORS'
            AND t.treatment_date is null
            AND t.unique_id = CONCAT(n.nfporsfid,'-',n.trt_id)
            AND n.col_date IS NOT NULL;
        ''')


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





def common_attributes_download_and_insert(projection, conn, ogr_db_string, schema, treatment_index, facts_haz_table):
    common_attributes_fc_name = 'Actv_CommonAttribute_PL'
    urls = [
    'https://data.fs.usda.gov/geodata/edw/edw_resources/fc/Actv_CommonAttribute_PL_Region01.zip',
    'https://data.fs.usda.gov/geodata/edw/edw_resources/fc/Actv_CommonAttribute_PL_Region02.zip',
    'https://data.fs.usda.gov/geodata/edw/edw_resources/fc/Actv_CommonAttribute_PL_Region03.zip',
    'https://data.fs.usda.gov/geodata/edw/edw_resources/fc/Actv_CommonAttribute_PL_Region04.zip',
    'https://data.fs.usda.gov/geodata/edw/edw_resources/fc/Actv_CommonAttribute_PL_Region05.zip',
    'https://data.fs.usda.gov/geodata/edw/edw_resources/fc/Actv_CommonAttribute_PL_Region06.zip',
    'https://data.fs.usda.gov/geodata/edw/edw_resources/fc/Actv_CommonAttribute_PL_Region08.zip',
    'https://data.fs.usda.gov/geodata/edw/edw_resources/fc/Actv_CommonAttribute_PL_Region09.zip',
    'https://data.fs.usda.gov/geodata/edw/edw_resources/fc/Actv_CommonAttribute_PL_Region10.zip'

    ]

    t = []
    for url in urls:
        t.append(common_attributes_processing.s(url, projection, common_attributes_fc_name, schema, ogr_db_string,
                                     facts_haz_table, treatment_index))
    g = group(t)()
    g.get()

    common_attributes_type_filter(conn, schema, treatment_index)


@log_this
def add_twig_category(conn, schema):
    common_attributes_twig_category(conn, schema)
    facts_nfpors_twig_category(conn, schema)

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

    # Truncate the table before inserting new data
    pg_cursor = pg_conn.cursor()
    with pg_conn.transaction():
        pg_cursor.execute(f'''TRUNCATE TABLE {target_schema}.{insert_table}''')
        pg_cursor.execute('COMMIT;')

    # FACTS Hazardous Fuels
    hazardous_fuels_zip_file = f'{hazardous_fuels_table}.zip'
    download_file_from_url(facts_haz_gdb_url, hazardous_fuels_zip_file)
    extract_and_remove_zip_file(hazardous_fuels_zip_file)

    # special input srs for common attributes
    # https://gis.stackexchange.com/questions/112198/proj4-postgis-transformations-between-wgs84-and-nad83-transformations-in-alask
    # without modifying the proj4 srs with the towgs84 values, the data is not in the "correct" location
    input_srs = '+proj=longlat +datum=NAD83 +no_defs +type=crs +towgs84=-0.9956,1.9013,0.5215,0.025915,0.009426,0.011599,-0.00062'
    gdb_to_postgres(facts_haz_gdb, out_wkid, facts_haz_fc_name, hazardous_fuels_table,
                    target_schema, ogr_db_string, input_srs)
    hazardous_fuels_date_filtering(pg_conn, target_schema, hazardous_fuels_table)
    hazardous_fuels_insert(pg_conn, target_schema, insert_table, hazardous_fuels_table)
    remove_wildfire_non_treatment(pg_conn, target_schema, insert_table)


    # FACTS Common Attributes
    common_attributes_download_and_insert(out_wkid, pg_conn, ogr_db_string, target_schema, insert_table, hazardous_fuels_table)

    # NFPORS
    update_nfpors(nfpors_url, pg_conn, target_schema, out_wkid, ogr_db_string)
    nfpors_insert(pg_conn, target_schema, insert_table)
    nfpors_fund_code(pg_conn, target_schema, insert_table)
    nfpors_treatment_date_and_status(pg_conn, target_schema, insert_table)

    # IFPRS processing and insert
    update_ifprs(pg_conn, target_schema, out_wkid, ifprs_url, ogr_db_string)
    ifprs_insert(pg_conn, target_schema, insert_table)
    ifprs_treatment_date(pg_conn, target_schema, insert_table)
    ifprs_status_consolidation(pg_conn, target_schema, insert_table)

    # Modify treatment index in place
    remove_blank_strings(pg_conn, target_schema, insert_table, fields_to_clean)
    trim_whitespace(pg_conn, target_schema, insert_table, 'agency')
    fund_source_updates(pg_conn, target_schema, insert_table)
    update_total_cost(pg_conn, target_schema, insert_table)
    correct_biomass_removal_typo(pg_conn, target_schema, insert_table)
    add_twig_category(pg_conn, target_schema)
    update_state_abbr(pg_conn, target_schema, insert_table)
    flag_duplicate_ids(pg_conn, target_schema, insert_table)
    flag_high_cost(pg_conn, target_schema, insert_table)
    flag_duplicates(pg_conn, target_schema, insert_table)
    flag_uom_outliers(pg_conn, target_schema, insert_table)
    revert_multi_to_poly(pg_conn, target_schema, insert_table)
    simplify_large_polygons(pg_conn, target_schema, insert_table, max_points_before_simplify, simplify_tolerance, fc_resolution)
    makevalid_shapes(pg_conn, target_schema, insert_table, 'shape', fc_resolution)
    extract_geometry_collections(pg_conn, target_schema, insert_table, fc_resolution)
    remove_zero_area_polygons(pg_conn, target_schema, insert_table)
    flag_spatial_errors(pg_conn, target_schema, insert_table)

    # update treatment points
    update_treatment_points(pg_conn, target_schema, insert_table)

    # # treatment index
    # ti_data_source = hosted_upload_and_swizzle(root_url, gis_url, gis_user, gis_password, treatment_index_view_id, treatment_index_data_ids, target_schema,
    #                            insert_table, max_points_before_simplify, chunk)
    #
    # if additional_polygon_view_ids:
    #     for view_id in additional_polygon_view_ids:
    #         swizzle_view(root_url, gis_url, gis_user, gis_password, view_id, ti_data_source)
    #
    #
    # # treatment index points
    # ti_points_data_source = hosted_upload_and_swizzle(root_url, gis_url, gis_user, gis_password, treatment_index_points_view_id, treatment_index_points_data_ids, target_schema,
    #                           points_table, max_points_before_simplify, chunk)
    #
    # if additional_point_view_ids:
    #     for view_id in additional_point_view_ids:
    #         swizzle_view(root_url, gis_url, gis_user, gis_password, view_id, ti_points_data_source)
    #
    #
    # pg_conn.close()
