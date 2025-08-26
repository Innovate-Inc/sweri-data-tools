import os

os.environ["CRYPTOGRAPHY_OPENSSL_NO_LEGACY"]="1"
from dotenv import load_dotenv
import re

from sweri_utils.sql import connect_to_pg_db, postgres_create_index, add_column, revert_multi_to_poly, makevalid_shapes
from sweri_utils.download import service_to_postgres, get_ids
from sweri_utils.files import gdb_to_postgres, download_file_from_url, extract_and_remove_zip_file
from error_flagging import flag_duplicates, flag_high_cost, flag_uom_outliers, flag_duplicate_ids
from sweri_utils.sweri_logging import logging, log_this
from sweri_utils.hosted import hosted_upload_and_swizzle

logger = logging.getLogger(__name__)

def update_nfpors(nfpors_url, conn, schema, wkid, ogr_db_string):
    where = create_nfpors_where_clause()
    destination_table = 'nfpors'
    # database = 'sweri'

    try:
        service_to_postgres(nfpors_url, where, wkid, ogr_db_string, schema, destination_table, conn, 40)
    except Exception as e:
        logger.error(f'Error downloading NFPORS: {e}... continuing')
        pass

@log_this
def update_ifprs(conn, schema, wkid, service_url, ogr_db_string):
    where = '''
    (Class IN ('Actual Treatment','Estimated Treatment')) AND ((completiondate > DATE '1984-01-01 00:00:00')
    OR (completiondate IS NULL AND initiationdate > DATE '1984-01-01 00:00:00')
    OR (completiondate IS NULL AND initiationdate IS NULL AND createdondate > DATE '1984-01-01 00:00:00'))
'''

    destination_table = 'ifprs'

    service_to_postgres(service_url, where, wkid, ogr_db_string, schema, destination_table, conn, 250)

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
            acres, type, category, fund_code, fund_source,
            identifier_database, unique_id,
            state, status,
            total_cost, twig_category,
            agency, shape
        )
        SELECT
    
            sde.next_rowid('{schema}', '{treatment_index}'),
            name AS name, lastmodifieddate AS date_current,
            calculatedarea AS acres, type AS type, category AS category, fundingsourcecategory as fund_code,
            fundingsource as fund_source, 'IFPRS' AS identifier_database, id AS unique_id,
            state AS state, status as status,
            estimatedtotalcost as total_cost, category as twig_category, 
            agency as agency, shape as shape
    
        FROM {schema}.ifprs
        WHERE {schema}.ifprs.shape IS NOT NULL;
        ''')

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
    cursor = conn.cursor()
    with conn.transaction():
        cursor.execute(f'''UPDATE {schema}.{treatment_index}
                    SET fund_source = 'Multiple'
                    WHERE position(',' in fund_code) > 0''')

        cursor.execute(f'''
                UPDATE {schema}.{treatment_index}
                SET fund_source = 'No Funding Code'
                WHERE fund_code is null
            ''')

        cursor.execute(f'''UPDATE {schema}.{treatment_index} ti
                SET fund_source = lt.fund_source
                FROM {schema}.fund_source_lookup lt
                WHERE ti.fund_code = lt.fund_code
                AND ti.fund_source IS null
            ''')
        cursor.execute(f'''
                UPDATE {schema}.{treatment_index}
                SET fund_source = 'Other'
                WHERE fund_source IS null
                AND
                fund_code IS NOT null
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
             activity_code, activity, method, equipment, category, type, agency, 
            fund_source, fund_code, total_cost, cost_per_uom, uom, error)
            select ST_Centroid(shape), 
            sde.next_rowid('{schema}', '{treatment_index}_points'), 
            unique_id, name, state, acres, treatment_date, status, identifier_database, date_current, 
             activity_code, activity, method, equipment, category, type, agency, 
            fund_source, fund_code, total_cost, cost_per_uom, uom, error
            from {schema}.{treatment_index}
        ''')




# BEGIN Common Attributes Functions

def add_fields_and_indexes(conn, schema, feature_class, region):
    #adds fields that will contain the pass/fail for each rule, as well as an overall included to be populated later

    new_fields = ('included', 'r2', 'r3', 'r4','r5', 'r6')
    for field in new_fields:
        add_column(conn, schema, feature_class, field, 'TEXT')

@log_this
def common_attributes_date_filtering(conn, schema, table_name):
    # Excludes treatment entries before 1984
    # uses date_completed if available, and act_created_date if date_completed is null
    cursor = conn.cursor()
    with conn.transaction():
        cursor.execute(f'''DELETE from {schema}.{table_name} WHERE
        date_completed < '1984-1-1'::date
        OR
        (date_completed is null AND act_created_date < '1984-1-1'::date);''')


@log_this
def exclude_facts_hazardous_fuels(conn, schema, table, facts_haz_table):
    # Excludes FACTS Common Attributes records already being included via FACTS Hazardous Fuels
    cursor = conn.cursor()
    with conn.transaction():
        cursor.execute(f'''            
            DELETE FROM {schema}.{table} USING {schema}.{facts_haz_table}
            WHERE event_cn = activity_cn
        ''')

@log_this
def exclude_by_acreage(conn, schema, table):
    #removes all treatments with null acerage or <= 5 acres
    cursor = conn.cursor()
    with conn.transaction():
        cursor.execute(f'''
        DELETE FROM {schema}.{table}
        WHERE
        gis_acres <= 5 OR
        gis_acres IS NULL;
        ''')

@log_this
def trim_whitespace(conn, schema, table):
    #Some entries have spaces before or after that interfere with matching, this trims those spaces out
    cursor = conn.cursor()
    with conn.transaction():
        cursor.execute(f'''              
            UPDATE {schema}.{table}
            SET
            activity = TRIM(activity),
            method = TRIM(method),
            equipment = TRIM(equipment);
        ''')

@log_this
def include_logging_activities(conn, schema, table):
    # Make sure the activity includes 'thin' or 'cut'
    # Checks the lookup table for method and equipment slated for inclusion

    cursor = conn.cursor()
    with conn.transaction():
        cursor.execute(f'''
                       
        UPDATE {schema}.{table}
        SET r2 = 'PASS'
        WHERE
        (activity ILIKE '%thin%'
        OR
        activity ILIKE '%cut%')
        AND
        (method IN (
            SELECT value from {schema}.common_attributes_lookup
            WHERE activity = 'logging' 
            AND filter = 'method'
            AND include = 'TRUE') 
        OR method IS NULL)
        AND
        (equipment IN (
            SELECT value FROM {schema}.common_attributes_lookup 
            WHERE activity = 'logging' 
            AND filter = 'equipment'
            AND include = 'TRUE')
        OR equipment IS NULL);
        
        ''')

@log_this
def include_fire_activites(conn, schema, table):
    # Make sure the activity includes 'burn' or 'fire'
    # Checks the lookup table for method and equipment slated for inclusion

    cursor = conn.cursor()
    with conn.transaction():
        cursor.execute(f'''
                       
        UPDATE {schema}.{table}
        SET r3 = 'PASS'
        WHERE
        (activity ILIKE '%burn%'
        OR
        activity ILIKE '%fire%')
        AND
        (method IN (
            SELECT value from {schema}.common_attributes_lookup
            WHERE activity = 'fire' 
            AND filter = 'method'
            AND include = 'TRUE') 
        OR method IS NULL)
        AND
        (equipment IN (
            SELECT value FROM {schema}.common_attributes_lookup 
            WHERE activity = 'fire' 
            AND filter = 'equipment'
            AND include = 'TRUE')
        OR equipment IS NULL);
    
        
        ''')

@log_this
def include_fuel_activities(conn, schema, table):
    # Make sure the activity includes 'fuel'
    # Checks the lookup table for method and equipment slated for inclusion

    cursor = conn.cursor()
    with conn.transaction():
        cursor.execute(f'''
                       
        UPDATE {schema}.{table}
        SET r4 = 'PASS'
        WHERE
        activity ILIKE '%fuel%'
        AND
        (method IN (
            SELECT value from {schema}.common_attributes_lookup
            WHERE activity = 'fuel' 
            AND filter = 'method'
            AND include = 'TRUE') 
        OR method IS NULL)
        AND
        (equipment IN (
            SELECT value FROM {schema}.common_attributes_lookup 
            WHERE activity = 'fuel' 
            AND filter = 'equipment'
            AND include = 'TRUE')
        OR equipment IS NULL);
    
        
        ''')

@log_this
def activity_filter(conn, schema, table):
    # Filters based on activity to ensure only intended activities enter the database
    cursor = conn.cursor()
    with conn.transaction():
        cursor.execute(f'''
                       
        UPDATE {schema}.{table}
        SET r6 = 'PASS'
        WHERE
        (r2 = 'PASS'
        OR 
        r3 = 'PASS'
        OR
        r4 = 'PASS')
        AND
        activity IN (
            SELECT value 
            FROM {schema}.common_attributes_lookup
            WHERE filter = 'special_exclusions'
            AND include = 'TRUE'
        );
        
        ''')


def include_other_activites(conn, schema, table):
    #lookup based inclusion not dependent on other rules

    cursor = conn.cursor()
    with conn.transaction():
        cursor.execute(f'''
                       
        UPDATE {schema}.{table}
        SET r5 = 'PASS'
        WHERE
        activity NOT ILIKE '%thin%'
        AND
        activity NOT ILIKE '%cut%'
        AND
        activity NOT ILIKE '%burn%'
        AND
        activity NOT ILIKE '%fire%'
        AND
        method IS NOT NULL
        AND
        equipment IS NOT NULL
        AND 
        method != 'No method'
        AND
        equipment != 'No equipment'
        AND
        method IN (
            SELECT value from {schema}.common_attributes_lookup
            WHERE activity = 'other' 
            AND filter = 'method'
            AND include = 'TRUE')
        AND
        equipment IN (
            SELECT value FROM {schema}.common_attributes_lookup 
            WHERE activity = 'other' 
            AND filter = 'equipment'
            AND include = 'TRUE');
    
        
        ''')

@log_this
def common_attributes_type_filter(conn, schema, treatment_index):
    cursor = conn.cursor()
    with conn.transaction():
        cursor.execute(f'''           
            DELETE from {schema}.{treatment_index} 
            WHERE
            type IN (
                SELECT value from {schema}.common_attributes_lookup
                WHERE filter = 'type'
                AND include = 'FALSE')
            AND
            identifier_database = 'FACTS Common Attributes';
        ''')

@log_this
def set_included(conn, schema, table):
    # set included to yes when r5 passes or
    # r2, r3, or, r4 passes and r6 passes
    cursor = conn.cursor()
    with conn.transaction():

        cursor.execute(f'''
                       
        UPDATE {schema}.{table}
        SET included = 'yes'
        WHERE
        included is null
        AND
        r5 = 'PASS'
        OR
        ((r2 = 'PASS' OR r3 = 'PASS' OR r4 = 'PASS') AND r6 = 'PASS');
        
        ''')

@log_this
def common_attributes_insert(conn, schema, common_attributes_table, treatment_index_table):
    # insert records where included = 'yes'
    cursor = conn.cursor()
    with conn.transaction():
        cursor.execute(f'''
                       
        INSERT INTO {schema}.{treatment_index_table}(
    
            objectid, name, date_current,  acres, 
            type, category, fund_code, cost_per_uom, identifier_database, 
            unique_id, uom, state, activity, activity_code, treatment_date, status, 
            method, equipment, agency, shape
    
        )
        SELECT
    
            sde.next_rowid('{schema}', '{treatment_index_table}'),
            name AS name, act_modified_date AS date_current, gis_acres AS acres, 
            nfpors_treatment AS type, nfpors_category AS category, fund_codes as fund_code, 
            cost_per_unit as cost_per_uom, 'FACTS Common Attributes' AS identifier_database, 
            event_cn AS unique_id, uom as uom, state_abbr AS state, activity as activity,
            activity_code as activity_code, CASE WHEN date_completed IS NULL THEN act_created_date ELSE date_completed END AS treatment_date,
            CASE WHEN date_completed IS NULL THEN 'Planned' ELSE 'Completed' END AS status, method as method, equipment as equipment, 
            'USFS' as agency, shape as shape
            
        FROM {schema}.{common_attributes_table}
        WHERE included = 'yes'
        AND
        {schema}.{common_attributes_table}.shape IS NOT NULL;

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


    for url in urls:

        #expression pulls just the number out of the url, 01-10
        region_number = re.sub("\D", "", url)
        ca_table_name = f'common_attributes_{region_number}'
        gdb = f'Actv_CommonAttribute_PL_Region{region_number}.gdb'

        zip_file = f'{ca_table_name}.zip'

        logging.info(f'Downloading {url}')
        download_file_from_url(url, zip_file)

        logging.info(f'Extracting {zip_file}')
        extract_and_remove_zip_file(zip_file)

        # special input srs for common attributes
        # https://gis.stackexchange.com/questions/112198/proj4-postgis-transformations-between-wgs84-and-nad83-transformations-in-alask
        # without modifying the proj4 srs with the towgs84 values, the data is not in the "correct" location

        input_srs = '+proj=longlat +datum=NAD83 +no_defs +type=crs +towgs84=-0.9956,1.9013,0.5215,0.025915,0.009426,0.011599,-0.00062'
        gdb_to_postgres(gdb, projection, common_attributes_fc_name, ca_table_name, schema, ogr_db_string, input_srs)

        add_fields_and_indexes(conn, schema, ca_table_name, region_number)

        common_attributes_date_filtering(conn, schema, ca_table_name)
        exclude_by_acreage(conn, schema, ca_table_name)
        exclude_facts_hazardous_fuels(conn, schema, ca_table_name, facts_haz_table)

        trim_whitespace(conn, schema, ca_table_name)

        include_logging_activities(conn, schema, ca_table_name)
        include_fire_activites(conn, schema, ca_table_name)
        include_fuel_activities(conn, schema, ca_table_name)
        activity_filter(conn, schema, ca_table_name)
        include_other_activites(conn, schema, ca_table_name)

        set_included(conn, schema, ca_table_name)

        common_attributes_insert(conn, schema, ca_table_name, treatment_index)
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

def remove_zero_area_polygons(conn, schema, table):
    cursor = conn.cursor()
    with conn.transaction():
        cursor.execute(f'''
        
            DELETE FROM {schema}.{table}
            WHERE ST_Area(shape) = 0;
            
        ''')

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

    pg_conn = connect_to_pg_db(os.getenv('DB_HOST'), os.getenv('DB_PORT'), os.getenv('DB_NAME'),
                                 os.getenv('DB_USER'), os.getenv('DB_PASSWORD'))

    ogr_db_string = f"PG:dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')} port={os.getenv('DB_PORT')} host={os.getenv('DB_HOST')}"

    # Hosted upload variables
    gis_url = os.getenv("ESRI_PORTAL_URL")
    gis_user = os.getenv("ESRI_USER")
    gis_password = os.getenv("ESRI_PW")

    treatment_index_view_id = os.getenv('TREATMENT_INDEX_VIEW_ID')
    treatment_index_data_ids = [os.getenv('TREATMENT_INDEX_DATA_ID_1'), os.getenv('TREATMENT_INDEX_DATA_ID_2')]

    treatment_index_points_view_id = os.getenv('TREATMENT_INDEX_POINTS_VIEW_ID')
    treatment_index_points_data_ids = [os.getenv('TREATMENT_INDEX_POINTS_DATA_ID_1'), os.getenv('TREATMENT_INDEX_POINTS_DATA_ID_2')]
    treatment_index_points_table = 'treatment_index_points'

    chunk = 1000
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
    fund_source_updates(pg_conn, target_schema, insert_table)
    update_total_cost(pg_conn, target_schema, insert_table)
    correct_biomass_removal_typo(pg_conn, target_schema, insert_table)
    flag_duplicate_ids(pg_conn, target_schema, insert_table)
    flag_high_cost(pg_conn, target_schema, insert_table)
    flag_duplicates(pg_conn, target_schema, insert_table)
    flag_uom_outliers(pg_conn, target_schema, insert_table)
    add_twig_category(pg_conn, target_schema)
    revert_multi_to_poly(pg_conn, target_schema, insert_table)
    makevalid_shapes(pg_conn, target_schema, insert_table, 'shape')
    remove_zero_area_polygons(pg_conn, target_schema, insert_table)

    # update treatment points
    update_treatment_points(pg_conn, target_schema, insert_table)

    # treatment index
    hosted_upload_and_swizzle(gis_url, gis_user, gis_password, treatment_index_view_id, treatment_index_data_ids, pg_conn, target_schema,
                              insert_table, chunk, start_objectid)
    # treatment index points
    hosted_upload_and_swizzle(gis_url, gis_user, gis_password, treatment_index_points_view_id, treatment_index_points_data_ids, pg_conn, target_schema,
                              treatment_index_points_table, chunk, start_objectid)

    pg_conn.close()
