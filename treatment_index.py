import os
os.environ["CRYPTOGRAPHY_OPENSSL_NO_LEGACY"]="1"
from dotenv import load_dotenv
import re

from sweri_utils.sql import connect_to_pg_db, postgres_create_index, add_column
from sweri_utils.download import service_to_postgres, get_ids
from sweri_utils.files import gdb_to_postgres, download_file_from_url, extract_and_remove_zip_file
from error_flagging import flag_duplicates, flag_high_cost, flag_uom_outliers
from sweri_utils.logging import logging, log_this

logger = logging.getLogger(__name__)

def update_nfpors(nfpors_url, conn, schema, wkid, insert_nfpors_additions, ogr_db_string):
    where = create_nfpors_where_clause()
    destination_table = 'nfpors'
    # database = 'sweri'

    try:
        service_to_postgres(nfpors_url, where, wkid, ogr_db_string, schema, destination_table, conn, 70)
    except Exception as e:
        logger.error(f'Error downloading NFPORS: {e}... continuing')
        pass

@log_this
def update_ifprs(conn, schema, wkid, service_url, ogr_db_string):
    where = '1=1'
    destination_table = 'ifprs_actual_treatment'
    # database = 'sweri'

    service_to_postgres(service_url, where, wkid, ogr_db_string, schema, destination_table, conn, 250)

def create_nfpors_where_clause():
    #some ids break download, those will be excluded
    exclusion_ids = os.getenv('EXCLUSION_IDS')
    exlusion_ids_tuple = tuple(exclusion_ids.split(",")) if len(exclusion_ids) > 0 else tuple()

    where_clause = f"1=1"
    if len(exlusion_ids_tuple) > 0:
        where_clause += f' and objectid not in ({",".join(exlusion_ids_tuple)})'

    return where_clause

@log_this
def insert_nfpors_additions(conn, schema):
    common_fields = '''
        trt_unt_id, local_id, col_date, trt_status, col_meth,
        comments, gis_acres, pstatus, modifiedon, createdon, 
        cent_lat, cent_lon, userid, st_abbr, cong_dist, cnty_fips,
        trt_nm, fy, plan_acc_ac, act_acc_ac, act_init_dt, act_comp_dt,
        nfporsfid, trt_id_db, type_name, cat_nm, trt_statnm, col_methnm,
        plan_int_dt, unit_id, agency, trt_id, created_by, edited_by,
        projectname, regionname, projectid, keypointarea, unitname,
        deptname, countyname, statename, regioncode, districtname,
        isbil, bilfunding, shape
    '''

    cursor = conn.cursor()
    with conn.transaction():
        cursor.execute(f'''
        INSERT INTO {schema}.nfpors (
            objectid,
            {common_fields},
            globalid
            )
        SELECT 
            sde.next_rowid('{schema}', 'nfpors'),
            {common_fields},
            sde.next_globalid()
        FROM {schema}.nfpors_additions;
        ''')

@log_this
def insert_ifprs_additions(conn, schema):
    common_fields = '''
        objectid, actualtreatmentid, ispoint, createdby, name, unit, region,
        agency, department, isdepartmentmanual, latitude, longitude,
        calculatedacres, iswui, initiationdate, initiationfiscalyear,
        initiationfiscalquarter, completiondate, completionfiscalyear,
        completionfiscalquarter, notes, lastmodifiedby, createdondate,
        lastmodifieddate, status, statusreason, isarchived, class,
        category, type, durability, priority, fundingsource,
        congressionaldistrictnumber, county, state, estimatedpersonnelcost, 
        estimatedassetcost, estimatedgrantsfixedcost, estimatedcontractualcost,
        estimatedothercost, estimatedtotalcost, localapprovaldate, 
        regionalapprovaldate, agencyapprovaldate, departmentapprovaldate,
        fundeddate, estimatedsuccessprobability, feasibility, isapproved,
        isfunded, tribename, totalacres, fundingunit, fundingregion,
        fundingagency, fundingdepartment, fundingtribe, wbsid, costcenter,
        functionalarea, costcode, cancelleddate, hasgroup, groupcount,
        unitid, vegdeparturepercentagederived, vegdeparturepercentagemanual,
        isvegetationmanual, isrtrl, fundingsubunit, fundingunittype, isbil,
        bilfunding, treatmentdriver, contributedfundingsource, contributednotes,
        contributedpersonnelcost, contributedassetcost, contributedgrantsfixedcost,
        contributedcontractualcost, contributedothercost, contributedtotalcost,
        contributedcostcenter, contributedfunctionalarea, contributedcostcode,
        fundingsourceprogram, fundingsourcecategory, fundingsourcesubcategory,
        obligationfiscalyear, carryoverfiscalyear, iscarryover, iscanceled,
        entityid, entitytype, entitycategory, subtype, fundingunitid,
        originalinitiationdate, originalinitiationfiscalyear, 
        originalinitiationfiscalquarter, issagebrush, unapprovaldate,
        unapprovalreason, isfundingacresmanual, shape
        '''

    cursor = conn.cursor()
    with conn.transaction():
        cursor.execute(f'''
        INSERT INTO {schema}.ifprs_actual_treatment (    
            {common_fields}
            )
        SELECT 
            {common_fields}
        FROM {schema}.ifprs_actual_treatment_additions;
        ''')

@log_this
def ifprs_insert(conn, schema, treatment_index):
    cursor = conn.cursor()
    with conn.transaction():
        cursor.execute(f'''
        INSERT INTO {schema}.{treatment_index} (
    
            objectid, 
            name,  date_current, actual_completion_date, 
            acres, type, category, fund_code, fund_source,
            identifier_database, unique_id,
            state, treatment_date, date_source,
            total_cost, twig_category,
            agency, shape
        )
        SELECT
    
            sde.next_rowid('{schema}', '{treatment_index}'),
            name AS name, lastmodifieddate AS date_current, completiondate AS actual_completion_date,
            calculatedacres AS acres, type AS type, category AS category, fundingsourcecategory as fund_code,
            fundingsource as fund_source, 'IFPRS Actual Treatment' AS identifier_database, actualtreatmentid AS unique_id,
            state AS state, completiondate as treatment_date, 'completiondate' as date_source,
            estimatedtotalcost as total_cost, category as twig_category, 
            agency as agency, shape as shape
    
        FROM {schema}.ifprs_actual_treatment
        WHERE {schema}.ifprs_actual_treatment.shape IS NOT NULL;
        ''')

def ifprs_treatment_date(conn, schema, treatment_index):
    cursor = conn.cursor()
    with conn.transaction():
        cursor.execute(f'''
            UPDATE {schema}.{treatment_index} t
            SET treatment_date = i.initiationdate,
            date_source = 'initiationdate'
            FROM {schema}.ifprs_actual_treatment i
            WHERE t.identifier_database = 'IFPRS Actual Treatment'
            AND t.treatment_date is null
            AND t.unique_id = i.actualtreatmentid;
        ''')

        cursor.execute(f'''
            UPDATE {schema}.{treatment_index} t
            SET treatment_date = i.createdondate,
            date_source = 'createdondate'
            FROM {schema}.ifprs_actual_treatment i
            WHERE t.identifier_database = 'IFPRS Actual Treatment'
            AND t.treatment_date is null
            AND t.unique_id = i.actualtreatmentid;
        ''')

def ifprs_date_filtering(conn, schema):
    cursor = conn.cursor()
    with conn.transaction():
        cursor.execute(f'''             
            DELETE FROM {schema}.ifprs_actual_treatment WHERE
            completiondate < '1984-1-1'::date
            OR
            (completiondate is null AND initiationdate < '1984-1-1'::date)
            OR
            ((completiondate is null AND initiationdate is NULL) AND createdondate < '1984-1-1'::date);
        ''')

def nfpors_insert(conn, schema, treatment_index):
    cursor = conn.cursor()
    with conn.transaction():
        cursor.execute(f'''
                       
        INSERT INTO {schema}.{treatment_index} (
    
            objectid, 
            name,  date_current, actual_completion_date, 
            acres, type, category, fund_code, 
            identifier_database, unique_id,
            state, treatment_date, date_source, 
            agency, shape
        )
        SELECT
    
            sde.next_rowid('{schema}', '{treatment_index}'),
            trt_nm AS name, modifiedon AS date_current, act_comp_dt AS actual_completion_date,
            gis_acres AS acres, type_name AS type, cat_nm AS category, isbil as fund_code,
            'NFPORS' AS identifier_database, CONCAT(nfporsfid,'-',trt_id) AS unique_id,
            st_abbr AS state, act_comp_dt as treatment_date, 'act_comp_dt' as date_source,
            agency as agency, shape
    
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
            date_current, actual_completion_date, acres,    
            type, category, fund_code, cost_per_uom,
            identifier_database, unique_id,
            uom, state, activity, activity_code, treatment_date,
            date_source, method, equipment, agency,
            shape
    
        )
        SELECT
    
            sde.next_rowid('{schema}', '{treatment_index}'), activity_sub_unit_name AS name,
            etl_modified_date_haz AS date_current, date_completed AS actual_completion_date, gis_acres AS acres,
            treatment_type AS type, cat_nm AS category, fund_code AS fund_code, cost_per_uom AS cost_per_uom,
            'FACTS Hazardous Fuels' AS identifier_database, activity_cn AS unique_id,
            uom AS uom, state_abbr AS state, activity AS activity, activity_code as activity_code, 
            CASE WHEN date_completed IS NULL THEN date_planned ELSE date_completed END AS treatment_date,
            CASE WHEN date_completed IS NULL THEN 'date_planned' ELSE 'date_completed' END AS date_source, 
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
def nfpors_date_filtering(conn, schema):
    cursor = conn.cursor()
    with conn.transaction():
        cursor.execute(f'''             
            DELETE FROM {schema}.nfpors WHERE
            act_comp_dt < '1984-1-1'::date
            OR
            (act_comp_dt is null AND plan_int_dt < '1984-1-1'::date)
            OR
            ((act_comp_dt is null AND plan_int_dt is NULL) AND col_date < '1984-1-1'::date);
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
def nfpors_treatment_date(conn, schema,treatment_index):
    cursor = conn.cursor()
    with conn.transaction():
        cursor.execute(f'''
            UPDATE {schema}.{treatment_index} t
            SET treatment_date = n.plan_int_dt,
            date_source = 'plan_int_dt'
            FROM {schema}.nfpors n
            WHERE t.identifier_database = 'NFPORS'
            AND t.treatment_date is null
            AND t.unique_id = CONCAT(n.nfporsfid,'-',n.trt_id);
        ''')

        cursor.execute(f'''
            UPDATE {schema}.{treatment_index} t
            SET treatment_date = n.col_date,
            date_source = 'col_date'
            FROM {schema}.nfpors n
            WHERE t.identifier_database = 'NFPORS'
            AND t.treatment_date is null
            AND t.unique_id = CONCAT(n.nfporsfid,'-',n.trt_id);
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
            date_source, identifier_database, date_current, 
            actual_completion_date, activity_code, activity, method, equipment, category, type, agency, 
            fund_source, fund_code, total_cost, cost_per_uom, uom, error)
            select ST_Centroid(shape), 
            sde.next_rowid('{schema}', '{treatment_index}_points'), 
            unique_id, name, state, acres, treatment_date, date_source, identifier_database, date_current, 
            actual_completion_date, activity_code, activity, method, equipment, category, type, agency, 
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
def common_attributes_insert(conn, schema, table, insert_table):
    # insert records where included = 'yes'
    cursor = conn.cursor()
    with conn.transaction():
        cursor.execute(f'''
                       
        INSERT INTO {schema}.{insert_table}(
    
            objectid, name, date_current, actual_completion_date, acres, 
            type, category, fund_code, cost_per_uom, identifier_database, 
            unique_id, uom, state, activity, activity_code, treatment_date, date_source, 
            method, equipment, agency, shape
    
        )
        SELECT
    
            sde.next_rowid('{schema}', '{insert_table}'),
            name AS name, act_modified_date AS date_current, 
            date_completed AS actual_completion_date, gis_acres AS acres, 
            nfpors_treatment AS type, nfpors_category AS category, fund_codes as fund_code, 
            cost_per_unit as cost_per_uom, 'FACTS Common Attributes' AS identifier_database, 
            event_cn AS unique_id, uom as uom, state_abbr AS state, activity as activity,
            activity_code as activity_code, date_completed as treatment_date,
            'date_completed' as date_source, method as method, equipment as equipment, 
            'USFS' as agency, shape as shape
            
        FROM {schema}.{table}
        WHERE included = 'yes'
        AND
        {schema}.{table}.shape IS NOT NULL;
    
        ''')

@log_this
def common_attributes_treatment_date(conn, schema, table, treatment_index):
    cursor = conn.cursor()
    with conn.transaction():
        cursor.execute(f'''
            UPDATE {schema}.{treatment_index} t
            SET treatment_date = f.act_created_date,
            date_source = 'act_created_date'
            FROM {schema}.{table} f
            WHERE t.treatment_date is null
            AND t.identifier_database = 'FACTS Common Attributes'
            AND t.unique_id = f.event_cn;
        ''')

def common_attributes_download_and_insert(projection, cursor, ogr_db_string, schema, treatment_index, facts_haz_table):
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
        table_name = f'common_attributes_{region_number}'
        gdb = f'Actv_CommonAttribute_PL_Region{region_number}.gdb'

        zip_file = f'{table_name}.zip'

        logging.info(f'Downloading {url}')
        download_file_from_url(url, zip_file)

        logging.info(f'Extracting {zip_file}')
        extract_and_remove_zip_file(zip_file)

        gdb_to_postgres(gdb, projection, common_attributes_fc_name, table_name, schema, ogr_db_string)

        add_fields_and_indexes(conn, schema, table_name, region_number)

        common_attributes_date_filtering(conn, schema, table_name)
        exclude_by_acreage(conn, schema, table_name)
        exclude_facts_hazardous_fuels(conn, schema, table_name, facts_haz_table)

        trim_whitespace(conn, schema, table_name)

        include_logging_activities(conn, schema, table_name)
        include_fire_activites(conn, schema, table_name)
        include_fuel_activities(conn, schema, table_name)
        activity_filter(conn, schema, table_name)
        include_other_activites(conn, schema, table_name)

        set_included(conn, schema, table_name)

        common_attributes_insert(conn, schema, table_name, treatment_index)
        common_attributes_treatment_date(conn, schema, table_name, treatment_index)
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

if __name__ == "__main__":
    load_dotenv()

    out_wkid = 4326

    target_schema = os.getenv('SCHEMA')
    exluded_ids = os.getenv('EXCLUSION_IDS')
    facts_haz_gdb_url = os.getenv('FACTS_GDB_URL')
    ifprs_url = os.getenv('IFPRS_URL')
    facts_haz_gdb = 'S_USA.Activity_HazFuelTrt_PL.gdb'
    facts_haz_fc_name = 'Activity_HazFuelTrt_PL'
    hazardous_fuels_table = 'facts_hazardous_fuels'
    nfpors_url = os.getenv('NFPORS_URL')

    #This is the final table
    insert_table = 'treatment_index'

    conn = connect_to_pg_db(os.getenv('DB_HOST'), os.getenv('DB_PORT'), os.getenv('DB_NAME'),
                                 os.getenv('DB_USER'), os.getenv('DB_PASSWORD'))

    # Truncate the table before inserting new data
    cursor = conn.cursor()
    with conn.transaction():
        cursor.execute(f'''TRUNCATE TABLE {target_schema}.{insert_table}''')
        cursor.execute('COMMIT;')

    ogr_db_string = f"PG:dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')} port={os.getenv('DB_PORT')} host={os.getenv('DB_HOST')}"

    # FACTS Hazardous Fuels
    hazardous_fuels_zip_file = f'{hazardous_fuels_table}.zip'
    download_file_from_url(facts_haz_gdb_url, hazardous_fuels_zip_file)
    extract_and_remove_zip_file(hazardous_fuels_zip_file)
    gdb_to_postgres(facts_haz_gdb, out_wkid, facts_haz_fc_name, hazardous_fuels_table,
                    target_schema, ogr_db_string)
    hazardous_fuels_date_filtering(conn, target_schema, hazardous_fuels_table)
    hazardous_fuels_insert(conn, target_schema, insert_table, hazardous_fuels_table)

    # FACTS Common Attributes
    common_attributes_download_and_insert(out_wkid, conn, ogr_db_string, target_schema, insert_table, hazardous_fuels_table)
    common_attributes_type_filter(conn, target_schema, insert_table)

    # NFPORS
    update_nfpors(nfpors_url, conn, target_schema, out_wkid, insert_nfpors_additions, ogr_db_string)
    nfpors_date_filtering(conn, target_schema)
    nfpors_insert(conn, target_schema, insert_table)
    nfpors_fund_code(conn, target_schema, insert_table)
    nfpors_treatment_date(conn, target_schema, insert_table)

    # IFPRS processing and insert
    update_ifprs(conn, target_schema, out_wkid, ifprs_url, ogr_db_string)
    ifprs_date_filtering(conn, target_schema)
    ifprs_insert(conn, target_schema, insert_table)
    ifprs_treatment_date(conn, target_schema, insert_table)

    # Modify treatment index in place
    fund_source_updates(conn, target_schema, insert_table)
    update_total_cost(conn, target_schema, insert_table)
    correct_biomass_removal_typo(conn, target_schema, insert_table)
    flag_high_cost(conn, target_schema, insert_table)
    flag_duplicates(conn, target_schema, insert_table)
    flag_uom_outliers(conn, target_schema, insert_table)
    add_twig_category(conn, target_schema)

    # update treatment points
    update_treatment_points(conn, target_schema, insert_table)

    conn.close()
