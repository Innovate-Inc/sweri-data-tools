import os
from sweri_utils.files import get_wkid_from_geoparquet, geoparquet_to_postgres
from sweri_utils.sql import add_column
from sweri_utils.sweri_logging import log_this

# BEGIN Common Attributes Functions


def add_fields_and_indexes(conn, schema, feature_class, region):
    # adds fields that will contain the pass/fail for each rule, as well as an overall included to be populated later

    new_fields = ('included', 'r2', 'r3', 'r4', 'r5', 'r6')
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
    # removes all treatments with null acerage or <= 5 acres
    cursor = conn.cursor()
    with conn.transaction():
        cursor.execute(f'''
        DELETE FROM {schema}.{table}
        WHERE
        gis_acres <= 5 OR
        gis_acres IS NULL;
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
    # lookup based inclusion not dependent on other rules

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
def update_state_data(parquet_file, out_wkid, schema,  ogr_db_string):
    where = "DataCategory = 'State'"

    in_wkid = get_wkid_from_geoparquet(parquet_file)

    destination_table = 'state_data'
    geoparquet_to_postgres(parquet_file, out_wkid, destination_table, schema, ogr_db_string, where, in_wkid)
    # service_to_postgres(service_url, where, wkid, ogr_db_string, schema, destination_table, conn, 40)

def state_data_insert(conn, schema, treatment_index):
    cursor = conn.cursor()
    with conn.transaction():
        cursor.execute(f'''

        INSERT INTO {schema}.{treatment_index} (

            objectid, name, treatment_date, date_current,
            acres, fund_code, identifier_database, 
            category, unique_id, state, agency,
            total_cost, status, shape
        )
        SELECT

            sde.next_rowid('{schema}', '{treatment_index}'),
            treatmentname AS name, actualcompletiondate AS treatment_date, edit_date as date_current,
            treatmentgisacres AS acres, federalfundingprogram as fund_code, 'NASF' AS identifier_database, 
            treatmentcategory as category, globalid AS unique_id, source AS state, treatmentidentifierdatabase as agency, 
            federalfundingamount as total_cost, 'Completed' as status, geometry as shape
        FROM {schema}.state_data
        WHERE {schema}.state_data.geometry IS NOT NULL
        and
        {schema}.state_data.actualcompletiondate IS NOT NULL;

        ''')

def null_missing_state_fund_codes(conn, schema, table):
    cursor = conn.cursor()
    with conn.transaction():
        cursor.execute(f'''

            UPDATE {schema}.{table} 
            SET fund_code = null 
            WHERE identifier_database = 'NASF' 
            AND
            (fund_code = 'VALUE NOT GIVEN'
            OR fund_code = 'VALUE NOT MAPPED');

        ''')

def null_missing_state_categories(conn, schema, table):
    cursor = conn.cursor()
    with conn.transaction():
        cursor.execute(f'''

            UPDATE {schema}.{table} 
            SET category = null 
            WHERE identifier_database = 'NASF' 
            AND
            (category = 'VALUE NOT GIVEN'
            OR category = 'VALUE NOT MAPPED');

        ''')
