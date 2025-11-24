# BEGIN Common Attributes Functions
from sweri_utils.sql import add_column
from sweri_utils.sweri_logging import log_this


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
