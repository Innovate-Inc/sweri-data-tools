from ..sweri_utils.logging import log_this, logging
from ..sweri_utils.sql import add_column

logger = logging.getLogger(__name__)


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
        cursor.execute(f'''

        DELETE from {schema}.{table_name} WHERE
        date_completed < '1984-1-1'::date
        OR
        (date_completed is null AND act_created_date < '1984-1-1'::date);


        ''')

    logger.info(f"Records from before Jan 1 1984 deleted from {schema}.{table_name}")


@log_this
def exclude_facts_hazardous_fuels(conn, schema, table, facts_haz_table):
    # Excludes FACTS Common Attributes records already being included via FACTS Hazardous Fuels
    cursor = conn.cursor()
    with conn.transaction():
        cursor.execute(f'''

        DELETE FROM {schema}.{table}
        WHERE EXISTS (
            SELECT 1 FROM {schema}.{facts_haz_table}
            WHERE activity_cn = event_cn 
        )

        ''')
    logger.info(f"deleted {schema}.{table} entries that are also in FACTS Hazardous Fuels")


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
    logger.info(f"deleted Entries <= 5 acres {schema}.{table}")


@log_this
def trim_whitespace(conn, schema, table):
    # Some entries have spaces before or after that interfere with matching, this trims those spaces out
    cursor = conn.cursor()
    with conn.transaction():
        cursor.execute(f'''

        UPDATE {schema}.{table}
        SET
        activity = TRIM(activity),
        method = TRIM(method),
        equipment = TRIM(equipment);

        ''')
    logger.info(f"removed white space from activity, method, and equipment in {schema}.{table}")


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
    logger.info(f"r2 set to 'PASS' for logging activities with proper methods and equipment in {schema}.{table}")


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
    logger.info(f"r3 set to 'PASS' for fire activities with proper methods and equipment in {schema}.{table}")


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
    logger.info(f"r4 set to 'PASS' for fuel activities with proper methods and equipment in {schema}.{table}")


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
    logger.info(f"r6 set to 'PASS' passing activities with acceptable activity values {schema}.{table}")


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
    logger.info(f"r5 set to 'PASS' for other activities with proper methods and equipment in {schema}.{table}")


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
    logger.info(f"deleted Common Attributes problem types from {schema}.{treatment_index}")


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
    logger.info(f"included set to 'yes' for {schema}.{table} records with proper rules passing")


def common_attributes_insert(conn, schema, table, treatment_index):
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
    logger.info(f"{schema}.{table} inserted into {schema}.{treatment_index} where included = 'yes'")


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
    logger.info(f'updated treatment_date for FACTS Common Attributes entries in {schema}.{treatment_index}')
