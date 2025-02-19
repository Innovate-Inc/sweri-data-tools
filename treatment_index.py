import os
os.environ["CRYPTOGRAPHY_OPENSSL_NO_LEGACY"]="1"
import arcpy
from dotenv import load_dotenv
import logging
import re
from arcgis.features import FeatureLayer
import watchtower

from sweri_utils.sql import rename_postgres_table, connect_to_pg_db, postgres_create_index
from sweri_utils.download import get_ids, service_to_postgres
from sweri_utils.files import gdb_to_postgres
from error_flagging import flag_duplicates, flag_high_cost, flag_uom_outliers

logger = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',filename='./treatment_index.log', encoding='utf-8', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
logger.addHandler(watchtower.CloudWatchLogHandler())

def create_temp_table(sde_file, table_name, projection, cur, schema):

    table_location = os.path.join(sde_file,f'{table_name}_temp')
    if arcpy.Exists(table_location):
        arcpy.management.Delete(table_location)

    arcpy.management.CreateFeatureclass(
        out_path=sde_file,
        out_name=f'{table_name}_temp',
        geometry_type='POLYGON',
        spatial_reference=projection
    )
    arcpy.management.AddFields(
        in_table=table_location,
        field_description=[
            ['unique_id','TEXT','Unique Treatment ID', 255, '', ''],
            ['name', 'TEXT', 'Name', 255, '', ''],
            ['state', 'TEXT', 'State', 255, '', ''],
            ['acres', 'DOUBLE', 'Acres', '', '', ''],
            ['treatment_date', 'DATE', 'Treatment Date', '', '', ''],
            ['date_source', 'TEXT', 'Date Source', 255, '', ''],
            ['identifier_database', 'TEXT', 'Identifier Database', 255, '', ''],
            ['date_current', 'DATE', 'Date Current', '', '', ''],
            ['actual_completion_date', 'DATE', 'Actual Completion Date', '', '', ''],
            ['activity_code', 'TEXT', 'Activity Code', 255, '', ''],
            ['activity', 'TEXT', 'Activity', 255, '', ''],
            ['method', 'TEXT', 'Method', 255, '', ''],
            ['equipment', 'TEXT', 'Equipment', 255, '', ''],
            ['category', 'TEXT', 'Category', 255, '', ''],
            ['type', 'TEXT', 'Type', 255, '', ''],
            ['agency', 'TEXT', 'Agency', 255, '', ''],
            ['fund_source', 'TEXT', 'Fund Source', 255, '', ''],
            ['fund_code', 'TEXT', 'Fund Code', 255, '', 'fund_name'],
            ['total_cost', 'DOUBLE', 'Total Cost', '', '', ''],
            ['cost_per_uom', 'DOUBLE', 'Cost per Unit of Measure', '', '', ''],
            ['uom', 'TEXT', 'Unit of Measure', 255, '', ''],
            ['error', 'TEXT', 'Error Code', 255, '', '']
        ]
    )

    fields_to_index = ['unique_id','name','state','acres','treatment_date','date_source',
    'identifier_database','date_current','actual_completion_date','activity','activity_code','method','equipment',
    'category','type','agency','fund_source','fund_code', 'total_cost', 'cost_per_uom','uom','error']

    for field in fields_to_index:
        postgres_create_index(cur, schema, f'{table_name}_temp', field)


def update_nfpors(cursor, schema, sde_file, wkid, insert_nfpors_additions):
    nfpors_url = os.getenv('NFPORS_URL')
    where = create_nfpors_where_clause()
    destination_table = 'nfpors'
    database = 'sweri'
    
    service_to_postgres(nfpors_url, where, wkid, database, schema, destination_table, cursor, sde_file, insert_nfpors_additions)

def create_nfpors_where_clause():
    #some ids break download, those will be excluded
    exclusion_ids = os.getenv('EXCLUSION_IDS')
    exlusion_ids_tuple = tuple(exclusion_ids.split(",")) if len(exclusion_ids) > 0 else tuple()

    where_clause = f"1=1"
    if len(exlusion_ids_tuple) > 0:
        where_clause += f' and objectid not in ({",".join(exlusion_ids_tuple)})'
    
    return where_clause

def insert_nfpors_additions(cursor, schema):
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

    cursor.execute('BEGIN;')
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
    cursor.execute('COMMIT;')

def nfpors_insert(cursor, schema, treatment_index):
    cursor.execute('BEGIN;')
    cursor.execute(f'''
                   
    INSERT INTO {schema}.{treatment_index}_temp(

        objectid, 
        name,  date_current, actual_completion_date, 
        acres, type, category, fund_code, 
        identifier_database, unique_id,
        state, treatment_date, date_source, 
        agency, shape
    )
    SELECT

        sde.next_rowid('{schema}', '{treatment_index}_temp'),
        trt_nm AS name, modifiedon AS date_current, act_comp_dt AS actual_completion_date,
        gis_acres AS acres, type_name AS type, cat_nm AS category, isbil as fund_code,
        'NFPORS' AS identifier_database, CONCAT(nfporsfid,'-',trt_id) AS unique_id,
        st_abbr AS state, act_comp_dt as treatment_date, 'act_comp_dt' as date_source,
        agency as agency, shape

    FROM {schema}.nfpors
    WHERE {schema}.nfpors.shape IS NOT NULL;
    ''')
    cursor.execute('COMMIT;')

    logger.info(f'NFPORS entries inserted into {schema}.{treatment_index}_temp')

def hazardous_fuels_insert(cursor, schema, treatment_index):
    cursor.execute('BEGIN;')
    cursor.execute(f'''
                   
    INSERT INTO {schema}.{treatment_index}_temp(

        objectid, name, 
        date_current, actual_completion_date, acres,    
        type, category, fund_code, cost_per_uom,
        identifier_database, unique_id,
        uom, state, activity, activity_code, treatment_date,
        date_source, method, equipment, agency,
        shape

    )
    SELECT

        sde.next_rowid('{schema}', '{treatment_index}_temp'), activity_sub_unit_name AS name,
        etl_modified_date_haz AS date_current, date_completed AS actual_completion_date, gis_acres AS acres,
        treatment_type AS type, cat_nm AS category, fund_code AS fund_code, cost_per_uom AS cost_per_uom,
        'FACTS Hazardous Fuels' AS identifier_database, activity_cn AS unique_id,
        uom AS uom, state_abbr AS state, activity AS activity, activity_code as activity_code, date_completed AS treatment_date,
        'date_completed' AS date_source, method AS method, equipment AS equipment,
        'USFS' AS agency, shape
        
    FROM {schema}.facts_haz_3857_2
    WHERE {schema}.facts_haz_3857_2.shape IS NOT NULL;
    
    ''')
    cursor.execute('COMMIT;')

    logger.info(f'FACTS entries inserted into {schema}.{treatment_index}_temp')
    #FACTS Insert Complete 

def hazardous_fuels_date_filtering(cursor, schema):
    cursor.execute('BEGIN;')
    cursor.execute(f'''             
        DELETE FROM {schema}.facts_haz_3857_2 WHERE
        date_completed < '1984-1-1'::date
        OR
        (date_completed is null AND date_planned < '1984-1-1'::date);
    ''')
    logger.info('Records from before Jan 1 1984 deleted from FACTS Hazardous Fuels')

def hazardous_fuels_treatment_date(cursor, schema, treatment_index):
    cursor.execute('BEGIN;')
    cursor.execute(f'''
        UPDATE {schema}.{treatment_index}_temp t
        SET treatment_date = f.date_planned,
        date_source = 'date_planned'
        FROM {schema}.facts_haz_3857_2 f
        WHERE t.treatment_date is null
        AND t.identifier_database = 'FACTS Hazardous Fuels'
        AND t.unique_id = f.activity_cn;
    ''')
    cursor.execute('COMMIT;')
    logger.info(f'updated treatment_date for FACTS Hazardous Fuels entries in {schema}.{treatment_index}_temp')

def nfpors_date_filtering(cursor, schema):
    cursor.execute('BEGIN;')
    cursor.execute(f'''             
        DELETE FROM {schema}.nfpors WHERE
        act_comp_dt < '1984-1-1'::date
        OR
        (act_comp_dt is null AND plan_int_dt < '1984-1-1'::date)
        OR
        ((act_comp_dt is null AND plan_int_dt is NULL) AND col_date < '1984-1-1'::date);
    ''')
    cursor.execute('COMMIT;')
    logger.info('Records from before Jan 1 1984 deleted from NFPORS')

def nfpors_fund_code(cursor, schema, treatment_index):

    cursor.execute('BEGIN;')
    cursor.execute(f'''   
        UPDATE {schema}.{treatment_index}_temp
        SET fund_code = null
        WHERE fund_code = 'No';        
    ''')
    cursor.execute('COMMIT;')

    cursor.execute('BEGIN;')
    cursor.execute(f'''   
        UPDATE {schema}.{treatment_index}_temp
        SET fund_code = 'BIL'
        WHERE fund_code = 'Yes';
    ''')
    cursor.execute('COMMIT;')

    logger.info(f'updated treatment_date for NFPORS entries in {schema}.{treatment_index}_temp')


def nfpors_treatment_date(cursor, schema,treatment_index):
    cursor.execute('BEGIN;')
    cursor.execute(f'''
        UPDATE {schema}.{treatment_index}_temp t
        SET treatment_date = n.plan_int_dt,
        date_source = 'plan_int_dt'
        FROM {schema}.nfpors n
        WHERE t.identifier_database = 'NFPORS'
        AND t.treatment_date is null
        AND t.unique_id = CONCAT(n.nfporsfid,'-',n.trt_id);
    ''')
    cursor.execute('COMMIT;')

    cursor.execute('BEGIN;')
    cursor.execute(f'''
        UPDATE {schema}.{treatment_index}_temp t
        SET treatment_date = n.col_date,
        date_source = 'col_date'
        FROM {schema}.nfpors n
        WHERE t.identifier_database = 'NFPORS'
        AND t.treatment_date is null
        AND t.unique_id = CONCAT(n.nfporsfid,'-',n.trt_id);
    ''')
    cursor.execute('COMMIT;')

    logger.info(f'updated treatment_date for NFPORS entries in {schema}.{treatment_index}_temp')

def fund_source_updates(cursor, schema, treatment_index):
    cursor.execute('BEGIN;')
    cursor.execute(f'''
        UPDATE {schema}.{treatment_index}_temp 
        SET fund_source = 'Multiple'
        WHERE fund_code LIKE '%,%';
    ''')
    cursor.execute('COMMIT;')

    cursor.execute('BEGIN;')
    cursor.execute(f'''
        UPDATE {schema}.{treatment_index}_temp 
        SET fund_source = 'No Funding Code'
        WHERE fund_code is null;
    ''')
    cursor.execute('COMMIT;')

    cursor.execute('BEGIN;')
    cursor.execute(f'''
        UPDATE {schema}.{treatment_index}_temp ti
        SET fund_source = lt.fund_source
        FROM {schema}.fund_source_lookup lt
        WHERE ti.fund_code = lt.fund_code
        AND ti.fund_source IS null;
    ''')
    cursor.execute('COMMIT;')

    cursor.execute('BEGIN;')
    cursor.execute(f'''
        UPDATE {schema}.{treatment_index}_temp
        SET fund_source = 'Other'
        WHERE fund_source IS null 
        AND
        fund_code IS NOT null;
    ''')
    cursor.execute('COMMIT;')

    logger.info(f'updated fund_source in {schema}.{treatment_index}_temp')

def fix_typos(cursor, schema, treatment_index):
    cursor.execute('BEGIN;')
    cursor.execute(f'''
        UPDATE {schema}.{treatment_index}_temp
        SET type = 'Biomass Removal'
        WHERE 
        type = 'Biomass Removall'
    ''')
    cursor.execute('COMMIT;')

def update_total_cost(cursor, schema, treatment_index):
    cursor.execute('BEGIN;')
    cursor.execute(f'''      
        UPDATE {schema}.{treatment_index}_temp
        SET total_cost = 
            CASE
                WHEN uom = 'EACH' THEN cost_per_uom
                WHEN uom = 'ACRES' THEN cost_per_uom * acres
                WHEN uom = 'MILES' THEN cost_per_uom * (acres / 640)
                ELSE total_cost
            END;
    ''')
    cursor.execute('COMMIT;')


def treatment_index_renames(cursor, schema, treatment_index, sde_file):
    # backup to backup temp
    rename_postgres_table(cursor, schema, f'{treatment_index}_backup', f'{treatment_index}_backup_temp')
    logger.info(f'{schema}.{treatment_index}_backup renamed to {schema}.{treatment_index}_backup_temp')

    # current to backup
    rename_postgres_table(cursor, schema, f'{treatment_index}', f'{treatment_index}_backup')
    logger.info(f'{schema}.{treatment_index} renamed to {schema}.{treatment_index}_backup')

    # temp to current
    rename_postgres_table(cursor, schema, f'{treatment_index}_temp', f'{treatment_index}')
    logger.info(f'{schema}.{treatment_index}_temp renamed to {schema}.{treatment_index}')

    backup_temp_location = os.path.join(sde_file, f'{treatment_index}_backup_temp')

    if arcpy.Exists(backup_temp_location):
        arcpy.management.Delete(backup_temp_location)

def create_treatment_points(schema, sde_file, treatment_index):
    temp_polygons = os.path.join(sde_file, f"sweri.{schema}.{treatment_index}_temp")
    temp_points = os.path.join(sde_file, f"sweri.{schema}.{treatment_index}_points_temp")

    arcpy.management.FeatureToPoint(
        in_features=temp_polygons,
        out_feature_class=temp_points,
        point_location="INSIDE"
    )

    fields_to_index = ['unique_id','name','state','acres','treatment_date','date_source',
    'identifier_database','date_current','actual_completion_date','activity','activity_code','method','equipment',
    'category','type','agency','fund_source','fund_code', 'total_cost', 'cost_per_uom','uom','error']

    for field in fields_to_index:
        postgres_create_index(cur, schema, f'{treatment_index}_points_temp', field)

    arcpy.management.EnableFeatureBinning(
        in_features=temp_points,
    )
    arcpy.management.ManageFeatureBinCache(
        in_features=temp_points,
        bin_type="FLAT_HEXAGON",
        max_lod="COUNTY",
        add_cache_statistics="acres SUM",
        delete_cache_statistics=None
    )
    logger.info(f'points layer created at {temp_points}')



def rename_treatment_points(schema, sde_file, cursor, treatment_index):

    # rename current table to backup
    rename_postgres_table(cursor, schema, f'{treatment_index}_points', f'{treatment_index}_points_backup')
    logger.info(f'{schema}.{treatment_index}_points renamed to {schema}.{treatment_index}_points_backup')

    # rename temp to current table
    rename_postgres_table(cursor, schema, f'{treatment_index}_points_temp', f'{treatment_index}_points')
    logger.info(f'{schema}.{treatment_index}_points_temp renamed to {schema}.{treatment_index}_points')

    # rename backup to temp so we can delete it
    rename_postgres_table(cursor, schema, f'{treatment_index}_points_backup', f'{treatment_index}_points_temp')
    logger.info(f'{schema}.{treatment_index}_points_backup renamed to {schema}.{treatment_index}_points_temp')

    # clean up so we can create points again later
    backup_points_temp_location = os.path.join(sde_file, f'{treatment_index}_backup_temp')

    if arcpy.Exists(backup_points_temp_location):
        arcpy.management.Delete(backup_points_temp_location)

    arcpy.management.Delete(os.path.join(sde_file, f"sweri.{schema}.{treatment_index}_points_temp"))

# BEGIN Common Attributes Functions

def add_fields_and_indexes(feature_class, region):
    #adds fields that will contain the pass/fail for each rule, as well as an overall included to be populated later

    new_fields = ('included', 'r2', 'r3', 'r4','r5', 'r6')
    for field in new_fields:
        arcpy.management.AddField(feature_class, field, 'TEXT')
        arcpy.management.AddIndex(feature_class, field, f'{field}_idx_{region}', ascending="ASCENDING")

    arcpy.management.AddIndex(feature_class, 'event_cn', f'event_cn_idx_{region}', unique="UNIQUE", ascending="ASCENDING")
    arcpy.management.AddIndex(feature_class, 'date_completed', f'date_completed_idx_{region}', ascending="ASCENDING")
    arcpy.management.AddIndex(feature_class, 'act_created_date', f'act_created_date_idx_{region}', ascending="ASCENDING")


    new_indexes = ('gis_acres', 'activity', 'equipment', 'method')
    for index in new_indexes: 
            arcpy.management.AddIndex(feature_class, index, f'{index}_idx_{region}', ascending="ASCENDING")

def common_attributes_date_filtering(cursor, schema, table_name):
    # Excludes treatment entries before 1984
    # uses date_completed if available, and act_created_date if date_completed is null

    cursor.execute('BEGIN;')
    cursor.execute(f'''
                   
    DELETE from {schema}.{table_name} WHERE
    date_completed < '1984-1-1'::date
    OR
    (date_completed is null AND act_created_date < '1984-1-1'::date);

          
    ''')
    cursor.execute('COMMIT;')

    logger.info(f"Records from before Jan 1 1984 deleted from {schema}.{table_name}")


def exclude_facts_hazardous_fuels(cursor, schema, table, facts_haz_table):
    # Excludes FACTS Common Attributes records already being included via FACTS Hazardous Fuels

    cursor.execute('BEGIN;')
    cursor.execute(f'''
                   
    DELETE FROM {schema}.{table}
    WHERE 
    event_cn IN(
        SELECT activity_cn FROM {schema}.{facts_haz_table}
    )
          
    ''')
    cursor.execute('COMMIT;')
    logger.info(f"deleted {schema}.{table} entries that are also in FACTS Hazardous Fuels")

def exclude_by_acreage(cursor, schema, table):
    #removes all treatments with null acerage or <= 5 acres

    cursor.execute('BEGIN;')
    cursor.execute(f'''
                   
    DELETE FROM {schema}.{table}
    WHERE
    gis_acres <= 5 OR
    gis_acres IS NULL; 
    
    ''')
    cursor.execute('COMMIT;')
    logger.info(f"deleted Entries <= 5 acres {schema}.{table}")

def trim_whitespace(cursor, schema, table):
    #Some entries have spaces before or after that interfere with matching, this trims those spaces out

    cursor.execute('BEGIN;')
    cursor.execute(f'''
                   
    UPDATE {schema}.{table}
    SET
    activity = TRIM(activity),
    method = TRIM(method),
    equipment = TRIM(equipment);
    
    ''')
    cursor.execute('COMMIT;')
    logger.info(f"removed white space from activity, method, and equipment in {schema}.{table}")

def include_logging_activities(cursor, schema, table):
    # Make sure the activity includes 'thin' or 'cut'
    # Checks the lookup table for method and equipment slated for inclusion

    cursor.execute('BEGIN;')
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
    cursor.execute('COMMIT;')
    logger.info(f"r2 set to 'PASS' for logging activities with proper methods and equipment in {schema}.{table}")
    
def include_fire_activites(cursor, schema, table):
    # Make sure the activity includes 'burn' or 'fire'
    # Checks the lookup table for method and equipment slated for inclusion

    cursor.execute('BEGIN;')
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
    cursor.execute('COMMIT;')
    logger.info(f"r3 set to 'PASS' for fire activities with proper methods and equipment in {schema}.{table}")

def include_fuel_activities(cursor, schema, table):
    # Make sure the activity includes 'fuel'
    # Checks the lookup table for method and equipment slated for inclusion

    cursor.execute('BEGIN;')
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
    cursor.execute('COMMIT;')
    logger.info(f"r4 set to 'PASS' for fuel activities with proper methods and equipment in {schema}.{table}")

def activity_filter(cursor, schema, table):
    # Filters based on activity to ensure only intended activities enter the database

    cursor.execute('BEGIN;')
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
    cursor.execute('COMMIT;')
    logger.info(f"r6 set to 'PASS' passing activities with acceptable activity values {schema}.{table}")


def include_other_activites(cursor, schema, table):
    #lookup based inclusion not dependent on other rules

    cursor.execute('BEGIN;')
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
    cursor.execute('COMMIT;')
    logger.info(f"r5 set to 'PASS' for other activities with proper methods and equipment in {schema}.{table}")

def common_attributes_type_filter(cursor, schema, treatment_index):
    cursor.execute('BEGIN;')
    cursor.execute(f'''           
        DELETE from {schema}.{treatment_index}_temp 
        WHERE
        type IN (
            SELECT value from {schema}.common_attributes_lookup
            WHERE filter = 'type'
            AND include = 'FALSE')
        AND
        identifier_database = 'FACTS Common Attributes';
    ''')
    cursor.execute('COMMIT;')
    logger.info(f"deleted Common Attributes problem types from {schema}.{treatment_index}")

def set_included(cursor, schema, table):
    # set included to yes when r5 passes or
    # r2, r3, or, r4 passes and r6 passes

    cursor.execute('BEGIN;')
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
    cursor.execute('COMMIT;')    
    logger.info(f"included set to 'yes' for {schema}.{table} records with proper rules passing")

def common_attributes_insert(cursor, schema, table, treatment_index):
    # insert records where included = 'yes'
    
    cursor.execute('BEGIN;')
    cursor.execute(f'''
                   
    INSERT INTO {schema}.{insert_table}_temp(

        objectid, name, date_current, actual_completion_date, acres, 
        type, category, fund_code, cost_per_uom, identifier_database, 
        unique_id, uom, state, activity, activity_code, treatment_date, date_source, 
        method, equipment, agency, shape

    )
    SELECT

        sde.next_rowid('{schema}', '{insert_table}_temp'),
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
    cursor.execute('COMMIT;')
    logger.info(f"{schema}.{table} inserted into {schema}.{treatment_index}_temp where included = 'yes'")

def common_attributes_treatment_date(cursor, schema, table, treatment_index):
    cursor.execute('BEGIN;')
    cursor.execute(f'''
        UPDATE {schema}.{treatment_index}_temp t
        SET treatment_date = f.act_created_date,
        date_source = 'act_created_date'
        FROM {schema}.{table} f
        WHERE t.treatment_date is null
        AND t.identifier_database = 'FACTS Common Attributes'
        AND t.unique_id = f.event_cn;
    ''')
    cursor.execute('COMMIT;')
    logger.info(f'updated treatment_date for FACTS Common Attributes entries in {schema}.{treatment_index}_temp')

def flag_errors(cursor, schema, target_table):
    flag_high_cost(cursor, schema, target_table)
    flag_duplicates(cursor, schema, target_table)  
    flag_uom_outliers(cursor, schema, target_table) 


def common_attributes_download_and_insert(projection, sde_file, schema, cursor, treatment_index, facts_haz_table):
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
        postgres_fc = os.path.join(sde_file, table_name)

        gdb_to_postgres(url, gdb, projection, common_attributes_fc_name, table_name, sde_file, schema)

        add_fields_and_indexes(postgres_fc, region_number) 

        common_attributes_date_filtering(cursor, schema, table_name)
        exclude_by_acreage(cursor, schema, table_name)
        exclude_facts_hazardous_fuels(cursor, schema, table_name, facts_haz_table)

        trim_whitespace(cursor, schema, table_name)

        include_logging_activities(cursor, schema, table_name)
        include_fire_activites(cursor, schema, table_name)
        include_fuel_activities(cursor, schema, table_name)
        activity_filter(cursor, schema, table_name)
        include_other_activites(cursor, schema, table_name)

        set_included(cursor, schema, table_name)

        common_attributes_insert(cursor, schema, table_name, treatment_index)
        common_attributes_treatment_date(cursor, schema, table_name, treatment_index)

        #Deletes singluar region pg table after processing that table
        if arcpy.Exists(postgres_fc):
            arcpy.management.Delete(postgres_fc)


if __name__ == "__main__":

    load_dotenv()

    arcpy.env.workspace = arcpy.env.scratchGDB
    arcpy.env.overwriteOutput = True
    out_wkid = 3857
    target_projection = arcpy.SpatialReference(out_wkid)

    sde_connection_file = os.getenv('SDE_FILE')
    target_schema = os.getenv('SCHEMA')
    exluded_ids = os.getenv('EXCLUSION_IDS')
    facts_haz_gdb_url = os.getenv('FACTS_GDB_URL')
    facts_haz_gdb = 'S_USA.Activity_HazFuelTrt_PL.gdb'
    facts_haz_fc_name = 'Activity_HazFuelTrt_PL'
    hazardous_fuels_table = 'facts_haz_3857_2'

    #This is the path of the final table, _backup of this table must also exist
    insert_table = f'treatment_index'

    cur = connect_to_pg_db(os.getenv('DB_HOST'), os.getenv('DB_PORT'), os.getenv('DB_NAME'), os.getenv('DB_USER'), os.getenv('DB_PASSWORD'))

    create_temp_table(sde_connection_file, insert_table, target_projection, cur, target_schema)  

    # gdb_to_postgres here updates FACTS Hazardous Fuels in our Database
    common_attributes_download_and_insert(target_projection, sde_connection_file, target_schema, cur, insert_table, hazardous_fuels_table)
    update_nfpors(cur, target_schema, sde_connection_file, out_wkid, insert_nfpors_additions)
    gdb_to_postgres(facts_haz_gdb_url, facts_haz_gdb, target_projection, facts_haz_fc_name, hazardous_fuels_table, sde_connection_file, target_schema)

    common_attributes_type_filter(cur, target_schema, insert_table)

    #MERGE
    nfpors_date_filtering(cur, target_schema)
    nfpors_insert(cur, target_schema, insert_table)
    nfpors_fund_code(cur, target_schema, insert_table)
    nfpors_treatment_date(cur, target_schema, insert_table)

    # Insert FACTS entries and enter proper treatement dates
    hazardous_fuels_date_filtering(cur, target_schema)
    hazardous_fuels_insert(cur, target_schema, insert_table)
    hazardous_fuels_treatment_date(cur, target_schema, insert_table)

    # Insert NFPORS, convert isbil Yes/No to fund_code 'BIL'/null
    fund_source_updates(cur, target_schema, insert_table)
    update_total_cost(cur, target_schema, insert_table)
    fix_typos(cur, target_schema, insert_table)

    arcpy.management.RebuildIndexes(sde_connection_file, 'NO_SYSTEM', f'sweri.{target_schema}.{insert_table}_temp', 'ALL')

    flag_errors(cur, target_schema, f'{insert_table}_temp')

    create_treatment_points(target_schema, sde_connection_file, insert_table)
    rename_treatment_points(target_schema, sde_connection_file, cur, insert_table)

    # current -> backup
    # temp -> current
    # delete backup temp
    treatment_index_renames(cur, target_schema, insert_table, sde_connection_file)