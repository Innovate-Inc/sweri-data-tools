import os
os.environ["CRYPTOGRAPHY_OPENSSL_NO_LEGACY"]="1"
import arcpy
from dotenv import load_dotenv
import logging
import re
from arcgis.features import FeatureLayer

from sweri_utils.sql import rename_postgres_table, connect_to_pg_db
from sweri_utils.download import get_ids, service_to_postgres
from sweri_utils.files import gdb_to_postgres
# import watchtower

logger = logging.getLogger(__name__)
logging.basicConfig( format='%(asctime)s %(levelname)-8s %(message)s',filename='./treatment_index.log', encoding='utf-8', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
# logger.addHandler(watchtower.CloudWatchLogHandler())



def update_nfpors(cursor, schema, sde_file, wkid, insert_nfpors_additions):
    
    nfpors_url = os.getenv('NFPORS_URL')
    where = create_nfpors_where_clause()
    destination_table = 'nfpors'
    database = 'sweri'

    service_to_postgres(nfpors_url, database, schema, destination_table, cursor, sde_file, where, wkid, insert_nfpors_additions)

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
        shape, globalid
    )
    SELECT

        sde.next_rowid('{schema}', '{treatment_index}_temp'),
        trt_nm AS name, modifiedon AS date_current, act_comp_dt AS actual_completion_date,
        gis_acres AS acres, type_name AS type, cat_nm AS category, isbil as fund_code,
        'NFPORS' AS identifier_database, CONCAT(nfporsfid,'-',trt_id) AS unique_id,
        st_abbr AS state, act_comp_dt as treatment_date, 'act_comp_dt' as date_source,
        shape, sde.next_globalid()

    FROM {schema}.nfpors;
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
        uom, state, activity, treatment_date,
        date_source, shape, globalid

    )
    SELECT

        sde.next_rowid('{schema}', '{treatment_index}_temp'), activity_sub_unit_name AS name,
        etl_modified_date_haz AS date_current, date_completed AS actual_completion_date, gis_acres AS acres,
        treatment_type AS type, cat_nm AS category, fund_code AS fund_code, cost_per_uom AS cost_per_uom,
        'FACTS Hazardous Fuels' AS identifier_database, activity_cn AS unique_id,
        uom as uom, state_abbr AS state, activity as activity, date_completed as treatment_date,
        'date_completed' as date_source, shape, sde.next_globalid()
        
    FROM {schema}.facts_haz_3857_2;
    
    ''')
    cursor.execute('COMMIT;')

    logger.info(f'FACTS entries inserted into {schema}.{treatment_index}_temp')
    #FACTS Insert Complete 

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


def treatment_index_renames(cursor, schema, treatment_index):
    # backup to backup temp
    rename_postgres_table(cursor, schema, f'{treatment_index}_backup', f'{treatment_index}_backup_temp')
    logger.info(f'{schema}.{treatment_index}_backup renamed to {schema}.{treatment_index}_backup_temp')

    # current to backup
    rename_postgres_table(cursor, schema, f'{treatment_index}', f'{treatment_index}_backup')
    logger.info(f'{schema}.{treatment_index} renamed to {schema}.{treatment_index}_backup')

    # temp to current
    rename_postgres_table(cursor, schema, f'{treatment_index}_temp', f'{treatment_index}')
    logger.info(f'{schema}.{treatment_index}_temp renamed to {schema}.{treatment_index}')

    #backup temp to temp
    rename_postgres_table(cursor, schema, f'{treatment_index}_backup_temp', f'{treatment_index}_temp')
    logger.info(f'{schema}.{treatment_index}_backup_temp renamed to {schema}.{treatment_index}_temp')

def create_treatment_points(schema, sde_file, treatment_index):
    temp_polygons = os.path.join(sde_file, f"sweri.{schema}.{treatment_index}_temp")
    temp_points = os.path.join(sde_file, f"sweri.{schema}.{treatment_index}_points_temp")

    arcpy.management.FeatureToPoint(
        in_features=temp_polygons,
        out_feature_class=temp_points,
        point_location="INSIDE"
    )
    arcpy.management.AddIndex(
        in_table=temp_points,
        fields="treatment_date",
        index_name="treatment_date_idx",
        unique="NON_UNIQUE",
        ascending="ASCENDING"
    )
    arcpy.management.AddIndex(
        in_table=temp_points,
        fields="acres",
        index_name="treatment_acres_idx",
        unique="NON_UNIQUE",
        ascending="ASCENDING"
    )
    arcpy.management.AddIndex(
        in_table=temp_points,
        fields="name",
        index_name="treatment_name_idx",
        unique="NON_UNIQUE",
        ascending="ASCENDING"
    )
    arcpy.management.AddIndex(
        in_table=temp_points,
        fields="type",
        index_name="treatment_type_idx",
        unique="NON_UNIQUE",
        ascending="ASCENDING"
    )
    arcpy.management.AddIndex(
        in_table=temp_points,
        fields="fund_source",
        index_name="treatment_fund_source_idx",
        unique="NON_UNIQUE",
        ascending="ASCENDING"
    )
    arcpy.management.AddIndex(
        in_table=temp_points,
        fields="actual_completion_date",
        index_name="treatment_actual_completion_date_idx",
        unique="NON_UNIQUE",
        ascending="ASCENDING"
    )
    arcpy.management.AddIndex(
        in_table=temp_points,
        fields="fund_code",
        index_name="treatment_fund_code_idx",
        unique="NON_UNIQUE",
        ascending="ASCENDING"
    )
    arcpy.management.AddIndex(
        in_table=temp_points,
        fields="unique_id",
        index_name="treatment_unique_id_idx",
        unique="NON_UNIQUE",
        ascending="ASCENDING"
    )

    arcpy.management.AddGlobalIDs(temp_points)

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
    arcpy.management.Delete(os.path.join(sde_file, f"sweri.{schema}.{treatment_index}_points_temp"))

# BEGIN Common Attributes Functions

def add_fields_and_indexes(feature_class, region):
    new_fields = ('included', 'r2', 'r3', 'r4','r5', 'r6')
    for field in new_fields:
        arcpy.management.AddField(feature_class, field, 'TEXT')
        arcpy.management.AddIndex(feature_class, field, f'{field}_idx_{region}', ascending="ASCENDING")

    arcpy.management.AddIndex(feature_class, 'event_cn', f'event_cn_idx_{region}', unique="UNIQUE", ascending="ASCENDING")

    new_indexes = ('gis_acres', 'activity', 'equipment', 'method')
    for index in new_indexes: 
            arcpy.management.AddIndex(feature_class, index, f'{index}_idx_{region}', ascending="ASCENDING")
       
def exclude_facts_hazardous_fuels(cursor, schema, table, facts_haz_table):
    # Do Not Included Entries Already Being Included via Hazardous Fuels
    cursor.execute('BEGIN;')
    cursor.execute(f'''
                   
    DELETE FROM {schema}.{table}
    WHERE 
    event_cn IN(
        SELECT activity_cn FROM {schema}.{facts_haz_table}
    )
          
    ''')
    cursor.execute('COMMIT;')
    logging.info(f"deleted {schema}.{table} entries that are also in FACTS Hazardous Fuels")

def exclude_by_acreage(cursor, schema, table):
    cursor.execute('BEGIN;')
    cursor.execute(f'''
                   
    DELETE FROM {schema}.{table}
    WHERE
    gis_acres <= 5 OR
    gis_acres IS NULL; 
    
    ''')
    cursor.execute('COMMIT;')
    logging.info(f"deleted Entries <= 5 acres {schema}.{table}")

def trim_whitespace(cursor, schema, table):
    cursor.execute('BEGIN;')
    cursor.execute(f'''
                   
    UPDATE {schema}.{table}
    SET
    activity = TRIM(activity),
    method = TRIM(method),
    equipment = TRIM(equipment);
    
    ''')
    cursor.execute('COMMIT;')
    logging.info(f"removed white space from activity, method, and equipment in {schema}.{table}")

def include_logging_activities(cursor, schema, table):
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
    logging.info(f"included set to 'yes' for logging activities with proper methods and equipment in {schema}.{table}")
    
def include_fire_activites(cursor, schema, table):
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
    logging.info(f"included set to 'yes' for fire activities with proper methods and equipment in {schema}.{table}")

def include_fuel_activities(cursor, schema, table):
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
    logging.info(f"included set to 'yes' for fuel activities with proper methods and equipment in {schema}.{table}")

def special_exclusions(cursor, schema, table):
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
    logging.info(f"included set to 'no' for special exclusions in {schema}.{table}")

def include_other_activites(cursor, schema, table):
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
    logging.info(f"included set to 'yes' for other activities with proper methods and equipment in {schema}.{table}")

def set_included(cursor, schema, table):
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

def common_attributes_insert(cursor, schema, table, treatment_index):
    #Need to figure out state and dates outside of date completed
    cursor.execute('BEGIN;')
    cursor.execute(f'''
                   
    INSERT INTO {schema}.{insert_table}_temp(

        objectid, name, date_current, actual_completion_date, acres, 
        type, category, fund_code, cost_per_uom, identifier_database, 
        unique_id, uom, state, activity, treatment_date, date_source, 
        shape, globalid

    )
    SELECT

        sde.next_rowid('{schema}', '{insert_table}_temp'),
        name AS name, act_modified_date AS date_current, 
        date_completed AS actual_completion_date, gis_acres AS acres, 
        nfpors_treatment AS type, nfpors_category AS category, fund_codes as fund_code, 
        cost_per_unit as cost_per_uom, 'FACTS Common Attributes' AS identifier_database, 
        event_cn AS unique_id, uom as uom, state_abbr AS state, activity as activity, 
        date_completed as treatment_date, 'date_completed' as date_source, shape, 
        sde.next_globalid()
        
    FROM {schema}.{table}
    WHERE included = 'yes'
    ;

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
        region_number = re.sub("\D", "", url)
        table_name = f'common_attributes_{region_number}'
        gdb = f'Actv_CommonAttribute_PL_Region{region_number}.gdb'
        postgres_fc = os.path.join(sde_file, table_name)

        gdb_to_postgres(url, gdb, projection, common_attributes_fc_name, table_name, sde_file, schema)

        add_fields_and_indexes(postgres_fc, region_number) 

        exclude_by_acreage(cursor, schema, table_name)
        exclude_facts_hazardous_fuels(cursor, schema, table_name, facts_haz_table)

        trim_whitespace(cursor, schema, table_name)

        include_logging_activities(cursor, schema, table_name)
        include_fire_activites(cursor, schema, table_name)
        include_fuel_activities(cursor, schema, table_name)
        special_exclusions(cursor, schema, table_name)
        include_other_activites(cursor, schema, table_name)

        set_included(cursor, schema, table_name)

        common_attributes_insert(cursor, schema, table_name, treatment_index)
        common_attributes_treatment_date(cursor, schema, table_name, treatment_index)


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

    #This is the path of the final table, _temp and _backup of this table must also exist
    insert_table = f'treatment_index_facts_nfpors'

    cur = connect_to_pg_db(os.getenv('DB_HOST'), os.getenv('DB_PORT'), os.getenv('DB_NAME'), os.getenv('DB_USER'), os.getenv('DB_PASSWORD'))
    
    cur.execute(f'TRUNCATE {target_schema}.{insert_table}_temp')

    #gdb_to_postgres here updates FACTS Hazardous Fuels in our Database
    update_nfpors(cur, target_schema, sde_connection_file, out_wkid, insert_nfpors_additions)
    gdb_to_postgres(facts_haz_gdb_url, facts_haz_gdb, target_projection, facts_haz_fc_name, hazardous_fuels_table, sde_connection_file, target_schema)
    common_attributes_download_and_insert(target_projection, sde_connection_file, target_schema, cur, insert_table, hazardous_fuels_table)

    #MERGE
    nfpors_insert(cur, target_schema, insert_table)
    nfpors_fund_code(cur, target_schema, insert_table)
    nfpors_treatment_date(cur, target_schema, insert_table)

    # Insert FACTS entries and enter proper treatement dates
    hazardous_fuels_insert(cur, target_schema, insert_table)
    hazardous_fuels_treatment_date(cur, target_schema, insert_table)

    # Insert NFPORS, convert isbil Yes/No to fund_code 'BIL'/null
    fund_source_updates(cur, target_schema, insert_table)

    arcpy.management.RebuildIndexes(sde_connection_file, 'NO_SYSTEM', f'sweri.{target_schema}.{insert_table}_temp', 'ALL')

    create_treatment_points(target_schema, sde_connection_file, insert_table)
    rename_treatment_points(target_schema, sde_connection_file, cur, insert_table)

    # Does a series of renames
    # Backup -> backup_temp
    # Current treatment_index -> backup
    # Newly populated treatment_index_temp -> current treatment_index
    # backup_temp -> treatment_index_temp for next run
    treatment_index_renames(cur, target_schema, insert_table)