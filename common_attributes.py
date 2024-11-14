from sweri_utils.files import gdb_to_postgres
import arcpy
import os
os.environ["CRYPTOGRAPHY_OPENSSL_NO_LEGACY"]="1"
from dotenv import load_dotenv
import logging
import re
import watchtower

logger = logging.getLogger(__name__)
logging.basicConfig( format='%(asctime)s %(levelname)-8s %(message)s',filename='./common_attributes.log', encoding='utf-8', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
# logger.addHandler(watchtower.CloudWatchLogHandler())

def add_fields_and_indexes(feature_class, region):
    arcpy.management.AddField(feature_class,'included', 'TEXT')
    arcpy.management.AddIndex(feature_class, 'included', f'included_idx_{region}', ascending="ASCENDING")

    arcpy.management.AddField(feature_class,'r2', 'TEXT')
    arcpy.management.AddIndex(feature_class, 'r2', f'r2_idx_{region}', ascending="ASCENDING")

    arcpy.management.AddField(feature_class,'r3', 'TEXT')
    arcpy.management.AddIndex(feature_class, 'r3', f'r3_idx_{region}',  ascending="ASCENDING")


    arcpy.management.AddField(feature_class,'r4', 'TEXT')
    arcpy.management.AddIndex(feature_class, 'r4', f'r4_idx_{region}', ascending="ASCENDING")

    arcpy.management.AddField(feature_class,'r5', 'TEXT')
    arcpy.management.AddIndex(feature_class, 'r5', f'r5_idx_{region}',  ascending="ASCENDING")

    arcpy.management.AddField(feature_class,'r6', 'TEXT')
    arcpy.management.AddIndex(feature_class, 'r6', f'r6_idx_{region}',  ascending="ASCENDING")


    arcpy.management.AddIndex(feature_class, 'event_cn', f'event_cn_idx_{region}', unique="UNIQUE", ascending="ASCENDING")
    arcpy.management.AddIndex(feature_class, 'gis_acres', f'gis_acres_idx_{region}', ascending="ASCENDING")
    arcpy.management.AddIndex(feature_class, 'activity', f'activity_idx_{region}', ascending="ASCENDING")
    arcpy.management.AddIndex(feature_class, 'equipment', f'equipment_idx_{region}', ascending="ASCENDING")
    arcpy.management.AddIndex(feature_class, 'method', f'method_idx_{region}', ascending="ASCENDING")
       
def exclude_facts_hazardous_fuels(connection, schema, table, facts_haz_table):
    # Do Not Included Entries Already Being Included via Hazardous Fuels
    connection.startTransaction()
    connection.execute(f'''
                   
    DELETE FROM {schema}.{table}
    WHERE 
    event_cn IN(
        SELECT activity_cn FROM {schema}.{facts_haz_table}
    )
          
    ''')
    connection.commitTransaction()
    logging.info(f"deleted {schema}.{table} entries that are also in FACTS Hazardous Fuels")

def exclude_by_acreage(connection, schema, table):
    connection.startTransaction()
    connection.execute(f'''
                   
    DELETE FROM {schema}.{table}
    WHERE
    gis_acres <= 5 OR
    gis_acres IS NULL; 
    
    ''')
    connection.commitTransaction()
    logging.info(f"deleted Entries <= 5 acres {schema}.{table}")

def trim_whitespace(connection, schema, table):
    connection.startTransaction()
    connection.execute(f'''
                   
    UPDATE {schema}.{table}
    SET
    activity = TRIM(activity),
    method = TRIM(method),
    equipment = TRIM(equipment);
    
    ''')
    connection.commitTransaction()
    logging.info(f"removed white space from activity, method, and equipment in {schema}.{table}")

def include_logging_activities(connection, schema, table):
    connection.startTransaction()
    connection.execute(f'''
                   
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
    connection.commitTransaction()
    logging.info(f"included set to 'yes' for logging activities with proper methods and equipment in {schema}.{table}")
    
def include_fire_activites(connection, schema, table):
    connection.startTransaction()
    connection.execute(f'''
                   
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
    connection.commitTransaction()
    logging.info(f"included set to 'yes' for fire activities with proper methods and equipment in {schema}.{table}")

def include_fuel_activities(connection, schema, table):
    connection.startTransaction()
    connection.execute(f'''
                   
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
    connection.commitTransaction()
    logging.info(f"included set to 'yes' for fuel activities with proper methods and equipment in {schema}.{table}")

def special_exclusions(connection, schema, table):
    connection.startTransaction()
    connection.execute(f'''
                   
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
    connection.commitTransaction()
    logging.info(f"included set to 'no' for special exclusions in {schema}.{table}")

def include_other_activites(connection, schema, table):
    connection.startTransaction()
    connection.execute(f'''
                   
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
    connection.commitTransaction()
    logging.info(f"included set to 'yes' for other activities with proper methods and equipment in {schema}.{table}")

def set_included(connection, schema, table):
    connection.startTransaction()
    connection.execute(f'''
                   
    UPDATE {schema}.{table}
    SET included = 'yes'
    WHERE
    included is null
    AND
    r5 = 'PASS'
    OR
    ((r2 = 'PASS' OR r3 = 'PASS' OR r4 = 'PASS') AND r6 = 'PASS');
    
    ''')
    connection.commitTransaction()    

def common_attributes_insert(connection, schema, table):
    #Need to figure out state and dates outside of date completed
    connection.startTransaction()
    connection.execute(f'''
                   
    INSERT INTO {schema}.treatment_index_common_attributes(

        objectid, name, date_current, actual_completion_date, acres, 
        type, category, fund_code, cost_per_uom, identifier_database, 
        unique_id, uom, state, activity, treatment_date, date_source, 
        shape, globalid

    )
    SELECT

        sde.next_rowid('{schema}', 'treatment_index_common_attributes'),
        name AS name, act_modified_date AS date_current, 
        date_completed AS actual_completion_date, gis_acres AS acres, 
        nfpors_treatment AS type, nfpors_category AS category, fund_codes as fund_code, 
        cost_per_unit as cost_per_uom, 'FACTS Common Attributes' AS identifier_database, 
        event_cn AS unique_id, uom as uom, state_abbr AS state, activity as activity, 
        date_completed as treatment_date, 'date_completed' as date_source, shape, 
        sde.next_globalid()
        
    FROM {schema}.{table}
    WHERE included = 'yes'
    OR 
    included = 'haz';

    ''')
    connection.commitTransaction()
    logger.info(f"{schema}.{table} inserted into {schema}.treatment_index_common_attributes whre included = 'yes'")


if __name__ == '__main__':
    load_dotenv()
    out_wkid = 3857
    target_projection = arcpy.SpatialReference(out_wkid)

    sde_connection_file = os.getenv('SDE_FILE')
    target_schema = os.getenv('SCHEMA')
    common_attributes_url = os.getenv('COMMON_ATTRIBUTES_URL')
    common_attributes_fc_name = 'Actv_CommonAttribute_PL'
    hazardous_fuels_table = 'facts_haz_3857_2'

    con = arcpy.ArcSDESQLExecute(sde_connection_file)

    insert_table = f'treatment_index_common_attributes'
    insert_table_path = os.path.join(sde_connection_file, insert_table)

    con.execute(f'TRUNCATE {target_schema}.{insert_table}')

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
        print(url)
        region_number = re.sub("\D", "", url)
        table_name = f'common_attributes_region_{region_number}'
        gdb = f'Actv_CommonAttribute_PL_Region{region_number}.gdb'
        postgres_fc = os.path.join(sde_connection_file, table_name)

        gdb_to_postgres(url, gdb, target_projection, common_attributes_fc_name, table_name, sde_connection_file, target_schema)

        add_fields_and_indexes(postgres_fc, region_number) 

        exclude_by_acreage(con, target_schema, table_name)
        exclude_facts_hazardous_fuels(con, target_schema, table_name, hazardous_fuels_table)

        trim_whitespace(con, target_schema, table_name)

        include_logging_activities(con, target_schema, table_name)
        include_fire_activites(con, target_schema, table_name)
        include_fuel_activities(con, target_schema, table_name)
        special_exclusions(con, target_schema, table_name)
        include_other_activites(con, target_schema, table_name)

        set_included(con, target_schema, table_name)

        common_attributes_insert(con, target_schema, table_name)
        arcpy.management.Delete(postgres_fc)


