from sweri_utils.files import download_file_from_url, extract_and_remove_zip_file
import arcpy
import os
os.environ["CRYPTOGRAPHY_OPENSSL_NO_LEGACY"]="1"
from dotenv import load_dotenv
import logging
import shutil
import zipfile
import re
import watchtower

logger = logging.getLogger(__name__)
logging.basicConfig( format='%(asctime)s %(levelname)-8s %(message)s',filename='./common_attributes.log', encoding='utf-8', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
# logger.addHandler(watchtower.CloudWatchLogHandler())

def gdb_to_postgres(url, gdb_name, projection, fc_name, postgres_table_name, sde_file, schema):
    # Downloads a gdb with a single feature class
    # And uploads that featureclass to postgres
    zip_file = f'{postgres_table_name}.zip'

    #Download and extract gdb file
    logger.info(f'Downloading {url}')
    download_file_from_url(url, zip_file)

    logger.info(f'Extracting {zip_file}')
    extract_and_remove_zip_file(zip_file)

    #Set Workspace to Downloaded GDB and set paths for feature class and reprojection
    gdb_path = os.path.join(os.getcwd(),gdb_name)
    feature_class = os.path.join(gdb_path,fc_name)
    reprojected_fc = os.path.join(gdb_path, f'{postgres_table_name}')
    postgres_table_location = os.path.join(sde_file, f'sweri.{schema}.{postgres_table_name}')

    #Reproject layer
    logger.info(f'reprojecting {feature_class}')
    arcpy.Project_management(feature_class, reprojected_fc, projection)
    logger.info('layer reprojected')

    #Clear space in postgres for table
    if(arcpy.Exists(postgres_table_location)):
        arcpy.management.Delete(postgres_table_location)
        logger.info(f'{postgres_table_name} has been deleted')

    #Upload fc to postgres
    arcpy.conversion.FeatureClassToGeodatabase(reprojected_fc, sde_file)
    logger.info(f'{postgres_table_location} now in geodatabase')

    # #Remove gdb
    # shutil.rmtree(gdb_path)
    # logger.info(f'{gdb_path} deleted')

def add_postgres_column(connection, schema, postgres_table_name, column_name, data_type):
    connection.startTransaction()
    connection.execute(f'''
    ALTER TABLE {schema}.{postgres_table_name}
    ADD COLUMN {column_name} {data_type};
    ''')
    connection.commitTransaction()
    logging.info(f'{column_name} added to {schema}.{postgres_table_name}')


def facts_hazardous_fuels_exclusion(connection, schema, table, facts_haz_table):
    # Do Not Included Entries Already Being Included via Hazardous Fuels
    connection.execute(f'''
                   
    UPDATE {schema}.{table}
    SET included = 'No'
    WHERE 
    event_cn IN(
        SELECT activity_cn FROM {schema}.{facts_haz_table}
    )
    
    ''')
    logging.info(f'Hazardous Fuels Entries Excluded from {schema}.{table}')

def inclusion_rules():
    #TODO after Anson rules final
    test = 'test'

def common_attributes_insert(connection, schema, postgres_table_name):
    #Need to figure out state and dates outside of date completed
    connection.startTransaction()
    connection.execute(f'''
                   
    INSERT INTO {schema}.treatment_index_common_attributes(

        objectid, 
        name, 
        date_current,
        actual_completion_date, 
        acres, 
        type, 
        category, 
        fund_code, 
        cost_per_uom,
        identifier_database, 
        unique_id,
        uom, state, activity, 
        treatment_date,
        date_source, 
        shape, 
        globalid

    )
    SELECT

        sde.next_rowid('{schema}', 'treatment_index_common_attributes'),
        name AS name,
        act_modified_date AS date_current,
        date_completed AS actual_completion_date, 
        gis_acres AS acres,
        nfpors_treatment AS type, 
        nfpors_category AS category, 
        fund_codes as fund_code, 
        cost_per_unit as cost_per_uom,
        'FACTS Common Attributes' AS identifier_database, 
        event_cn AS unique_id,
        uom as uom, 
        state_abbr AS state, 
        activity as activity, 
        date_completed as treatment_date,
        'date_completed' as date_source, 
        shape, 
        sde.next_globalid()
        
    FROM {schema}.{postgres_table_name}
    WHERE included = 'yes';
    
    ''')
    connection.commitTransaction()

def inclusion_rules():
    rule_0()
    rule_1()
    rule_2()
    rule_3()
    rule_4()
    rule_5()
    rule_6()
    rule_7()
    rule_8()
    rule_9()
    rule_10()

def rule_0(connection, schema, postgres_table_name):
    activity_exclusions = '''('Certification', 'Reforestation Need Change', 'Examination',
        'Prescription', 'Diagnosis', 'Exam', 'Survey', 'Analysis', 'Delineation',
        'Monitoring', 'Data', '(FIA)', 'Inventory', 'Permanent Plot', 'Remote Sensing',
        'Administrative Changes', 'Cruising', 'Layout and Design', 'Cone Collection',
        'Seed Collection', 'seed collecting', 'Seed Storage', 'Seed Extraction',
        'Pollen', 'Scion', 'Cooler', 'Activity Review', 'TSI Need', 'Fences')'''
    
    connection.beginTransaction()
    connection.execute(f'''
    
    UPDATE {schema}.{postgres_table_name}
    SET included = 'no'
    WHERE activity in {activity_exclusions}
    AND included IS NULL;

    ''')
    connection.commitTransaction()

def rule_1(connection, schema, postgres_table_name):
    connection.beginTransaction()
    connection.execute(f'''
    
    UPDATE {schema}.{postgres_table_name}
    SET included = 'yes'
    WHERE fuels_keypoint_area in ('3', '6')
    AND included IS NULL;

    ''')
    connection.commitTransaction()

def rule_2(connection, schema, postgres_table_name):
    connection.beginTransaction()
    connection.execute(f'''
    
    UPDATE {schema}.{postgres_table_name}
    SET included = 'no'
    WHERE 
    included IS NULL
    AND
    (gis_acres <= 10 OR gis_acres IS NULL);
    
    ''')
    connection.commitTransaction()
    

def rule_3():
    print()

def rule_4():
    print()

def rule_5(connection, schema, postgres_table_name):
    connection.beginTransaction()
    connection.execute(f'''
    
    UPDATE {schema}.{postgres_table_name}
    set included = 'no'
    WHERE 
    included IS NULL
    AND
    activity ILIKE '%wildfire%';
    
    ''')
    connection.commitTransaction()

def rule_6(connection, schema, postgres_table_name):
    connection.beginTransaction()
    connection.execute(f'''
    
    UPDATE {schema}.{postgres_table_name}
    set included = 'no'
    WHERE 
    included IS NULL
    AND
    activity ILIKE '%fish%'
    AND
    equipment NOT ILIKE '%logging%';
    
    ''')
    connection.commitTransaction()

def rule_7(connection, schema, postgres_table_name):
    connection.beginTransaction()
    connection.execute(f'''
    
    UPDATE {schema}.{postgres_table_name}
    SET included = 'yes'
    WHERE 
    included IS NULL
    AND
    activity ILIKE '%clearcut%'
    AND
    method not ILIKE ('%inventory%'|'%Designation%'|'%Marking%');
    
    ''')
    connection.commitTransaction()

def rule_8(connection, schema, postgres_table_name):
    connection.beginTransaction()
    connection.execute(f'''
    
    UPDATE {schema}.{postgres_table_name}
    set included = 'yes'
    WHERE 
    included IS NULL
    AND
    activity ILIKE '%burn%'
    AND
    method ILIKE ('%Fire%'|'%Manual%'|'%No method%');
    
    ''')
    connection.commitTransaction()

def rule_9(connection, schema, postgres_table_name):
    connection.beginTransaction()
    connection.execute(f'''
    
    UPDATE {schema}.{postgres_table_name}
    SET included = 'yes'
    WHERE 
    included IS NULL
    AND
    activity IN ('Drip Torch', 'Terra torch', 'Verray torch', 
    'Ping pong balls', 'Aerial ignition device', 'Air Curtain Incinerator');
    
    ''')
    connection.commitTransaction()


def rule_10(connection, schema, postgres_table_name):
    connection.beginTransaction()
    connection.execute(f'''
    
    UPDATE {schema}.{postgres_table_name}
    set included = 'yes'
    WHERE 
    included IS NULL
    AND
    activity ILIKE ('%Commercial Thin%'|'%Precommercial Thin%'") 
    AND 
    method IN ('Manual', 'No method', 'Power hand', 'Mechanical', 
    'Logging Methods', 'Tractor Logging')
    AND 
    equipment IN ('No equipment', 'Chain saw', 'Feller Buncher', 
    'Rubber tired skidder logging', 'Tractor logging');
    
    ''')
    connection.commitTransaction()

if __name__ == '__main__':
    load_dotenv()
    out_wkid = 3857
    target_projection = arcpy.SpatialReference(out_wkid)
    sde_connection_file = os.getenv('SDE_FILE')
    target_schema = os.getenv('SCHEMA')
    common_attributes_url = os.getenv('COMMON_ATTRIBUTES_URL')
    common_attributes_fc_name = 'Actv_CommonAttribute_PL'
    table_name = 'common_attributes'
    hazardous_fuels_table = 'facts_haz_3857_2'
    con = arcpy.ArcSDESQLExecute(sde_connection_file)
    con.execute(f'TRUNCATE {target_schema}.treatment_index_common_attributes')

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
        gdb_to_postgres(url, gdb, target_projection, common_attributes_fc_name, table_name, sde_connection_file, target_schema)
        add_postgres_column(con, target_schema, table_name, 'included', 'TEXT')
        postgres_fc = os.path.join(sde_connection_file, table_name)

        logging.info('postgres column added')
        arcpy.management.AddIndex(postgres_fc, 'event_cn', f'event_cn_idx_{region_number}', unique="UNIQUE", ascending="ASCENDING")
        logging.info('index created on event_cn')

        arcpy.management.AddIndex(postgres_fc, 'method', f'method_idx_{region_number}', unique="NON_UNIQUE", ascending="ASCENDING")
        logging.info('index created on method')

        arcpy.management.AddIndex(postgres_fc, 'equipment', f'equipment_idx_{region_number}', unique="NON_UNIQUE", ascending="ASCENDING")
        logging.info('index created on equipment')

        arcpy.management.AddIndex(postgres_fc, 'activity', f'activity_idx_{region_number}', unique="NON_UNIQUE", ascending="ASCENDING")
        logging.info('index created on activity')
        
        arcpy.management.AddIndex(postgres_fc, 'gis_acres', f'activity_idx_{region_number}', unique="NON_UNIQUE", ascending="ASCENDING")
        logging.info('index created on activity')

        facts_hazardous_fuels_exclusion(con, target_schema, table_name, hazardous_fuels_table)

        inclusion_rules()

        common_attributes_insert(con, target_schema, table_name)
        
    #Strip Out Hazardous Fuels
    #Rules
    #Insert

