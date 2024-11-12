import arcpy
import os
os.environ["CRYPTOGRAPHY_OPENSSL_NO_LEGACY"]="1"
from dotenv import load_dotenv
import logging
import watchtower

from sweri_utils.sql import rename_postgres_table
from sweri_utils.download import get_ids, fetch_and_create_featureclass
from sweri_utils.files import download_file_from_url, extract_and_remove_zip_file, gdb_to_postgres

logger = logging.getLogger(__name__)
logging.basicConfig( format='%(asctime)s %(levelname)-8s %(message)s',filename='./treatment_index.log', encoding='utf-8', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
logger.addHandler(watchtower.CloudWatchLogHandler())

def update_nfpors(connection, schema, sde_file, wkid, exclusion_ids, chunk_size = 70):

    max_modifiedon = connection.execute(f'select max(modifiedon) from {schema}.nfpors;')
    logger.info(f'max datetime in NFPORS: {max_modifiedon}')

    nfpors_url = os.getenv('NFPORS_URL')
    nfpors_postgres = os.path.join(sde_file, f'sweri.{schema}.nfpors')
    nfpors_additions_postgres = os.path.join(sde_file, f'sweri.{schema}.nfpors_additions')
    where_clause = f"modifiedon > TIMESTAMP '{max_modifiedon}'"

    exlusion_ids_tuple = tuple(exclusion_ids.split(",")) if len(exclusion_ids) > 0 else tuple()
    if len(exlusion_ids_tuple) > 0:
        where_clause += f' and objectid not in ({",".join(exlusion_ids_tuple)})'

    # fetch_and_create_featureclass returns an exception if no features are 
    # returned, in that case, nfpors is up to date, log exception and continue
    try:
        nfpors_additions_fc = fetch_and_create_featureclass(nfpors_url, where_clause, arcpy.env.scratchGDB, 'nfpors_additions', geometry=None, geom_type=None, out_sr=wkid,
                                    out_fields=None, chunk_size = chunk_size)
        logger.info('Uploading additions to postgress')
        arcpy.conversion.FeatureClassToGeodatabase(nfpors_additions_fc, sde_connection_file)
        logger.info("additions uploaded to postgrees")

        logger.info(f'inserting {nfpors_additions_postgres} into {nfpors_postgres}')
        insert_nfpors_additions(connection, schema)
        logger.info("additions appended to nfpors")
    except Exception as e:
        logger.error(e)
        pass

    logger.info('NFPORS up to date')

def insert_nfpors_additions(connection, schema):
    common_fields = '''
        trt_unt_id, local_id, col_date, trt_status, col_meth,
        comments, gis_acres, pstatus, modifiedon, createdon, 
        cent_lat, cent_lon, userid, st_abbr, cong_dist, cnty_fips,
        trt_nm, fy, plan_acc_ac, act_acc_ac, act_init_dt, act_comp_dt,
        nfporsfid, trt_id_db, type_name, cat_nm, trt_statnm, col_methnm,
        plan_int_dt, unit_id, agency, trt_id, created_by, edited_by,
        projectname, regionname, projectid, keypointarea, unitname,
        deptname, countyname, statename, regioncode, districtname,
        isbil, bilfunding, gdb_geomattr_data, shape
    '''

    connection.startTransaction()
    connection.execute(f'''
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
    connection.commitTransaction()

def nfpors_insert(connection, schema):
    connection.startTransaction()
    connection.execute(f'''
                   
    INSERT INTO {schema}.treatment_index_facts_nfpors_temp(

        objectid, 
        name,  date_current, actual_completion_date, 
        acres, type, category, fund_code, 
        identifier_database, unique_id,
        state, treatment_date, date_source, 
        shape, globalid
    )
    SELECT

        sde.next_rowid('{schema}', 'treatment_index_facts_nfpors_temp'),
        trt_nm AS name, modifiedon AS date_current, act_comp_dt AS actual_completion_date,
        gis_acres AS acres, type_name AS type, cat_nm AS category, isbil as fund_code,
        'NFPORS' AS identifier_database, CONCAT(nfporsfid,'-',trt_id) AS unique_id,
        st_abbr AS state, act_comp_dt as treatment_date, 'act_comp_dt' as date_source,
        shape, sde.next_globalid()

    FROM {schema}.nfpors;
    ''')
    connection.commitTransaction()

    logger.info(f'NFPORS entries inserted into {schema}.treatment_index_facts_nfpors_temp')

def facts_insert(connection, schema):
    connection.startTransaction()
    connection.execute(f'''
                   
    INSERT INTO {schema}.treatment_index_facts_nfpors_temp(

        objectid, name, 
        date_current, actual_completion_date, acres, 
        type, category, fund_code, cost_per_uom,
        identifier_database, unique_id,
        uom, state, activity, treatment_date,
        date_source, shape, globalid

    )
    SELECT

        sde.next_rowid('{schema}', 'treatment_index_facts_nfpors_temp'), activity_sub_unit_name AS name,
        etl_modified_date_haz AS date_current, date_completed AS actual_completion_date, gis_acres AS acres,
        treatment_type AS type, cat_nm AS category, fund_code, cost_per_uom,
        'FACTS Hazardous Fuels' AS identifier_database, activity_cn AS unique_id,
        uom, state_abbr AS state, activity, date_completed as treatment_date,
        'date_completed' as date_source, shape, sde.next_globalid()
        
    FROM {schema}.facts_haz_3857_2;
    
    ''')
    connection.commitTransaction()

    logger.info(f'FACTS entries inserted into {schema}.treatment_index_facts_nfpors_temp')
    #FACTS Insert Complete 

def facts_treatment_date(connection, schema):
    connection.startTransaction()
    connection.execute(f'''
        UPDATE {schema}.treatment_index_facts_nfpors_temp t
        SET treatment_date = f.date_planned,
        date_source = 'date_planned'
        FROM {schema}.facts_haz_3857_2 f
        WHERE t.identifier_database = 'FACTS Hazardous Fuels'
        AND t.treatment_date is null
        AND t.unique_id = f.activity_cn;
    ''')
    connection.commitTransaction()
    logger.info(f'updated treatment_date for FACTS Hazardous Fuels entries in {schema}.treatment_index_facts_nfpors_temp')

def nfpors_fund_code(connection, schema):

    connection.startTransaction()
    connection.execute(f'''   
        UPDATE {schema}.treatment_index_facts_nfpors_temp
        SET fund_code = null
        WHERE fund_code = 'No';        
    ''')
    connection.commitTransaction()

    connection.startTransaction()
    connection.execute(f'''   
        UPDATE {schema}.treatment_index_facts_nfpors_temp
        SET fund_code = 'BIL'
        WHERE fund_code = 'Yes';
    ''')
    connection.commitTransaction()

    logger.info(f'updated treatment_date for NFPORS entries in {schema}.treatment_index_facts_nfpors_temp')


def nfpors_treatment_date(connection, schema):
    connection.startTransaction()
    connection.execute(f'''
        UPDATE {schema}.treatment_index_facts_nfpors_temp t
        SET treatment_date = n.plan_int_dt,
        date_source = 'plan_int_dt'
        FROM {schema}.nfpors n
        WHERE t.identifier_database = 'NFPORS'
        AND t.treatment_date is null
        AND t.unique_id = CONCAT(n.nfporsfid,'-',n.trt_id);
    ''')
    connection.commitTransaction()

    connection.startTransaction()
    connection.execute(f'''
        UPDATE {schema}.treatment_index_facts_nfpors_temp t
        SET treatment_date = n.col_date,
        date_source = 'col_date'
        FROM {schema}.nfpors n
        WHERE t.identifier_database = 'NFPORS'
        AND t.treatment_date is null
        AND t.unique_id = CONCAT(n.nfporsfid,'-',n.trt_id);
    ''')
    connection.commitTransaction()

    logger.info(f'updated treatment_date for NFPORS entries in {schema}.treatment_index_facts_nfpors_temp')

def fund_source_updates(connection, schema):
    connection.startTransaction()
    connection.execute(f'''
        UPDATE {schema}.treatment_index_facts_nfpors_temp 
        SET fund_source = 'Multiple'
        WHERE fund_code LIKE '%,%';
    ''')
    connection.commitTransaction()

    connection.startTransaction()
    connection.execute(f'''
        UPDATE {schema}.treatment_index_facts_nfpors_temp 
        SET fund_source = 'No Funding Code'
        WHERE fund_code is null;
    ''')
    connection.commitTransaction()

    connection.startTransaction()
    connection.execute(f'''
        UPDATE {schema}.treatment_index_facts_nfpors_temp ti
        SET fund_source = lt.fund_source
        FROM {schema}.fund_source_lookup lt
        WHERE ti.fund_code = lt.fund_code
        AND ti.fund_source IS null;
    ''')
    connection.commitTransaction()

    connection.startTransaction()
    connection.execute(f'''
        UPDATE {schema}.treatment_index_facts_nfpors_temp
        SET fund_source = 'Other'
        WHERE fund_source IS null 
        AND
        fund_code IS NOT null;
    ''')
    connection.commitTransaction()

    logger.info(f'updated fund_source in {schema}.treatment_index_facts_nfpors_temp')


def treatment_index_renames(connection, schema):
    # backup to backup temp
    rename_postgres_table(connection, schema, 'treatment_index_facts_nfpors_backup', 'treatment_index_facts_nfpors_backup_temp')
    logger.info(f'{schema}.treatment_index_facts_nfpors_backup renamed to {schema}.treatment_index_facts_nfpors_backup_temp')

    # current to backup
    rename_postgres_table(connection, schema, 'treatment_index_facts_nfpors', 'treatment_index_facts_nfpors_backup')
    logger.info(f'{schema}.treatment_index_facts_nfpors renamed to {schema}.treatment_index_facts_nfpors_backup')

    # temp to current
    rename_postgres_table(connection, schema, 'treatment_index_facts_nfpors_temp', 'treatment_index_facts_nfpors')
    logger.info(f'{schema}.treatment_index_facts_nfpors_temp renamed to {schema}.treatment_index_facts_nfpors')

    #backup temp to temp
    rename_postgres_table(connection, schema, 'treatment_index_facts_nfpors_backup_temp', 'treatment_index_facts_nfpors_temp')
    logger.info(f'{schema}.treatment_index_facts_nfpors_backup_temp renamed to {schema}.treatment_index_facts_nfpors_temp')

def create_treatment_points(schema, sde_file):
    temp_polygons = os.path.join(sde_file, f"sweri.{schema}.treatment_index_facts_nfpors_temp")
    temp_points = os.path.join(sde_file, f"sweri.{schema}.treatment_index_facts_nfpors_points_temp")

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



def rename_treatment_points(schema, sde_file, connection):

    # rename current table to backup
    rename_postgres_table(connection, schema, 'treatment_index_facts_nfpors_points', 'treatment_index_facts_nfpors_points_backup')
    logger.info(f'{schema}.treatment_index_facts_nfpors_points renamed to {schema}.treatment_index_facts_nfpors_points_backup')

    # rename temp to current table
    rename_postgres_table(connection, schema, 'treatment_index_facts_nfpors_points_temp', 'treatment_index_facts_nfpors_points')
    logger.info(f'{schema}.treatment_index_facts_nfpors_points_temp renamed to {schema}.treatment_index_facts_nfpors_points')

    # rename backup to temp so we can delete it
    rename_postgres_table(connection, schema, 'treatment_index_facts_nfpors_points_backup', 'treatment_index_facts_nfpors_points_temp')
    logger.info(f'{schema}.treatment_index_facts_nfpors_points_backup renamed to {schema}.treatment_index_facts_nfpors_points_temp')

    # clean up so we can create points again later
    arcpy.management.Delete(os.path.join(sde_file, f"sweri.{schema}.treatment_index_facts_nfpors_points_temp"))

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
    facts_haz_pg_table = 'facts_haz_3857_2'


    
    con = arcpy.ArcSDESQLExecute(sde_connection_file)

    update_nfpors(con, target_schema, sde_connection_file, out_wkid, exluded_ids)
    #gdb_to_postgres here updates FACTS Hazardous Fuels in our Database
    gdb_to_postgres(facts_haz_gdb_url, facts_haz_gdb, target_projection, facts_haz_fc_name, facts_haz_pg_table, sde_connection_file, target_schema)

    #Clear out overall treatment index insert destination table while maintaining globalids and versioning
    con.execute(f'TRUNCATE {target_schema}.treatment_index_facts_nfpors_temp;')

    #MERGE
    nfpors_insert(con, target_schema)
    nfpors_fund_code(con, target_schema)
    nfpors_treatment_date(con, target_schema)

    # Insert FACTS entries and enter proper treatement dates
    facts_insert(con, target_schema)
    facts_treatment_date(con, target_schema)

    # Insert NFPORS, convert isbil Yes/No to fund_code 'BIL'/null
    fund_source_updates(con, target_schema)

    arcpy.management.RebuildIndexes(sde_connection_file, 'NO_SYSTEM', f'sweri.{target_schema}.treatment_index_facts_nfpors_temp', 'ALL')

    create_treatment_points(target_schema, sde_connection_file)
    rename_treatment_points(target_schema, sde_connection_file, con)

    # Does a series of renames
    # Backup -> backup_temp
    # Current treatment_index -> backup
    # Newly populated treatment_index_temp -> current treatment_index
    # backup_temp -> treatment_index_temp for next run
    treatment_index_renames(con, target_schema)
