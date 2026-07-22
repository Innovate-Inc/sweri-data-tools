import datetime
from dotenv import load_dotenv
import os

os.environ["CRYPTOGRAPHY_OPENSSL_NO_LEGACY"] = "1"
import watchtower
from arcgis.gis import GIS

from sweri_utils.sql import connect_to_pg_db, add_column
from sweri_utils.download import service_to_postgres
from sweri_utils.hosted import hosted_upload_and_swizzle, hosted_upload_from_postgres, \
    delete_features_from_hosted_layer, get_feature_layer_from_item, verify_feature_count
from sweri_utils.sweri_logging import logging, log_this

logger = logging.getLogger(__name__)


@log_this
def import_current_fires_snapshot(current_fires_url, out_wkid, ogr_string, db_conn, schema):
    # Connect to NIFC WFIGS Current Wildfires Perimeters Service and Import to Database
    service_to_postgres(current_fires_url, '1=1', out_wkid, ogr_string, schema, 'current_fires_snapshot', db_conn)

@log_this
def makevalid_snapshot_shapes(db_conn, schema):
    cursor = db_conn.cursor()
    with db_conn.transaction():
        cursor.execute(f'''
            
            UPDATE {schema}.current_fires_snapshot
            SET shape = ST_MakeValid(shape)
            WHERE NOT ST_IsValid(shape);

        ''')

def return_ids(db_conn, query):
    # Formats returned ids for sql queries
    cursor = db_conn.cursor()

    with db_conn.transaction():
        cursor.execute(query)
        ids_list = [row[0] for row in cursor.fetchall()]
        ids_string = ','.join(f"'{id}'" for id in ids_list)
    return ids_string, ids_list


def id_set_to_sql_string(ids_set):
    if not ids_set:
        return ""

    # Handle numeric and string IDs differently
    if all(isinstance(id, (int, float)) for id in ids_set):
        # No quotes for numeric IDs
        ids_string = ','.join(str(id) for id in ids_set)
    else:
        # Wrap string IDs in single quotes
        ids_string = ','.join(f"'{id}'" for id in ids_set)

    return ids_string


@log_this
def add_new_fires(schema, db_conn, start_date):
    add_ids_query = f'''
    
        SELECT poly_irwinid FROM {schema}.current_fires_snapshot 
        WHERE poly_irwinid NOT IN 
        (        
        SELECT poly_irwinid 
        FROM {schema}.daily_progression 
        WHERE removal_date IS NULL
        );
        
    '''
    add_ids_string, add_ids_list = return_ids(db_conn, add_ids_query)

    if len(add_ids_string) > 0:
        insert_fires(db_conn, schema, start_date, add_ids_string)
        logger.info(f'Added Fires : {add_ids_string}')
    else:
        logger.info(f'No new fires to add')
    return add_ids_list


@log_this
def notate_removed_fires(schema, db_conn, removal_date):
    removed_ids_query = f'''
    
        SELECT poly_irwinid from {schema}.daily_progression
        WHERE removal_date is null
        AND
        poly_irwinid NOT IN
        (
            SELECT poly_irwinid
            FROM {schema}.current_fires_snapshot
        );
        
    '''
    removed_ids_string, removed_ids_list = return_ids(db_conn, removed_ids_query)

    if len(removed_ids_string) > 0:
        update_removal_date(db_conn, schema, removal_date, removed_ids_string)
        logger.info(f'removal_date set to {removal_date} for : {removed_ids_string}')
    else:
        logger.info(f'No fire removals to notate')

    return removed_ids_list


@log_this
def update_modified_fires(schema, db_conn, start_date, removal_date):
    modified_ids_query = f'''
    
        SELECT dp.poly_irwinid  
        FROM {schema}.daily_progression dp
        JOIN {schema}.current_fires_snapshot cf
        ON dp.poly_irwinid = cf.poly_irwinid
        WHERE
        dp.removal_date IS NULL
        AND
        cf.attr_modifiedondatetime_dt > dp.attr_modifiedondatetime_dt
        
    '''
    modified_ids_string, modified_ids_list = return_ids(db_conn, modified_ids_query)

    if len(modified_ids_string) > 0:
        update_removal_date(db_conn, schema, removal_date, modified_ids_string)
        insert_fires(db_conn, schema, start_date, modified_ids_string)
        logger.info(f'Modified fires {modified_ids_string}')
    else:
        logger.info(f'No fires modified since last run')

    return modified_ids_list


@log_this
def insert_fires(db_conn, schema, start_date, id_list):
    # insert from current fires into daily progression 
    # start_date set to current_date for all new entries
    cursor = db_conn.cursor()
    common_fields = '''
    poly_sourceoid, poly_incidentname, poly_featurecategory, poly_mapmethod, 
    poly_gisacres, poly_createdate, poly_datecurrent, poly_polygondatetime, poly_irwinid, 
    poly_forid, poly_acres_autocalc, poly_sourceglobalid, poly_source, attr_sourceoid, attr_abcdmisc,
    attr_adspermissionstate, attr_calculatedacres, attr_containmentdatetime, attr_controldatetime,
    attr_createdbysystem, attr_incidentsize, attr_discoveryacres, attr_dispatchcenterid, 
    attr_estimatedcosttodate, attr_finalacres, attr_ffreportapprovedbytitle, 
    attr_ffreportapprovedbyunit, attr_ffreportapproveddate, attr_firebehaviorgeneral, 
    attr_firebehaviorgeneral1, attr_firebehaviorgeneral2, attr_firebehaviorgeneral3, 
    attr_firecause, attr_firecausegeneral, attr_firecausespecific, attr_firecode, 
    attr_firedepartmentid, attr_firediscoverydatetime, attr_firemgmtcomplexity, 
    attr_fireoutdatetime, attr_firestrategyconfinepercent, attr_firestrategyfullsuppprcnt, 
    attr_firestrategymonitorpercent, attr_firestrategypointzoneprcnt, attr_fsjobcode, 
    attr_fsoverridecode, attr_gacc, attr_ics209reportdatetime, attr_ics209rptfortimeperiodfrom, 
    attr_ics209rptfortimeperiodto, attr_ics209reportstatus, attr_incidentmanagementorg,
    attr_incidentname, attr_incidentshortdescription, attr_incidenttypecategory, 
    attr_incidenttypekind, attr_initiallatitude, attr_initiallongitude, 
    attr_initialresponseacres, attr_initialresponsedatetime, attr_irwinid, 
    attr_isfirecauseinvestigated, attr_isfirecoderequested, attr_isfsassisted, 
    attr_ismultijurisdictional, attr_isreimbursable, attr_istrespass, attr_isunifiedcommand, 
    attr_localincidentidentifier, attr_modifiedbysystem, attr_percentcontained, 
    attr_percentperimtobecontained, attr_poocity, attr_poocounty, attr_poodispatchcenterid, 
    attr_poofips, attr_poojurisdictionalagency, attr_poojurisdictionalunit, 
    attr_poojurisdictunitparentunit, attr_poolandownercategory, attr_poolandownerkind, 
    attr_poolegaldescprincipalmerid, attr_poolegaldescqtr, attr_poolegaldescqtrqtr, 
    attr_poolegaldescrange, attr_poolegaldescsection, attr_poolegaldesctownship, 
    attr_poopredictiveserviceareaid, attr_pooprotectingagency, attr_pooprotectingunit, 
    attr_poostate, attr_predominantfuelgroup, attr_predominantfuelmodel, attr_primaryfuelmodel, 
    attr_secondaryfuelmodel, attr_totalincidentpersonnel, attr_uniquefireidentifier, attr_forid, 
    attr_wfdssdecisionstatus, attr_estimatedfinalcost, attr_organizationalassessment, 
    attr_stratdecisionpublishdate, attr_createdondatetime_dt, attr_modifiedondatetime_dt, 
    attr_source, attr_iscpxchild, attr_cpxname, attr_cpxid, attr_sourceglobalid
    '''
    with db_conn.transaction():
        cursor.execute(f'''
                                    
            INSERT INTO {schema}.daily_progression
            (
            objectid, {common_fields},
            globalid, removal_date, start_date, gdb_geomattr_data, shape
            ) 
        
            SELECT
        
            sde.next_rowid('{schema}', 'daily_progression'), {common_fields}, 
            sde.next_globalid(), NULL, '{start_date}', gdb_geomattr_data, shape
        
            FROM {schema}.current_fires_snapshot cf
            WHERE cf.poly_irwinid IN (
                {id_list}
            );
    
        ''')


@log_this
def update_removal_date(db_conn, schema, removal_date, id_list):
    cursor = db_conn.cursor()
    with db_conn.transaction():
        cursor.execute(f'''
                                    
            UPDATE {schema}.daily_progression
            SET removal_date = '{removal_date}'
            WHERE removal_date is null
            AND poly_irwinid IN (
                {id_list}
        );
    
        ''')

@log_this
def update_global_date_values(db_conn, schema, id_list, today_removal_date):
    # Updates global_start_date and global_removal_date of all fire progressions in the provided id list
    # Since progressions with null removal_dates are still active, global_removal_date for such fires is set to today
    if len(id_list) > 0:
        cursor = db_conn.cursor()
        with db_conn.transaction():
            cursor.execute(f'''
            
                WITH global_dates AS (
                    SELECT poly_irwinid,
                           MIN(start_date) AS global_start_date,
                           MAX(COALESCE(removal_date,'{today_removal_date}'))
                AS global_removal_date
                    FROM {schema}.daily_progression
                    GROUP BY poly_irwinid
                )
                UPDATE {schema}.daily_progression w
                SET global_start_date = g.global_start_date,
                    global_removal_date = g.global_removal_date
                FROM global_dates g
                WHERE w.poly_irwinid = g.poly_irwinid
                  AND w.poly_irwinid IN ({id_list});
      
        ''')
    logger.info(f'Updated global_start_date and global_removal_date for {len(id_list)} fires')

@log_this
def detect_and_update_fire_complexes(db_conn, schema, iteration_limit):
    """
    If two progressions have geographic intersection and date overlap, they are considered a complex

    If two or more progressions are part of the same complex, the global_start_date is set to the earliest start date
    in the complex and the global_removal_date is set to the latest removal date in the complex.
    """

    # Select query looks for a single row that needs dates updated
    select_query = f"""
    
        WITH complex_dates AS (
            SELECT -- Find all progressions that intersect geographically, and have overlapping date ranges
                a.poly_irwinid AS wildfire_id,                                 
                MIN(b.global_start_date) AS new_start_date,                   
                MAX(b.global_removal_date) AS new_removal_date                
            FROM {schema}.daily_progression a
            JOIN {schema}.daily_progression b
              ON ST_Intersects(a.shape, b.shape)                             
             AND a.global_start_date <= b.global_removal_date                
             AND b.global_start_date <= a.global_removal_date
            GROUP BY a.poly_irwinid                                          
        )
        SELECT * -- Find if any of the intersecting progressions                                                              
        FROM {schema}.daily_progression w
        INNER JOIN complex_dates c
          ON w.poly_irwinid = c.wildfire_id                                  
        WHERE (w.global_start_date != c.new_start_date                       
               OR w.global_removal_date != c.new_removal_date)
        LIMIT 1;    
                                                                 
    """

    # update_query expands global_start_date and global_end_date of fires in all complexes
    update_query = f"""
        WITH complex_dates AS (
            SELECT -- Select all progressions that intersect geographically, and have overlapping date ranges
                a.poly_irwinid AS wildfire_id,                                 
                MIN(b.global_start_date) AS new_start_date,                   
                MAX(b.global_removal_date) AS new_removal_date               
            FROM {schema}.daily_progression a
            JOIN {schema}.daily_progression b
              ON ST_Intersects(a.shape, b.shape)                             
             AND a.global_start_date <= b.global_removal_date                
             AND b.global_start_date <= a.global_removal_date
            GROUP BY a.poly_irwinid                                           
        )
        UPDATE {schema}.daily_progression w -- Expand global dates of all features to max and min of all fires in complex
        SET 
            global_start_date = c.new_start_date,                            
            global_removal_date = c.new_removal_date                      
        FROM complex_dates c
        WHERE w.poly_irwinid = c.wildfire_id                                
          AND (w.global_start_date != c.new_start_date                      
               OR w.global_removal_date != c.new_removal_date);
    """

    rows_to_update = True  # Set to true to begin the loop
    iteration_count = 0
    updated_ids = []
    cursor = db_conn.cursor()


    try:
        while rows_to_update:

            # select query runs to see if any complexes need updating
            logger.info(f"Checking for complex updates. Iteration {iteration_count}...")
            with db_conn.transaction():
                cursor.execute(select_query)

                # If a single row needs to be updated, rows_to_update is set to true
                rows_to_update = cursor.fetchone() is not None

                if rows_to_update:
                    iteration_ids = [row[0] for row in cursor.fetchall()]
                    updated_ids.extend(iteration_ids)
                    # If rows are found, execute the update query
                    logger.info(f"Rows found to update. Executing update query...")
                    with db_conn.transaction():
                        cursor.execute(update_query)

                        rows_updated = cursor.rowcount
                        logger.info(f"Number of rows updated in this iteration: {rows_updated}")
                    iteration_count += 1
                else:
                    logger.info(f"No rows left to update. Process complete after {iteration_count} iterations.")

            if iteration_count > iteration_limit:
                break

        return updated_ids
    except Exception as e:
        print(f"Error during complex update loop: {e}")
        raise

def update_and_verify_progressions(gis_url, gis_user, gis_password, feature_layer_id, where,
                                   target_schema, daily_progression_table,
                                   max_points_before_single_geom_chunk, chunk, conn):

    # Delete all progressions that were updated
    delete_features_from_hosted_layer(gis_url, gis_user, gis_password, feature_layer_id, where)
    # Add updated progressions
    hosted_upload_from_postgres(gis_url, gis_user, gis_password, feature_layer_id, target_schema,
                                daily_progression_table,
                                max_points_before_single_geom_chunk, chunk, where=where)

    # Verify Count
    feature_layer = get_feature_layer_from_item(gis_url, gis_user, gis_password, feature_layer_id)
    verify_feature_count(conn, target_schema, daily_progression_table, feature_layer)


def run_daily_progressions(wfigs_current_fires_url, wkid, ogr_db_string, conn, target_schema,
                           gis_url, gis_user, gis_password,
                           daily_progression_view_id, feature_layer_id,
                           run_sync_hosted_upload):
    #start date and removal date 1 second apart to prevent overlap between old and new polygons
    current_time = datetime.datetime.now()
    one_second_ago = current_time - datetime.timedelta(seconds=1)

    # Time strings
    current_time_str = current_time.strftime('%Y-%m-%d %H:%M:%S')
    one_second_ago_str = one_second_ago.strftime('%Y-%m-%d %H:%M:%S')

    daily_progression_table = 'daily_progression'

    chunk = 1000
    max_points_before_single_geom_chunk = 10000
    complex_iteration_limit = int(os.getenv('COMPLEX_ITERATION_LIMIT', 50))

    # import current fires layer into postgres
    import_current_fires_snapshot(wfigs_current_fires_url, wkid, ogr_db_string, conn, target_schema)
    makevalid_snapshot_shapes(conn, target_schema)

    # add new fires from current fires into daily progression
    added_ids = add_new_fires(target_schema, conn, current_time_str)

    # set removal date to current date for fires removed from current fires since last update
    removed_ids = notate_removed_fires(target_schema, conn, one_second_ago_str)

    # update entries modified since last run (inactivate old, add new)
    modified_ids = update_modified_fires(target_schema, conn, current_time_str, one_second_ago_str)

    # update global dates on all fires modified this run
    all_ids = set(added_ids + removed_ids + modified_ids)
    all_ids_string = ','.join(f"'{id}'" for id in all_ids)

    if len(all_ids) > 0:
        update_global_date_values(conn, target_schema, all_ids_string, one_second_ago_str)

        # expand global dates that are part of complexes, return ids of
        updated_complex_ids = detect_and_update_fire_complexes(conn, target_schema, complex_iteration_limit)

        all_ids.update(updated_complex_ids)
        all_ids_string = ','.join(f"'{id}'" for id in all_ids)

        where = f"poly_irwinid IN ({all_ids_string})"

        update_and_verify_progressions(gis_url, gis_user, gis_password, feature_layer_id, where,
                                       target_schema, daily_progression_table,
                                       max_points_before_single_geom_chunk, chunk, conn)
    conn.close()

if __name__ == '__main__':
    load_dotenv()

    target_schema = os.getenv('SCHEMA')
    wfigs_current_fires_url = os.getenv('CURRENT_FIRES')
    wkid = 4326
    conn = connect_to_pg_db(os.getenv('DB_HOST'), os.getenv('DB_PORT'), os.getenv('DB_NAME'),
                            os.getenv('DB_USER'), os.getenv('DB_PASSWORD'))
    ogr_db_string = f"PG:dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')} port={os.getenv('DB_PORT')} host={os.getenv('DB_HOST')}"

    # Hosted upload variables
    root_url = os.getenv('ESRI_ROOT_URL')
    gis_url = os.getenv("ESRI_PORTAL_URL")
    gis_user = os.getenv("ESRI_USER")
    gis_password = os.getenv("ESRI_PW")
    run_sync_hosted_upload = os.getenv('DAILY_PROG_RUN_SYNC_HOSTED_UPLOAD').lower() == 'true'

    daily_progression_data_ids = os.getenv('DAILY_PROG_TEST_ID')
    daily_progression_view_id = os.getenv('DAILY_PROGRESSION_VIEW_ID')

    run_daily_progressions(wfigs_current_fires_url, wkid, ogr_db_string, conn, target_schema,
                           gis_url, gis_user, gis_password,
                           daily_progression_view_id, daily_progression_data_ids,
                           run_sync_hosted_upload)


