import datetime
from dotenv import load_dotenv
import os

os.environ["CRYPTOGRAPHY_OPENSSL_NO_LEGACY"] = "1"
import watchtower
from arcgis.gis import GIS

from sweri_utils.sql import connect_to_pg_db
from sweri_utils.download import service_to_postgres
from sweri_utils.hosted import hosted_upload_and_swizzle
from sweri_utils.sweri_logging import logging, log_this

logger = logging.getLogger(__name__)


@log_this
def import_current_fires_snapshot(current_fires_url, out_wkid, ogr_string, db_conn, schema):
    # Connect to NIFC WFIGS Current Wildfires Perimeters Service and Import to Database
    service_to_postgres(current_fires_url, '1=1', out_wkid, ogr_string, schema, 'current_fires_snapshot', db_conn)


def return_ids(db_conn, query):
    # Formats returned ids for sql queries
    cursor = db_conn.cursor()

    with db_conn.transaction():
        cursor.execute(query)
        ids_list = [row[0] for row in cursor.fetchall()]
        ids_string = ','.join(f"'{id}'" for id in ids_list)
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
    add_ids = return_ids(db_conn, add_ids_query)

    if len(add_ids) > 0:
        insert_fires(db_conn, schema, start_date, add_ids)
        logger.info(f'Added Fires : {add_ids}')
    else:
        logger.info(f'No new fires to add')


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
    removed_ids = return_ids(db_conn, removed_ids_query)

    if len(removed_ids) > 0:
        update_removal_date(db_conn, schema, removal_date, removed_ids)
        logger.info(f'removal_date set to {current_date} for : {removed_ids}')
    else:
        logger.info(f'No fire removals to notate')


@log_this
def update_modified_fires(schema, db_conn, removal_date):
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
    modified_ids = return_ids(db_conn, modified_ids_query)

    if len(modified_ids) > 0:
        update_removal_date(db_conn, schema, removal_date, modified_ids)
        insert_fires(db_conn, schema, removal_date, modified_ids)
        logger.info(f'Modified fires {modified_ids}')
    else:
        logger.info(f'No fires modified since last run')


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


if __name__ == '__main__':
    load_dotenv()
    current_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    target_schema = os.getenv('SCHEMA')
    wfigs_current_fires_url = os.getenv('CURRENT_FIRES')
    wkid = 4326
    conn = connect_to_pg_db(os.getenv('DB_HOST'), os.getenv('DB_PORT'), os.getenv('DB_NAME'),
                            os.getenv('DB_USER'), os.getenv('DB_PASSWORD'))
    ogr_db_string = f"PG:dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')} port={os.getenv('DB_PORT')} host={os.getenv('DB_HOST')}"

    # Hosted upload variables
    gis = GIS("https://gis.reshapewildfire.org/arcgis", os.getenv("ESRI_USER"), os.getenv("ESRI_PW"), expiration=120)
    daily_progression_data_ids = [os.getenv('DAILY_PROGRESSION_DATA_ID_1'), os.getenv('DAILY_PROGRESSION_DATA_ID_2')]
    daily_progression_view_id = os.getenv('DAILY_PROGRESSION_VIEW_ID')
    daily_progression_table = 'daily_progression'
    chunk = 1000
    start_objectid = 0

    # import current fires layer into postgres
    import_current_fires_snapshot(wfigs_current_fires_url, wkid, ogr_db_string, conn, target_schema)

    # add new fires from current fires into daily progression
    add_new_fires(target_schema, conn, current_date)

    # set removal date to current date for fires removed from current fires since last update
    notate_removed_fires(target_schema, conn, current_date)

    # update entries modified since last run (inactivate old, add new)
    update_modified_fires(target_schema, conn, current_date)

    # update hosted feature layer with upload and swizzle
    hosted_upload_and_swizzle(gis, daily_progression_view_id, daily_progression_data_ids, conn, target_schema,
                              daily_progression_table, chunk, start_objectid)
