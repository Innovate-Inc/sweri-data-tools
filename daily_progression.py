from arcgis.features import FeatureLayer
import datetime
from dotenv import load_dotenv
import os
os.environ["CRYPTOGRAPHY_OPENSSL_NO_LEGACY"]="1"
import logging
import arcpy
import watchtower


logger = logging.getLogger(__name__)
logging.basicConfig( format='%(asctime)s %(levelname)-8s %(message)s',filename='./daily_progression.log', encoding='utf-8', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
logger.addHandler(watchtower.CloudWatchLogHandler())

def import_current_fires_snapshot(sde_file, schema, out_wkid):
    #Connect to NIFC WFIGS Current Wildfires Perimeters Service and Covert to Feature Layer
    current_fires_url = os.getenv('CURRENT_FIRES')
    current_fires = FeatureLayer(current_fires_url)
    current_fires_query = current_fires.query(where='1=1',  out_fields='*', return_geometry=True, out_sr=out_wkid)
    current_fires_fc = current_fires_query.save(arcpy.env.scratchGDB, 'current_fires_snapshot')
    current_fires_postgres = os.path.join(sde_file, f'sweri.{schema}.current_fires_snapshot')
    logger.info('current fires downloaded')

    if arcpy.Exists(current_fires_postgres):
                        arcpy.management.Delete(current_fires_postgres)
                        logger.info("current fires snapshot deleted")

    arcpy.conversion.FeatureClassToGeodatabase(current_fires_fc, sde_connection_file)
    arcpy.management.AddIndex(current_fires_postgres, 'poly_irwinid', 'irwinid_idx', 'NON_UNIQUE', 'ASCENDING') 
      
def return_ids(connection, query):
    # ArcSDEExecute returns boolean True when there are no ids,
    # a string when there is 1 id, and a list of 1 item lists
    #  when there are multiple ids

    ids_nested_list = connection.execute(query)
    if type(ids_nested_list) == str:
         logger.info(f'nested Id list: {ids_nested_list}')
         return f"'{ids_nested_list}'"
    elif type(ids_nested_list) == list:

        ids_list = [item[0] for item in ids_nested_list]
        ids_list_str  = ', '.join(f"'{id}'" for id in ids_list)
        logger.info(f'list of ids: {ids_list_str}')
        return ids_list_str
    else:
        logger.info('no ids')
        return ''

def add_new_fires(schema, connection, start_date):
    add_ids_query = f'''
    SELECT poly_irwinid from {schema}.current_fires_snapshot where poly_irwinid not in (        
    SELECT poly_irwinid 
    FROM {schema}.daily_progression 
    WHERE removal_date IS NULL
    );
    '''
    add_ids = return_ids(con, add_ids_query)

    if len(add_ids) > 0:
        insert_fires(connection, schema, start_date, add_ids)
        logger.info(f'Added Fires : {add_ids}')
    else:
        logger.info(f'No new fires to add')

def notate_removed_fires(schema, connection, removal_date):
    removed_ids_query = f'''
    SELECT poly_irwinid from {schema}.daily_progression
    WHERE removal_date is null
    AND
    poly_irwinid NOT IN(
        SELECT poly_irwinid
        FROM {schema}.current_fires_snapshot
    );
    '''
    removed_ids = return_ids(connection, removed_ids_query)

    if len(removed_ids) > 0:
        update_removal_date(connection, schema, removal_date, removed_ids)
        logger.info(f'removal_date set to {current_date} for : {removed_ids}')
    else:
        logger.info(f'No fire removals to notate')


def update_modified_fires(schema, connection, removal_date):
    modified_ids_query = f'''
    SELECT dp.poly_irwinid  
    FROM {schema}.daily_progression dp
    JOIN {schema}.current_fires_snapshot cf
    ON dp.poly_irwinid = cf.poly_irwinid
    WHERE
    dp.removal_date is null
    and
    cf.attr_modifiedondatetime_dt > dp.attr_modifiedondatetime_dt
    '''
    modified_ids = return_ids(connection, modified_ids_query)

    if len(modified_ids) > 0:
        update_removal_date(connection, schema, removal_date, modified_ids)
        insert_fires(connection, schema, removal_date, modified_ids)
        logger.info(f'Modified fires {modified_ids}')
    else:
        logger.info(f'No fires modified since last run')
    
    

def insert_fires(connection, schema, start_date, id_list):
    # insert from current fires into daily progression 
    # start_date set to current_date for all new entries
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
    
    connection.execute(f'''
                                
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

def update_removal_date(connection, schema, removal_date, id_list):
    
    connection.execute(f'''
                                
    UPDATE {schema}.daily_progression
    SET removal_date = '{removal_date}'
    WHERE removal_date is null
    AND poly_irwinid IN (
        {id_list}
    );

    ''')

if __name__ == '__main__':
    
    current_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    load_dotenv()
    arcpy.env.workspace = arcpy.env.scratchGDB
    arcpy.env.overwriteOutput = True
    sde_connection_file = os.getenv('SDE_FILE')
    target_schema = os.getenv('SCHEMA')
    wkid = 4326
    con = arcpy.ArcSDESQLExecute(sde_connection_file)

    #import current fires layer into postgres
    import_current_fires_snapshot(sde_connection_file, target_schema, wkid)

    #add new fires from current fires into daily progression
    add_new_fires(target_schema, con, current_date)

    #set removal date to current date for fires removed from current fires since last update
    notate_removed_fires(target_schema, con, current_date)

    #update entries modified since last run (inactivate old, add new)
    update_modified_fires(target_schema, con, current_date)