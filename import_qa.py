from dotenv import load_dotenv
import os
os.environ["CRYPTOGRAPHY_OPENSSL_NO_LEGACY"]="1"
import requests
import json
import arcpy
import logging
import datetime

from sweri_utils.sql import rename_postgres_table, connect_to_pg_db
from sweri_utils.download import fetch_and_create_featureclass, fetch_features   

logger = logging.getLogger(__name__)
logging.basicConfig( format='%(asctime)s %(levelname)-8s %(message)s',filename='./import_qa.log', encoding='utf-8', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
# logger.addHandler(watchtower.CloudWatchLogHandler())

def field_equality(comparison_feature, sweri_feature, iterator_offset = 0):
    field_equal = {}
    iterator = 0 + iterator_offset
    
    if iterator_offset > 0:
        field_equal['id'] = sweri_feature[0]

    for key in comparison_feature['attributes']:
        field_equal[key] = comparison_feature['attributes'][key] == sweri_feature[iterator]
        iterator += 1

    if(sweri_feature[-1] is not None and comparison_feature['geometry'] is not None):
        field_equal['geom'] = arcpy.AsShape(comparison_feature['geometry'],True).equals(sweri_feature[-1]) 
    
    return field_equal

def value_comparison(comparison_feature, sweri_feature, source_database, iterator_offset = 0):    

    value_compare = {}
    iterator = 0 + iterator_offset

    value_compare[source_database] = 'sweri'

    if iterator_offset > 0:
        value_compare['id'] = sweri_feature[0]
    
    for key in comparison_feature['attributes']:
        value_compare[comparison_feature['attributes'][key]] = sweri_feature[iterator]
        iterator += 1
    
    if(sweri_feature[-1] != None):
        value_compare['geom'] = arcpy.AsShape(comparison_feature['geometry'],True).equals(sweri_feature[-1]) 
    
    return value_compare

def compare_features(comparison_feature, sweri_feature, iterator_offset = 0):
    iterator = 0 + iterator_offset
    total_compare = True
    
    for key in comparison_feature['attributes']:
        if type(sweri_feature[iterator]) == float:
            total_compare = total_compare and ((comparison_feature['attributes'][key]) - sweri_feature[iterator]) < 1
        elif type(sweri_feature[iterator]) == str:
            total_compare = total_compare and comparison_feature['attributes'][key].strip() == sweri_feature[iterator].strip()
        else:
            total_compare = total_compare and comparison_feature['attributes'][key] == sweri_feature[iterator]
        iterator += 1

    if(sweri_feature[-1] != None):
        total_compare = total_compare and arcpy.AsShape(comparison_feature['geometry'],True).equals(sweri_feature[-1])
    
    return total_compare


def get_comparison_ids(cur, identifier_database, treatment_index, schema):
    cur.execute(f'''
        SELECT unique_id
        FROM {schema}.{treatment_index}
        tablesample system (1)
	    WHERE identifier_database = '{identifier_database}'
        AND
        shape IS NOT NULL
        limit 575;
    ''')

    id_rows = cur.fetchall()
    comparison_ids = [str(id_row[0]) for id_row in id_rows]
    return comparison_ids


def compare_sweri_to_service(treatment_index_fc, sweri_fields, sweri_where_clause, service_fields, service_url, date_field, source_database, iterator_offset = 0, wkid = 3857):


    with arcpy.da.SearchCursor(treatment_index_fc, sweri_fields, where_clause=sweri_where_clause, spatial_reference=arcpy.SpatialReference(wkid)) as service_cursor:
        same = 0
        different = 0
        features_equal = False

        for row in service_cursor:
            
            if source_database == 'FACTS Hazardous Fuels':
                service_where_clause = f"activity_cn = '{row[0]}'" 

            elif source_database == 'FACTS Common Attributes':
                service_where_clause = f"event_cn = '{row[0]}'"

            elif source_database == 'NFPORS':
                nfporsfid, trt_id = row[0].split('-',1)
                service_where_clause = f"nfporsfid = '{nfporsfid}' AND trt_id = '{trt_id}'"
                
            params = {'f': 'json', 'outSR': wkid, 'outFields': ','.join(service_fields), 'returnGeometry': 'true',
            'where': service_where_clause}
        
            service_feature = fetch_features(service_url +'/query', params)
            
            if  service_feature == None or len(service_feature) == 0:
                logging.warning(f'No feature returned for {row[0]} in {source_database}')
                different += 1
                continue

            elif len(service_feature) > 1:
                logging.warning(f'more than one feature returned for {row[0]} in {source_database}, skipping comparison')
                continue
            
            target_feature = service_feature[0]

            if target_feature['attributes'][date_field] is not None:
                target_feature['attributes'][date_field] = datetime.datetime(1970, 1, 1) + datetime.timedelta(seconds=(target_feature['attributes'][date_field]/1000))
            
            target_feature['geometry']['spatialReference'] = {'wkid':wkid}

            features_equal = compare_features(target_feature, row, iterator_offset)  

            if features_equal == True:
                same += 1
            else:
                logging.warning(field_equality(target_feature, row, iterator_offset))
                logging.warning(value_comparison(target_feature, row, source_database, iterator_offset))
                different += 1
    
    logging.info(f'{source_database} comparison complete')
    logging.info(f'same: {same}')
    logging.info(f'different: {different}')
    if different >= 1:
        logging.warning(f'{different} features from {source_database} did not match')
    else:
        logging.info(f'all {same} sweri {source_database} features matched source {source_database} features')

def hazardous_fuels_sample(treatment_index_fc, cursor, treatment_index, schema, service_url):

    haz_fields = ['activity_cn', 'activity_sub_unit_name','date_completed','gis_acres','treatment_type','cat_nm','fund_code','cost_per_uom','uom','state_abbr','activity']
    sweri_haz_fields = ['unique_id', 'name', 'actual_completion_date', 'acres', 'type', 'category', 'fund_code', 'cost_per_uom', 'uom', 'state', 'activity', 'SHAPE@']
    source_database = 'FACTS Hazardous Fuels'
    date_field = 'DATE_COMPLETED'

    ids = get_comparison_ids(cursor, source_database, treatment_index, schema)

    if ids: 
        id_list = ', '.join(f"'{i}'" for i in ids)  
        sweri_haz_where_clause = f"identifier_database = 'FACTS Hazardous Fuels' AND unique_id IN ({id_list})"
    else:
        sweri_haz_where_clause = f"identifier_database = 'FACTS Hazardous Fuels' AND unique_id IN ()"

    logging.info('Running Hazardous Fuels sample comparison')
    compare_sweri_to_service(treatment_index_fc, sweri_haz_fields, sweri_haz_where_clause,haz_fields, service_url, date_field, source_database)




def nfpors_sample(treatment_index_fc, cursor, treatment_index, schema, service_url):
    nfpors_fields = ['trt_nm','act_comp_dt','gis_acres','type_name','cat_nm','st_abbr']
    sweri_nfpors_fields = ['unique_id', 'name', 'actual_completion_date', 'acres', 'type', 'category', 'state', 'SHAPE@']
    source_database = 'NFPORS'

    ids = get_comparison_ids(cursor, source_database, treatment_index, schema)
    if ids: 
        id_list = ', '.join(f"'{i}'" for i in ids)  
        sweri_where_clause = f"identifier_database = 'NFPORS' AND unique_id IN ({id_list})"
    else:
        sweri_where_clause = f"identifier_database = 'NFPORS' AND unique_id IN ()"
    
    date_field = 'act_comp_dt'

    #need to offset NFPORS by 1 to ignore the id field since we are splitting it apart
    iterator_offset = 1

    logging.info('Running NFPORS sample comparison')
    compare_sweri_to_service(treatment_index_fc, sweri_nfpors_fields, sweri_where_clause, nfpors_fields, service_url, date_field, source_database, iterator_offset)

                 


def common_attributes_sample(treatment_index_fc, cursor, treatment_index, schema, service_url):
    common_attributes_fields = ['event_cn', 'name','date_completed','gis_acres','nfpors_treatment','nfpors_category','state_abbr','fund_codes','cost_per_unit','uom','activity']
    sweri_common_attributes_fields = ['unique_id', 'name', 'actual_completion_date', 'acres', 'type', 'category', 'state', 'fund_code', 'cost_per_uom', 'uom', 'activity', 'SHAPE@']
    source_database = 'FACTS Common Attributes'

    ids = get_comparison_ids(cursor, source_database, treatment_index, schema)

    if ids: 
        id_list = ', '.join(f"'{i}'" for i in ids)  
        sweri_where_clause = f"identifier_database = 'FACTS Common Attributes' AND unique_id IN ({id_list})"
    else:
         sweri_where_clause = f"identifier_database = 'FACTS Common Attributes' AND unique_id IN ()"

    date_field = 'DATE_COMPLETED'

    logging.info('Running Common Attributes sample comparison')
    compare_sweri_to_service(treatment_index_fc, sweri_common_attributes_fields, sweri_where_clause, common_attributes_fields, service_url, date_field, source_database)



if __name__ == '__main__':
    load_dotenv()

    cur, conn = connect_to_pg_db(os.getenv('DB_HOST'), os.getenv('DB_PORT'), os.getenv('DB_NAME'), os.getenv('DB_USER'), os.getenv('DB_PASSWORD'))
    arcpy.env.workspace = arcpy.env.scratchGDB

    sde_connection_file = os.getenv('SDE_FILE')
    target_schema = os.getenv('SCHEMA')
    treatment_index_table = 'treatment_index_facts_nfpors'
    hazardous_fuels_url = os.getenv('HAZARDOUS_FUELS_URL')
    nfpors_url = os.getenv('NFPORS_URL')
    common_attributes_url =  os.getenv('COMMON_ATTRIBUTES_URL')
    treatment_index_sweri_fc = os.path.join(sde_connection_file, f"sweri.{target_schema}.{treatment_index_table}")

    logging.info('new run')
    logging.info('______________________________________')

    nfpors_sample(treatment_index_sweri_fc, cur, treatment_index_table, target_schema, nfpors_url)
    common_attributes_sample(treatment_index_sweri_fc, cur, treatment_index_table, target_schema, common_attributes_url)
    hazardous_fuels_sample(treatment_index_sweri_fc, cur, treatment_index_table, target_schema, hazardous_fuels_url)
    conn.close()