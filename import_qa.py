from dotenv import load_dotenv
import os
os.environ["CRYPTOGRAPHY_OPENSSL_NO_LEGACY"]="1"
import requests
import json
import arcpy

from sweri_utils.sql import rename_postgres_table, connect_to_pg_db
from sweri_utils.download import fetch_and_create_featureclass   

#Get Counts of Input Layers vs What we have in the database
def get_count(service_url):
    count_params = {
        'where':'1=1',
        'returnCountOnly': 'true',
        'f': 'json'  # Specify the response format
    }
    response = requests.post(service_url + "/query", data=count_params)
    data = response.json()
    return data['count']
#Get saple groups with big enough sizes and make sure entries are the same as the source data
if __name__ == '__main__':
    load_dotenv()

    cur = connect_to_pg_db(os.getenv('DB_HOST'), os.getenv('DB_PORT'), os.getenv('DB_NAME'), os.getenv('DB_USER'), os.getenv('DB_PASSWORD'))
    sde_connection_file = os.getenv('SDE_FILE')
    nfpors_url = os.getenv('NFPORS_URL')
    hazardous_fuels_url = os.getenv('HAZRADOUS_FUELS_URL')
    common_attributes_service = os.getenv('COMMON_ATTRIBUTES_SERVICE')
    sweri_treatment_index_url = os.getenv('TREATMENT_INDEX_URL')
    arcpy.env.workspace = arcpy.env.scratchGDB

    haz_fields = ['activity_sub_unit_name','etl_modified_date_haz','date_completed','gis_acres','treatment_type','cat_nm','fund_code','cost_per_uom','activity_cn','uom','state_abbr','activity','shape']

    cur.execute('''
        SELECT unique_id
        FROM staging.treatment_index_common_attributes
        tablesample system (1)
	    where identifier_database = 'FACTS Hazardous Fuels'
        limit 575;
    ''')
    rows = cur.fetchall()
    ids = [str(row[0]) for row in rows]

    if ids:  # Ensure there are IDs to process
        id_list = ', '.join(f"'{i}'" for i in ids)  # Add quotes around each ID
        hazardous_fuels_where_clause = f"activity_cn IN ({id_list})"
        sweri_where_clause = f"identifier_database = 'FACTS Hazardous Fuels' AND unique_id IN ({id_list})"
    else:
        where_clause = "activity_cn IN ()"  # Avoid invalid SQL when no IDs are present
    

    hazardous_fuels_sweri_fc = fetch_and_create_featureclass(sweri_treatment_index_url, sweri_where_clause, arcpy.env.scratchGDB, 
                                  'hazardous_fuels_sweri_fc', geometry=None, geom_type=None, out_sr=3857,
                                  out_fields=None, chunk_size = 100)
    
    hazardous_fuels_fs_fc = fetch_and_create_featureclass(hazardous_fuels_url, hazardous_fuels_where_clause, arcpy.env.scratchGDB, 
                                  'hazardous_fuels_fs_fc', geometry=None, geom_type=None, out_sr=3857,
                                  out_fields=haz_fields, chunk_size = 100)
    
    

    cur.execute('''
        SELECT unique_id
        FROM staging.treatment_index_common_attributes
        tablesample system (1)
	    where identifier_database = 'FACTS Common Attributes'
        limit 575;
    ''')
    rows = cur.fetchall()
    ids = [str(row[0]) for row in rows]

    if ids:  # Ensure there are IDs to process
        id_list = ', '.join(f"'{i}'" for i in ids)  # Add quotes around each ID
        common_attributes_where_clause = f"event_cn IN ({id_list})"
        sweri_where_clause = f"identifier_database = 'FACTS Common Attributes' AND unique_id IN ({id_list})"
    else:
        common_attributes_where_clause = "event_cn IN ()"  # Avoid invalid SQL when no IDs are present
        sweri_where_clause = f"identifier_database = 'FACTS Common Attributes' AND unique_id IN ()"
    

    common_attributes_sweri_fc = fetch_and_create_featureclass(sweri_treatment_index_url, sweri_where_clause, arcpy.env.scratchGDB, 
                                  'hazardous_fuels_sweri_fc', geometry=None, geom_type=None, out_sr=3857,
                                  out_fields=None, chunk_size = 100)
    
    common_attributes_fs_fc = fetch_and_create_featureclass(common_attributes_service, common_attributes_where_clause, arcpy.env.scratchGDB, 
                                  'hazardous_fuels_fs_fc', geometry=None, geom_type=None, out_sr=3857,
                                  out_fields=None, chunk_size = 100)
    print('test')

    cur.execute('''
        SELECT unique_id
        FROM staging.treatment_index_common_attributes
        tablesample system (1)
	    where identifier_database = 'NFPORS'
        limit 575;
    ''')
    rows = cur.fetchall()
    sweri_ids = [str(row[0]) for row in rows]
    nfpors_id_pairs = [row[0].split('-') for row in rows]


    if ids:  # Ensure there are IDs to process
        sweri_ids = ', '.join(f"'{i}'" for i in ids)  # Add quotes around each ID
        nfpors_where_clause = ' OR '.join(f"f(nfporsfid = '{nfporsfid} AND trt_id = '{trt_id}')" for nfporsfid, trt_id in nfpors_id_pairs)
        sweri_nfpors_where_clause = f"identifier_database = 'NFPORS' AND unique_id IN ({sweri_ids})"
    else:
        nfpors_where_clause = "1=0"  # Avoid invalid SQL when no IDs are present
        sweri_nfpors_where_clause = f"identifier_database = 'NFPORS' AND unique_id IN ()"
    

    nfpors_sweri_fc = fetch_and_create_featureclass(sweri_treatment_index_url, sweri_nfpors_where_clause, arcpy.env.scratchGDB, 
                                  'hazardous_fuels_sweri_fc', geometry=None, geom_type=None, out_sr=3857,
                                  out_fields=None, chunk_size = 100)
    
    nfpors_fc = fetch_and_create_featureclass(nfpors_url, nfpors_where_clause, arcpy.env.scratchGDB, 
                                  'hazardous_fuels_fs_fc', geometry=None, geom_type=None, out_sr=3857,
                                  out_fields=None, chunk_size = 100)
    print('test')
