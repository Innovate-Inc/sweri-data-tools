import requests
import arcpy
import os
from dotenv import load_dotenv
import psycopg2
from arcgis.features import FeatureLayer, GeoAccessor, GeoSeriesAccessor, FeatureSet
import logging

def insert_nfpors_additions():
    cur.execute('BEGIN')
    cur.execute('''
    INSERT INTO staging.nfpors_new (
        objectid,
        trt_unt_id,
        local_id,
        col_date,
        trt_status,
        col_meth,
        comments,
        gis_acres,
        pstatus,
        modifiedon,
        createdon,
        cent_lat,
        cent_lon,
        userid,
        st_abbr,
        cong_dist,
        cnty_fips,
        trt_nm,
        fy,
        plan_acc_ac,
        act_acc_ac,
        act_init_dt,
        act_comp_dt,
        nfporsfid,
        trt_id_db,
        type_name,
        cat_nm,
        trt_statnm,
        col_methnm,
        plan_int_dt,
        unit_id,
        agency,
        trt_id,
        created_by,
        edited_by,
        projectname,
        regionname,
        projectid,
        keypointarea,
        unitname,
        deptname,
        countyname,
        statename,
        regioncode,
        districtname,
        isbil,
        bilfunding,
        gdb_geomattr_data,
        shape,
        globalid
        )
    SELECT 
        sde.next_rowid('staging', 'nfpors_new'),
        trt_unt_id,
        local_id,
        col_date,
        trt_status,
        col_meth,
        comments,
        gis_acres,
        pstatus,
        modifiedon,
        createdon,
        cent_lat,
        cent_lon,
        userid,
        st_abbr,
        cong_dist,
        cnty_fips,
        trt_nm,
        fy,
        plan_acc_ac,
        act_acc_ac,
        act_init_dt,
        act_comp_dt,
        nfporsfid,
        trt_id_db,
        type_name,
        cat_nm,
        trt_statnm,
        col_methnm,
        plan_int_dt,
        unit_id,
        agency,
        trt_id,
        created_by,
        edited_by,
        projectname,
        regionname,
        projectid,
        keypointarea,
        unitname,
        deptname,
        countyname,
        statename,
        regioncode,
        districtname,
        isbil,
        bilfunding,
        gdb_geomattr_data,
        shape,
        sde.next_globalid()
    FROM staging.nfpors_additions_new_1;
    ''')
    cur.execute('COMMIT')    

def get_ids(service_url):
    count_params = {
        'where': '1=1',
        'returnIdsOnly': 'true',
        'f': 'json'  # Specify the response format
    }
    response = requests.get(service_url + "/query", params=count_params)
    data = response.json()
    arcpy.AddMessage(data)
    return data.get('objectIds')

arcpy.env.workspace = arcpy.env.scratchGDB
arcpy.env.overwriteOutput = True
load_dotenv()
sde_connection_file  = os.getenv('SDE_FILE')
logger = logging.getLogger(__name__)
logging.basicConfig(filename='./nfpors_pull.log', encoding='utf-8', level=logging.INFO)

conn = psycopg2.connect(
    host=os.getenv('DB_HOST'),
    port=os.getenv('DB_PORT'),
    dbname=os.getenv('DB_NAME'),
    user= os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD')
)

cur = conn.cursor()
out_wkid = 3857
target_projection = arcpy.SpatialReference(out_wkid)
nfpors_url = 'https://usgs.nfpors.gov/arcgis/rest/services/nfpors_WM/MapServer/15'
nfpors_postgres = os.path.join(sde_connection_file, 'sweri.staging.nfpors_new')
nfpors_additions_postgres = os.path.join(sde_connection_file, 'sweri.staging.nfpors_additions_new')


# #Query all entries since our most recent data and save to feature class
nfpors_fl = FeatureLayer(nfpors_url)
start = 0
chunk_size = 70
ids = get_ids(nfpors_url)
str_ids = [str(i) for i in ids]
if('111221' in str_ids):
    str_ids.remove('111221')
#If there is new data, make last addition a backup, add the current addition to our nfpors data
while start < len(ids):

    #find most recent entry in our nfpors data
    id_list = ','.join(str_ids[start:start + chunk_size])
    logging.info(f'start: {start} ids: {str_ids[start:start + chunk_size]} of {len(ids)}')

    nfpors_fl_query = nfpors_fl.query(object_ids=id_list,  out_fields="*", return_geometry=True, out_sr=out_wkid)
    nfpors_additions_fc = nfpors_fl_query.save(arcpy.env.scratchGDB, 'nfpors_additions_new_1')
    count = int(arcpy.management.GetCount(nfpors_additions_fc)[0])
    logging.info(f'{count} additions to NFPORS')

    if (count > 0):
        try:
            print('Deleting gdb table')                        
            #make space for nfpors additions table
            if(arcpy.Exists(nfpors_additions_postgres)):
                arcpy.management.Delete(nfpors_additions_postgres)
                logging.info("additions deleted")

            print('Upload to postgres')
            #upload current addition to postgres
            arcpy.conversion.FeatureClassToGeodatabase(nfpors_additions_fc, sde_connection_file)
            logging.info("additions uploaded to postgrees")

            logging.info(f'inserting {nfpors_additions_postgres} into {nfpors_postgres}')

            print(f'inserting {nfpors_additions_postgres} into {nfpors_postgres}')
            #insert new postgres table of additions to nfpors
            insert_nfpors_additions()
            logging.info("additions appended to nfpors")
        except Exception as e:
            logging.error(e.args[0])
            raise e
    start+=chunk_size