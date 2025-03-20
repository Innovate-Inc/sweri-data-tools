from arcgis.gis import GIS
from dotenv import load_dotenv
import os
from arcgis.features import FeatureLayer, FeatureLayerCollection
from osgeo import ogr
from sqlalchemy import create_engine, Table, MetaData, select, func
import geopandas
import json
import io
import logging
import sys
import shutil


def find_layer_by_name(layers, target_name):
    for layer in layers:
        if layer['title'] == target_name:
            return layer

    return None


def return_treatment_index_symbology(webmap_id, group_layer_name, layer_name):
    webmap = gis.content.get(webmap_id)
    webmap_data = webmap.get_data()

    group_layer = find_layer_by_name(webmap_data['operationalLayers'], group_layer_name)
    symbology_layer = find_layer_by_name(group_layer['layers'], layer_name)

    extracted_symbology = symbology_layer['layerDefinition']['drawingInfo']['renderer']
    return extracted_symbology


def return_db_connection_url(db_host: str, db_port: int, db_name: str, db_user: str, db_password: str) :
    return f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"


def postgis_query_to_geojson(pg_con, pg_query, output_path='output2.geojson'):

    """Okay we're going to switch from using geodataframes vectortranslate using

    https://gdal.org/en/latest/api/python/utilities.html#osgeo.gdal.VectorTranslate
    Example:
        where = "GIS_ACRES > 5 Or GIS_ACRES IS NOT NULL"
        options = VectorTranslateOptions(format='PostgreSQL', makeValid=True, dstSRS=f'EPSG:{projection}', where=where,
                                         accessMode='overwrite', layerName=f"{schema}.{postgres_table_name}",
                                         layers=[fc_name])

        gdb_path = os.path.join(os.getcwd(), gdb_name)
        pg = f'PG:host=db user=staging dbname=sweri password=sweri4postgres'

        logging.info(f'sending layer {postgres_table_name} to postgres using VectorTranslate')
        VectorTranslate(destNameOrDestDS=pg, srcDS=gdb_path, options=options)
    """
    gdf = geopandas.GeoDataFrame.from_postgis(pg_query, pg_con, geom_col='shape')

    if gdf.empty:
        return None

    max_objectid = gdf.iloc[-1]['objectid'] #set max_objectid to the max or last record in the gdf for objectid column (can do by -1 index most likely)
    esri_json_str = gdf.to_file(output_path, driver='ESRIJSON')

    # gdf.to_file(output_path, driver="ESRIJSON")
    #if gdf is empty, return max_objectid as -1 or none to stop loop 

    return max_objectid

def postgres_chunk_query(schema, table, chunk_size, objectid = 0):

    query = f"""
        SELECT * FROM {schema}.{table}
        WHERE
        objectid > {objectid}
        ORDER BY objectid ASC
        limit {chunk_size};
    """
    return query


def overwrite_portal_geojson(gis_con, item_id, geojson_data):
    item = gis_con.content.get(item_id)
    item.update(data=geojson_data)


def append_geojson_to_service(feature_layer, geojson_id):
    feature_layer.append(
        item_id=geojson_id,
        upload_format='geojson',
        upsert=False,
        use_globalids=False,
    )

def delete_features(feature_layer, where_clause = '1=1'):
    feature_layer.delete_features(
        where = where_clause
    )


if __name__ == '__main__':
    load_dotenv('.env')
    logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', filename='./hosted_upload.log',
                        encoding='utf-8', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')

    gis = GIS("https://gis.reshapewildfire.org/arcgis", os.getenv("ESRI_USER"), os.getenv("ESRI_PW"))
    target_schema = "sweri"
    target_table = "treatment_index_wgs84"
    geojson_item_id = '945554c6954a4d7691bea98060b1bb2c'
    hosted_feature_id = 'e1c81818ee184b2993d041abd8a330a7'
    hosted_item = gis.content.get(hosted_feature_id)
    hosted_feature_layer = hosted_item.layers[0]
    # Change view to look at newly updated hosted feature layer

    # Create 2 hosted feature layers in the test folder
    delete_features(hosted_feature_layer)
    db_connection_url = return_db_connection_url(os.getenv('DOCKER_DB_HOST'), os.getenv('DOCKER_DB_PORT'), os.getenv('DOCKER_DB_NAME'),
                                                 os.getenv('DOCKER_DB_USER'), os.getenv('DOCKER_DB_PASSWORD'))
    con = create_engine(db_connection_url)

    logging.info(f"{target_table}, {target_schema}")

    current_objectid = 0
    chunk_size = 1000

    while True:
        sql_query = postgres_chunk_query(target_schema, target_table, chunk_size, current_objectid)
        with io.BytesIO() as geojson_data:
            current_objectid = postgis_query_to_geojson(con, sql_query, geojson_data)
            if current_objectid is None:
                break

            overwrite_portal_geojson(gis,geojson_item_id, geojson_data)
        append_geojson_to_service(hosted_feature_layer, geojson_item_id)
        logging.info(current_objectid)

    print('hello')
