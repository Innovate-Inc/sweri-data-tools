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


def find_layer_by_name(layers, target_name):

    for layer in layers:
        if layer['title'] == target_name:
            return  layer
        
    return None

def return_treatment_index_symbology(webmap_id, group_layer_name, layer_name):

    webmap = gis.content.get(webmap_id)
    webmap_data = webmap.get_data()

    group_layer = find_layer_by_name(webmap_data['operationalLayers'], group_layer_name)
    symbology_layer = find_layer_by_name(group_layer['layers'], layer_name)

    extracted_symbology = symbology_layer['layerDefinition']['drawingInfo']['renderer']
    return extracted_symbology

def return_db_connection_url(db_host: str, db_port: int, db_name: str, db_user: str, db_password: str) -> tuple:

    return f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
         

def postgis_query_to_geojson(pg_con, query, output_path='output1.geojson'):

    if os.path.exists(output_path):
        os.remove(output_path)

    gdf = geopandas.GeoDataFrame.from_postgis(query, pg_con, geom_col='shape')
    gdf.to_file(output_path, driver="FileGDB") 

    return output_path
        
def postgres_chunk_query(schema, table, min, max):

    sql_query = f"""
    SELECT * FROM {schema}.{table} 
    WHERE
    objectid >= {min}
    AND
    objectid <= {max};
    """
    return sql_query

def overwrite_portal_geojson(gis_con, item_id, path):
    item = gis_con.content.get(item_id)
    with open(path, "rb") as f:
        geojson_data = io.BytesIO(f.read())
    item.update(data = geojson_data)

def append_geojson_to_service(feature_layer, geojson_id):
        feature_layer.append(
            item_id = geojson_id,
            upload_format='geojson',
            upsert = False,
            use_globalids=False,
            )

def return_max_and_min_objectid(connection, table, schema):

    table_ref = Table(table, MetaData(), autoload_with=connection, schema=schema)

    with connection.connect() as conn:
        min_objectid, max_objectid = conn.execute(
            select(func.min(table_ref.c.objectid), func.max(table_ref.c.objectid))
        ).fetchone()

    return min_objectid, max_objectid

def delete_features(feature_layer):
    feature_layer.delete_features(
        where = '1=1'
    )

if __name__ == '__main__':
    load_dotenv()
    logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',filename='./hosted_upload.log', encoding='utf-8', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')

    gis = GIS("https://gis.reshapewildfire.org/arcgis", os.getenv("ESRI_USER"), os.getenv("ESRI_PW"))
    target_schema = os.getenv('SCHEMA')
    target_table = "treatment_index"
    geojson_item_id = '945554c6954a4d7691bea98060b1bb2c'
    hosted_feature_id = 'e1c81818ee184b2993d041abd8a330a7'
    hosted_item = gis.content.get(hosted_feature_id)
    hosted_feature_layer = hosted_item.layers[0]
    # Delete Features where 1=1 arcgis for python api https://developers.arcgis.com/rest/services-reference/enterprise/delete-features/

    # Change view to look at newly updated hosted feature layer

    # Create 2 hosted feature layers in the test folder
    # delete_features(hosted_feature_layer)
    db_connection_url = return_db_connection_url(os.getenv('DB_HOST'), os.getenv('DB_PORT'), os.getenv('DB_NAME'), os.getenv('DB_USER'), os.getenv('DB_PASSWORD'))
    con = create_engine(db_connection_url)
    
    min_objectid, max_objectid = return_max_and_min_objectid(con, target_table, target_schema)

    start = min_objectid
    chunk_size = 10000

    while start < max_objectid:
        logging.info(start)
        sql_query = postgres_chunk_query(target_schema, target_table, start, start+chunk_size)
        geojson_path = postgis_query_to_geojson(con, sql_query) 
        overwrite_portal_geojson(gis,geojson_item_id, geojson_path)
        append_geojson_to_service(hosted_feature_layer, geojson_item_id)
        start += chunk_size
        # Append geojson item to hosted feature layer using id of geojson item


    # rehsape_webmap_id = "ed19286412494bd280393b6eaeb126c6"
    # reshape_group_layer_name = 'Staging Treatments'
    # reshape_layer_name = 'Staging TWIG Treatment Index and Intersections'

    # treatment_index_symbology = return_treatment_index_symbology(rehsape_webmap_id, reshape_group_layer_name, reshape_layer_name)
    
    # https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.from_postgis.html
    # https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.to_json.html
