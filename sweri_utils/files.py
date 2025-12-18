import os
import zipfile

import geopandas
import requests
import logging
import shutil
from .sweri_logging import log_this

try:
    from osgeo.gdal import VectorTranslateOptions, VectorTranslate
except ModuleNotFoundError:
    logging.warning('Missing osgeo, some functions will not work')

try:
    import arcpy
except ModuleNotFoundError:
    logging.warning('Missing arcpy, some functions will not work')


def get_disclaimer(out_dir, url, file_name='disclaimer.html'):
    try:
        file_name = os.path.join(out_dir, file_name)

        response = requests.get(url)
        if response.status_code == 200:
            response_dict = response.json()

            disclaimer_file = open(file_name, 'w')
            disclaimer_file.write(response_dict['content'])
            disclaimer_file.close()

            return file_name
        else:
            raise requests.exceptions.RequestException(response.status_code)
    except requests.exceptions.RequestException as e:
        return None

@log_this
def create_zip(zip_dir, name, out_dir=None):
    """
    creates a zip file from files in the zip_dir directory
    :param zip_dir: directory with files to zip
    :param name: name of zip file
    :param out_dir: temp directory for out zip file
    :return: path to zip file
    """
    if not out_dir:
        out_dir = os.path.dirname(zip_dir)
    out_path = os.path.join(out_dir, f'{name}.zip')
    zip_f = zipfile.ZipFile(out_path, 'w', zipfile.ZIP_DEFLATED)
    abs_src = os.path.abspath(zip_dir)
    for root, dirs, files in os.walk(zip_dir):
        for file in files:
            if not file.endswith('.lock'):
                abs_name = os.path.abspath(os.path.join(root, file))
                arc_name = abs_name[len(abs_src) + 1:]
                zip_f.write(abs_name, arc_name)
    zip_f.close()
    return out_path

@log_this
def download_file_from_url(url, destination_path):
    response = requests.get(url)
    with open(destination_path, 'wb') as file:
        file.write(response.content)

@log_this
def extract_and_remove_zip_file(zip_filepath):
    with zipfile.ZipFile(zip_filepath, 'r') as zip_file:
        zip_file.extractall()
    os.remove(zip_filepath)



@log_this
def gdb_to_postgres(gdb_name, projection: int, fc_name, postgres_table_name, schema, ogr_db_string, input_srs=None):
    os.environ['OGR_ORGANIZE_POLYGONS'] = 'SKIP'

    options_inputs = {
        "format": 'PostgreSQL',
        "makeValid": True,
        "dstSRS": f'EPSG:{projection}',
        "accessMode": 'overwrite',
        "layerName": f"{schema}.{postgres_table_name}",
        "layers": [fc_name]
    }

    if input_srs:
        options_inputs['srcSRS'] = input_srs

    options = VectorTranslateOptions(**options_inputs)

    gdb_path = os.path.join(os.getcwd(), gdb_name)

    # Upload fc to postgres
    _ = VectorTranslate(destNameOrDestDS=ogr_db_string, srcDS=gdb_path, options=options)
    del _
    logging.info(f'{postgres_table_name} now in geodatabase')

    # Remove gdb
    if os.path.exists(gdb_path):
        try:
            shutil.rmtree(gdb_path)
            logging.info(f'{gdb_path} gdb deleted')
        except OSError as e:
            logging.error(f'Error deleting {gdb_path}: {e}')


########################### arcpy required for below functions ###########################
def export_file_by_type(fc_path, filetype, out_dir, out_name, tmp_path):
    """
    Exports FeatureClass to different file types
    :param fc_path: path to FeatureClass
    :param out_dir: out directory of FeatureClass
    :param out_name: name of FeatureClass
    :param filetype: output file type: csv, gdb, shapefile, or geojson
    :param tmp_path: temp directory or workspace path
    :return: output file path
    """
    out_name_ext = f'{out_name}.{filetype}'
    arcpy.AddMessage(f'creating {out_name_ext}')
    try:
        if filetype == 'csv':
            outfile = arcpy.conversion.ExportTable(fc_path, os.path.join(out_dir, out_name_ext))
        elif filetype == 'gdb':
            # just save directly to new out directory
            outfile = arcpy.conversion.FeatureClassToGeodatabase(fc_path, os.path.join(out_dir, out_name_ext))
        elif filetype == 'shapefile':
            outfile = arcpy.conversion.FeatureClassToShapefile(fc_path, out_dir)
        elif filetype == 'geojson':
            outfile = arcpy.conversion.FeaturesToJSON(fc_path, os.path.join(out_dir, out_name_ext), geoJSON='GEOJSON')
        else:
            raise ValueError('invalid or missing file type')
    except Exception as e:
        raise e
    return outfile


def create_gdb(out_name, out_dir):
    """
    creates new file geodatabase
    :param out_name: out name for geodatabase
    :param out_dir: out directory of geodatabase
    :return: path to new geodatabase
    """
    out_name_ext = f'{out_name}.gdb'
    arcpy.management.CreateFileGDB(out_dir, out_name_ext)
    return os.path.join(out_dir, out_name_ext)

@log_this
def pg_table_to_gdb(ogr_db_string, schema, table, fc_name, wkid,
                    input_srs=None, work_dir=None, query=None, geom_col="shape"):
    if not work_dir:
        work_dir = os.getcwd()

    gdb_path = os.path.join(work_dir, f"{fc_name}.gdb")
    if os.path.exists(gdb_path):
        shutil.rmtree(gdb_path)

    opts = {
        "format": "OpenFileGDB",
        "makeValid": True,
        "dstSRS": f"EPSG:{wkid}",
        "geometryType": ["PROMOTE_TO_MULTI", "MULTIPOLYGON"],
        "layerName": fc_name,
    }

    if input_srs:
        opts["srcSRS"] = input_srs

    opts["SQLStatement"] = query or f"SELECT * FROM {schema}.{table} WHERE {geom_col} IS NOT NULL"

    options = VectorTranslateOptions(**opts)

    out_ds = VectorTranslate(destNameOrDestDS=gdb_path, srcDS=ogr_db_string, options=options)
    if out_ds is None:
        raise RuntimeError("nothing written")
    out_ds = None

    logging.info(f"{gdb_path} written")
    return gdb_path
