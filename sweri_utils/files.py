import os
import arcpy
import zipfile
import requests


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
            # just return the csv
            outfile = arcpy.conversion.ExportTable(fc_path, os.path.join(tmp_path, out_name_ext))
        elif filetype == 'gdb':
            # just save directly to new out directory
            outfile = create_zip(tmp_path, out_dir, out_name)
        elif filetype == 'shapefile':
            # zip and return it
            arcpy.conversion.FeatureClassToShapefile(fc_path, out_dir)
            outfile = create_zip(tmp_path, out_dir, out_name)
        elif filetype == 'geojson':
            # just return the geojson file
            outfile = arcpy.conversion.FeaturesToJSON(fc_path, os.path.join(tmp_path, out_name_ext), geoJSON='GEOJSON')
        else:
            raise ValueError('invalid or missing file type')
    except Exception as e:
        raise e
    return outfile


def create_zip(out_dir, zip_dir, name):
    """
    creates a zip file from files in the zip_dir directory
    :param out_dir: temp directory for out zip file
    :param zip_dir: directory with files to zip
    :param name: name of zip file
    :return: path to zip file
    """
    out_path = os.path.join(out_dir, f'{name}.zip')
    arcpy.AddMessage(f'creating zip file {out_path}')
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

def download_file_from_url(url, destination_path):
    response = requests.get(url)
    with open(destination_path, 'wb') as file:
        file.write(response.content)

def extract_and_remove_zip_file(zip_filepath):
    with zipfile.ZipFile(zip_filepath, 'r') as zip_file:
        zip_file.extractall()
    os.remove(zip_filepath)