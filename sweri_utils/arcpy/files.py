import arcpy
import logging
import os
from ..files import download_file_from_url, extract_and_remove_zip_file


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
            outfile = os.path.join(out_dir, out_name_ext)
        elif filetype == 'shapefile':
            outfile = arcpy.conversion.FeatureClassToShapefile(fc_path, out_dir)
        elif filetype == 'geojson':
            outfile = arcpy.conversion.FeaturesToJSON(fc_path, os.path.join(out_dir, out_name_ext), geoJSON='GEOJSON')
        else:
            raise ValueError('invalid or missing file type')
    except Exception as e:
        raise e
    return outfile


def gdb_to_postgres(url, gdb_name, projection, fc_name, postgres_table_name, sde_file, schema):
    # Downloads a gdb with a single feature class
    # And uploads that featureclass to postgres
    zip_file = f'{postgres_table_name}.zip'

    # Download and extract gdb file
    logging.info(f'Downloading {url}')
    download_file_from_url(url, zip_file)

    logging.info(f'Extracting {zip_file}')
    extract_and_remove_zip_file(zip_file)

    # Set Workspace to Downloaded GDB and set paths for feature class and reprojection
    gdb_path = os.path.join(os.getcwd(), gdb_name)
    feature_class = os.path.join(gdb_path, fc_name)
    reprojected_fc = os.path.join(gdb_path, f'{postgres_table_name}')
    postgres_table_location = os.path.join(sde_file, f'sweri.{schema}.{postgres_table_name}')

    # Reproject layer
    logging.info(f'reprojecting {feature_class}')
    arcpy.Project_management(feature_class, reprojected_fc, projection)
    logging.info('layer reprojected')

    # Clear space in postgres for table
    if (arcpy.Exists(postgres_table_location)):
        arcpy.management.Delete(postgres_table_location)
        logging.info(f'{postgres_table_name} has been deleted')

    # Upload fc to postgres
    arcpy.conversion.FeatureClassToGeodatabase(reprojected_fc, sde_file)
    logging.info(f'{postgres_table_location} now in geodatabase')

    # Remove gdb
    arcpy.Delete_management(gdb_path)
    logging.info(f'{gdb_path} deleted')


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
