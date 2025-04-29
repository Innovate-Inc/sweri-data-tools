import arcpy
import datetime as dt
import os
import sys
from urllib.parse import urljoin
################ Local Path to sweri_utils directory, must be hard-coded when publishing #################
mod = r"C:\path_to_your_local_sweri_utils_directory"
##########################################################################################################
sys.path.append(mod)
import files
import download


if __name__ == "__main__":
    # get params
    url = arcpy.GetParameterAsText(0)
    fc_in = arcpy.GetParameterAsText(1)
    filetype = arcpy.GetParameterAsText(2)
    where = arcpy.GetParameterAsText(3)
    geom = arcpy.GetParameterAsText(4)
    geom_type = arcpy.GetParameterAsText(5)
    api_url = arcpy.GetParameterAsText(6)

    # set defaults CMS API url
    if not api_url:
        api_base_url = os.getenv('API_URL') if os.getenv('API_URL') else 'https://cms.reshapewildfire.org/api/v2/'
        api_url = urljoin(api_base_url, 'snippets/download_disclaimer/')

    # set workspace
    arcpy.env.overwriteOutput = True
    gdb = arcpy.env.scratchGDB

    # replace invalid characters in the feature class name with _
    fc = "".join([x if x.isalnum() else "_" for x in fc_in])

    # set out name and create output directory
    out_name = f"{fc}_{dt.datetime.now().strftime('%m-%d-%Y_%H-%M-%S')}"
    out_dir = os.path.join(arcpy.env.scratchFolder, out_name)
    os.mkdir(out_dir)

    # get the features
    try:
        # if output is a gdb, create a fresh one instead of using the scratch geodatabase
        if filetype == 'gdb':
            gdb = files.create_gdb(out_name, out_dir)
        save_features = download.fetch_and_create_featureclass(url, where, gdb, fc, geom, geom_type)
        arcpy.AddMessage(f'out features: {save_features}')
    except Exception as e:
        arcpy.AddError(e.args[0])
        raise e

    # output the result file
    try:
        result_file = files.export_file_by_type(save_features, filetype, out_dir, out_name, arcpy.env.scratchFolder)
        arcpy.AddMessage(result_file)
        # get disclaimer and put it in the same directory as the result file
        arcpy.AddMessage('retrieving disclaimer')
        files.get_disclaimer(out_dir, api_url)
        # return zip file containing both
        arcpy.AddMessage('creating zip of result file and disclaimer')
        zip_file = files.create_zip(out_dir, out_name)
        arcpy.AddMessage(zip_file)
        # remove the scratch FC after creating the file
    except Exception as e:
        arcpy.AddError(e.args[0])
        raise e

    # return it
    arcpy.SetParameterAsText(7, zip_file)

