
############### for local debugging only #################
# import sys
# sys.path.append("C:\Program Files\JetBrains\PyCharm 2024.2.0.1\debug-eggs\pydevd-pycharm.egg")
# import pydevd_pycharm
#
# pydevd_pycharm.settrace('localhost', port=12345, stdoutToServer=True,
#                         stderrToServer=True)
##########################################################
import arcpy
import datetime as dt
import os
import sys
################ Local Path to sweri_utils directory, must be hard-coded when publishing #################
mod = r"C:\path_to_your_local_sweri_utils_directory"
##########################################################################################################
sys.path.append(mod)
import files
import download


if __name__ == "__main__":
    # get params
    url = arcpy.GetParameterAsText(0)
    fc = arcpy.GetParameterAsText(1)
    filetype = arcpy.GetParameterAsText(2)
    where = arcpy.GetParameterAsText(3)
    geom = arcpy.GetParameterAsText(4)
    geom_type = arcpy.GetParameterAsText(5)

    # set workspace
    arcpy.env.overwriteOutput = True
    gdb = arcpy.env.scratchGDB

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
        files.get_disclaimer(out_dir)
        # return zip file containing both
        arcpy.AddMessage('creating zip of result file and disclaimer')
        zip_file = files.create_zip(out_dir, out_name)
        arcpy.AddMessage(zip_file)
        # remove the scratch FC after creating the file
    except Exception as e:
        arcpy.AddError(e.args[0])
        raise e

    # return it
    arcpy.SetParameterAsText(6, zip_file)

