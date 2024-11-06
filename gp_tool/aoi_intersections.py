import urllib.parse
import arcpy
from os import getenv, path
import sys, os
os.environ["CRYPTOGRAPHY_OPENSSL_NO_LEGACY"] = "1"
# import boto3
from uuid import uuid4
from arcpy.management import CreateTable

# ############### for local debugging only #################
# import sys
# sys.path.append("C:\Program Files\JetBrains\PyCharm 2024.2.0.1\debug-eggs\pydevd-pycharm.egg")
# import pydevd_pycharm
#
# pydevd_pycharm.settrace('localhost', port=12345, stdoutToServer=True,
#                         stderrToServer=True)
##########################################################map

tools = r"C:\Users\Keaton Shennan\projects\sweri-data-tools\sweri_utils"
sys.path.append(tools)
from intersections import configure_intersection_sources, update_schema_for_intersections_insert

if __name__ == '__main__':
    arcpy.SetProgressor("step", "calculating intersections")
    arcpy.env.overwriteOutput = True
    aoi = arcpy.GetParameterAsText(0) # AOI
    schema = arcpy.GetParameterAsText(1)
    # sde_connection_file = 'Z:\\home\\arcgis\\sweri-staging\\sweri-data-tools\\sweri_staging.sde'
    sde_connection_file = arcpy.GetParameterAsText(2)
    s3_bucket = arcpy.GetParameterAsText(3)
    s3_url = arcpy.GetParameterAsText(4)

    treatment_intersections = path.join(sde_connection_file, 'sweri.{}.intersections'.format(schema))
    target_table = CreateTable(arcpy.env.scratchGDB, 'intersections', template=treatment_intersections)

    intersection_features = os.path.join(sde_connection_file, 'sweri.{}.intersection_features'.format(schema))

    source_feature = {'source_key': 'custom', 'source_value': aoi}
    _, intersect_targets = configure_intersection_sources(sde_connection_file, schema)
    progress = 10
    arcpy.SetProgressorPosition(progress)
    interval = round(80 / len(intersect_targets.items()))
    for target_key, target_value in intersect_targets.items():
        step = round(interval  / 3)
        arcpy.AddMessage('Calculating intersections for ' + target_key)
        target_where = "feat_source = '{}'".format(target_key)
        target_layer = arcpy.management.MakeFeatureLayer(intersection_features, where_clause=target_where)

        intersect_output = os.path.join(arcpy.env.scratchGDB, target_key)

        arcpy.analysis.PairwiseIntersect([aoi, target_layer], intersect_output)
        progress += step
        arcpy.SetProgressorPosition(round(progress))
        arcpy.management.Delete(target_layer)
        update_schema_for_intersections_insert(intersect_output, 'aoi', target_key)
        arcpy.management.CalculateGeometryAttributes(intersect_output, [['acre_overlap', 'AREA_GEODESIC']],
                                                     area_unit='ACRES_US')
        progress += step
        arcpy.SetProgressorPosition(round(progress))
        arcpy.management.Append(intersect_output, target_table, 'NO_TEST')
        progress += step
        arcpy.SetProgressorPosition(round(progress))

    filename = '{}.json'.format(uuid4())
    export = path.join(arcpy.env.scratchFolder, filename)
    records = arcpy.RecordSet(target_table)
    j = records.JSON
    with open(export, 'w') as f:
        f.write(j)

    # todo: add boto3 package to server base python environment
    # s3 = boto3.client('s3')
    # s3.upload_file(export, s3_bucket, export)
    #
    # path_to_s3_file = urllib.parse.urljoin(s3_url, export)
    arcpy.SetParameter(5, export)
