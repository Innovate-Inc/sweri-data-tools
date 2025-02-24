import arcpy
import json
from os import path
import sys, os
os.environ["CRYPTOGRAPHY_OPENSSL_NO_LEGACY"] = "1"
from uuid import uuid4
from arcpy.management import CreateTable

############### for local debugging only #################
# sys.path.append("C:\Program Files\JetBrains\PyCharm 2024.2.0.1\debug-eggs\pydevd-pycharm.egg")
# import pydevd_pycharm
#
# pydevd_pycharm.settrace('localhost', port=12345, stdoutToServer=True,
#                         stderrToServer=True)
#########################################################map

tools = r"C:\path\to\sweri_utils"
sys.path.append(tools)
from intersections import configure_intersection_sources, update_schema_for_intersections_insert

def format_message(progress, buffer, label):
    data = {
        'progress': progress,
        'buffer': buffer,
        'label': label
    }
    return json.dumps(data)

if __name__ == '__main__':
    arcpy.env.overwriteOutput = True
    aoi = arcpy.GetParameterAsText(0) # AOI
    schema = arcpy.GetParameterAsText(1)
    sde_connection_file = arcpy.GetParameterAsText(2)
    progress = 0
    buffer = 2
    label = "Configuring data sources"
    arcpy.AddMessage(format_message(progress, buffer, label))
    treatment_intersections = path.join(sde_connection_file, 'sweri.{}.intersections'.format(schema))
    target_table = CreateTable(arcpy.env.scratchGDB, 'intersections', template=treatment_intersections)

    intersection_features = os.path.join(sde_connection_file, 'sweri.{}.intersection_features'.format(schema))

    source_feature = {'source_key': 'custom', 'source_value': aoi}
    _, intersect_targets = configure_intersection_sources(sde_connection_file, schema)
    progress += buffer
    buffer = round(95/intersect_targets.items().length)

    for target_key, target_value in intersect_targets.items():
        tv = target_value['name'] if 'name' in target_value else target_key
        label = 'Calculating intersections for ' + tv
        arcpy.AddMessage(format_message(progress, buffer, label))
        target_where = "feat_source = '{}'".format(target_key)
        target_layer = arcpy.management.MakeFeatureLayer(intersection_features, where_clause=target_where)
        intersect_output = os.path.join(arcpy.env.scratchGDB, target_key)

        arcpy.analysis.PairwiseIntersect([aoi, target_layer], intersect_output)
        arcpy.management.Delete(target_layer)
        update_schema_for_intersections_insert(intersect_output, 'aoi', target_key)
        # calculate area if aoi is polygon otherwise just append the features and set acre_overlap to 0
        if arcpy.Describe(aoi).shapeType == 'Polygon':
            arcpy.management.CalculateGeometryAttributes(intersect_output, [['acre_overlap', 'AREA_GEODESIC']],
                                                     area_unit='ACRES_US')
        else:
             # set overlap to 0 if aoi is not a polygon
            arcpy.management.CalculateField(intersect_output, 'acre_overlap', '0', 'PYTHON3')

        arcpy.management.Append(intersect_output, target_table, 'NO_TEST')
        progress += buffer

    buffer = 2
    label = 'Generating output'
    arcpy.AddMessage(format_message(progress, buffer, label))

    filename = '{}.json'.format(uuid4())
    export = path.join(arcpy.env.scratchFolder, filename)
    records = arcpy.RecordSet(target_table)
    j = records.JSON
    with open(export, 'w') as f:
        f.write(j)
        
    arcpy.SetParameter(3, export)
