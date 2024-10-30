import arcpy
from os import getenv, path
import sys, os
import boto3
from uuid import uuid4

from arcpy.management import CreateTable


tools = r"C:\Data\Sweri\twig\scripts\sweri_utils"
sys.path.append(tools)
from intersections import configure_intersection_sources, update_schema_for_intersections_insert

if __name__ == '__main__':
    aoi = arcpy.GetParameterAsText(0) # AOI
    schema = arcpy.GetParameterAsText(1)
    sde_connection_file = arcpy.GetParameterAsText(2)
    s3_bucket = arcpy.GetParameterAsText(3)

    treatment_intersections = path.join(sde_connection_file, f'sweri.{schema}.intersections')
    target_table = CreateTable(arcpy.env.scratchGDB, 'intersections', template=treatment_intersections)

    intersection_features = os.path.join(sde_connection_file, f'sweri.{schema}.intersection_features')

    source_feature = {'source_key': 'custom', 'source_value': aoi}
    _, intersect_targets = configure_intersection_sources(sde_connection_file, schema)
    for target_key, target_value in intersect_targets.items():
        arcpy.AddMessage('Calculating intersections for ' + target_key)
        target_where = f"feat_source = '{target_key}'"
        target_layer = arcpy.management.MakeFeatureLayer(intersection_features, where_clause=target_where)

        intersect_output = os.path.join(arcpy.env.scratchGDB, target_key)

        arcpy.analysis.PairwiseIntersect([aoi, target_layer], intersect_output)

        arcpy.management.Delete(target_layer)
        update_schema_for_intersections_insert(intersect_output, 'aoi', target_key)
        arcpy.management.CalculateGeometryAttributes(intersect_output, [['acre_overlap', 'AREA_GEODESIC']],
                                                     area_unit='ACRES_US')
        arcpy.management.Append(intersect_output, target_table, 'NO_TEST')
        break

    csv_filename = '{}.json'.format(uuid4())
    csv_export = path.join(arcpy.env.scratchFolder, csv_filename)
    records = arcpy.RecordSet(target_table)
    j = records.JSON
    with open(csv_export, 'w') as f:
        f.write(j)

    # s3 = boto3.client('s3')
    # s3.upload_file(csv_export, s3_bucket, csv_export)

    arcpy.SetParameter(4, csv_filename)
    os.remove(arcpy.env.scratchFolder)

