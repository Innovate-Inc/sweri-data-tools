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

def configure_intersection_sources(intersections_source_list):
    """
    configures intersection sources fetched from intersection source list
    :param sde_connection_file: sde connection file path
    :param schema: target schema
    :return: intersection sources and intersection targets dictionaries
    """
    intersect_sources = {}
    intersect_targets = {}
    domains = fetch_domains(intersections_source_list)
    fields = ['source', 'id_source', 'uid_fields', 'use_as_target', 'source_type', 'name']
    with arcpy.da.SearchCursor(intersections_source_list,
                               field_names=fields, sql_clause=(None, "ORDER BY source_type ASC")) as source_cursor:
        for r in source_cursor:
            s = {'source': r[0], 'id': r[2], 'source_type': r[4]}
            if 'name' in domains and r[5] in domains['name']:
                s['name'] = domains['name'][r[5]]
            intersect_sources[r[1]] = s
            if r[3] == 1:
                intersect_targets[r[1]] = s
    return intersect_sources, intersect_targets


def update_schema_for_intersections_insert(intersect_result, fc_1_name, fc_2_name):
    # update existing fields with new id column names
    arcpy.management.AlterField(intersect_result, 'unique_id', 'id_1', 'id_1')
    arcpy.management.AlterField(intersect_result, 'unique_id_1', 'id_2', 'id_2')
    arcpy.management.AlterField(intersect_result, 'feat_source', 'id_1_source', 'id_1_source')
    arcpy.management.AlterField(intersect_result, 'feat_source_1', 'id_2_source', 'id_2_source')
    # add field names
    arcpy.management.CalculateField(intersect_result, 'id_1_source', "'{}'".format(fc_1_name), 'PYTHON3')
    arcpy.management.CalculateField(intersect_result, 'id_2_source', "'{}'".format(fc_2_name), 'PYTHON3')


def fetch_domains(in_table):
    """
    fetches domains from a table
    :param sde_connection_file: sde connection file path
    :param in_table: table to fetch domains from
    :return: dictionary of domains
    """
    all_domains = {d.name: d for d in arcpy.da.ListDomains(os.path.join(in_table, '..'))}
    domain_dict = {
        fld.name: {k: v for k, v in all_domains[fld.domain].codedValues.items()}
        for fld in arcpy.ListFields(in_table) if fld.domain
    }
    return domain_dict


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
    treatment_intersections = arcpy.GetParameterAsText(1) # Treatment Intersections
    intersection_features = arcpy.GetParameterAsText(2)
    intersections_source_list = arcpy.GetParameterAsText(3)
    progress = 0
    buffer = 2
    label = "Configuring data sources"
    arcpy.AddMessage(format_message(progress, buffer, label))
    target_table = CreateTable(arcpy.env.scratchGDB, 'intersections', template=treatment_intersections)

    source_feature = {'source_key': 'custom', 'source_value': aoi}
    _, intersect_targets = configure_intersection_sources(intersections_source_list)
    intersect_progress = round(95 / len(intersect_targets.items()))

    for target_key, target_value in intersect_targets.items():
        tv = target_value['name'] if 'name' in target_value else target_key
        label = 'Calculating intersections for ' + tv
        progress = buffer
        buffer += intersect_progress
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

    progress = buffer
    buffer += 2
    label = 'Generating output'
    arcpy.AddMessage(format_message(progress, buffer, label))

    filename = '{}.json'.format(uuid4())
    export = path.join(arcpy.env.scratchFolder, filename)
    records = arcpy.RecordSet(target_table)
    j = records.JSON
    with open(export, 'w') as f:
        f.write(j)

    arcpy.SetParameter(4, export)
