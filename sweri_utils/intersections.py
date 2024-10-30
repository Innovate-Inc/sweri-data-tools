from arcpy.da import SearchCursor
from arcpy.management import AlterField, CalculateField
from os import path


def configure_intersection_sources(sde_connection_file, schema):
    """
    configures intersection sources fetched from intersection source list
    :param sde_connection_file: sde connection file path
    :param schema: target schema
    :return: intersection sources and intersection targets dictionaries
    """
    intersect_sources = {}
    intersect_targets = {}
    fields = ['source', 'id_source', 'uid_fields', 'use_as_target', 'source_type']
    with SearchCursor(path.join(sde_connection_file, f'sweri.{schema}.intersections_source_list'),
                               field_names=fields, sql_clause=(None, "ORDER BY source_type ASC")) as source_cursor:
        for r in source_cursor:
            s = {'source': r[0], 'id': r[2],  'source_type': r[4]}
            intersect_sources[r[1]] = s
            if r[3] == 1:
                intersect_targets[r[1]] = s
    return intersect_sources, intersect_targets


def update_schema_for_intersections_insert(intersect_result, fc_1_name, fc_2_name):
    # update existing fields with new id column names
    AlterField(intersect_result, 'unique_id', 'id_1', 'id_1')
    AlterField(intersect_result, 'unique_id_1', 'id_2', 'id_2')
    AlterField(intersect_result, 'feat_source', 'id_1_source', 'id_1_source')
    AlterField(intersect_result, 'feat_source_1', 'id_2_source', 'id_2_source')
    # add field names
    CalculateField(intersect_result, 'id_1_source', f"'{fc_1_name}'", 'PYTHON3')
    CalculateField(intersect_result, 'id_2_source', f"'{fc_2_name}'", 'PYTHON3')
