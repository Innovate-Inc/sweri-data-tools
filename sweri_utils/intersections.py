from arcpy.da import SearchCursor
from os import path
import arcpy

def configure_intersection_sources(sde_connection_file, schema):
    """
    configures intersection sources fetched from intersection source list
    :param sde_connection_file: sde connection file path
    :param schema: target schema
    :return: intersection sources and intersection targets dictionaries
    """
    intersect_sources = {}
    intersect_targets = {}
    domains = fetch_domains(sde_connection_file, path.join(sde_connection_file, 'sweri.{}.intersections_source_list'.format(schema)))
    fields = ['source', 'id_source', 'uid_fields', 'use_as_target', 'source_type', 'name']
    with arcpy.da.SearchCursor(path.join(sde_connection_file, 'sweri.{}.intersections_source_list'.format(schema)),
                               field_names=fields, sql_clause=(None, "ORDER BY source_type ASC")) as source_cursor:
        for r in source_cursor:
            s = {'source': r[0], 'id': r[2],  'source_type': r[4]}
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
    arcpy.management.CalculateField(intersect_result, 'id_1_source', f"'{fc_1_name}'", 'PYTHON3')
    arcpy.management.CalculateField(intersect_result, 'id_2_source', f"'{fc_2_name}'", 'PYTHON3')

def fetch_domains(sde_connection_file, in_table):
    """
    fetches domains from a table
    :param sde_connection_file: sde connection file path
    :param in_table: table to fetch domains from
    :return: dictionary of domains
    """
    all_domains = {d.name: d for d in arcpy.da.ListDomains(sde_connection_file)}
    domain_dict = {
        f.name: {k: v for k, v in all_domains[f.domain].codedValues.items()}
        for f in arcpy.ListFields(in_table) if f.domain}
    return domain_dict