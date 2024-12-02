import arcpy
import logging
import os
from .conversion import insert_from_db

def calculate_area_overlap_and_insert(fc_area_calc, insert_table_path, insert_table_name, connection, schema, insert_fields,
                                      fc_area_calc_from_fields, area_field, units="ACRES_US"):
    """
    calculates area for a layer and inserts the results into another FeatureClass
    :param fc_area_calc: FeatureClass to calculate area
    :param insert_table_path: table to insert results from area calc
    :param insert_table_name: name of insert table for db insert
    :param connection: ArcSDESQLExecute connection
    :param schema: target schema
    :param insert_fields: insert fields for insert table
    :param fc_area_calc_from_fields: fields to use from area calc FC when inserting into insert_table
    :param area_field: field to use in area calculation
    :param units: area unit, defaults to US Acres
    :return: None
    """
    initial_count = int(arcpy.management.GetCount(insert_table_path)[0])
    insert_fc_count = int(arcpy.management.GetCount(fc_area_calc)[0])

    logging.info(f'calculating geometry for {insert_fc_count} features from {arcpy.Describe(fc_area_calc).name}')

    # calculate acre_overlap, must use AREA_GEODESIC and intersect_result must have a defined spatial reference
    arcpy.management.CalculateGeometryAttributes(fc_area_calc, [[area_field, 'AREA_GEODESIC']],
                                                 area_unit=units)

    fc_area_calc_table = os.path.split(fc_area_calc)[1]
    logging.info(f'inserting {insert_fc_count} features from {arcpy.Describe(fc_area_calc).name} into {insert_table_name}')
    # append to target dataset
    insert_from_db(connection,
                   schema,
                   insert_table_name, insert_fields,
                   fc_area_calc_table, fc_area_calc_from_fields,
                   False)

    updated_count = int(arcpy.management.GetCount(insert_table_path)[0])
    logging.info(f'{updated_count} features now in  {arcpy.Describe(insert_table_path).name}\n')

    if updated_count != initial_count + insert_fc_count:
        logging.warning(
            f"Feature merge counts incorrect, difference of {updated_count - (initial_count + insert_fc_count)}\n")


def layer_intersections(in_features, source_key, target_key, intersection_output_name, gdb, where_field='feat_source'):
    """
    performs pairwise intersect on two FeatureLayers
    :param in_features: input featureClass to create a featureLayer from
    :param source_key: source feature class name
    :param target_key: target feature class name
    :param intersection_output_name: output name for resulting intersection
    :param gdb: geodatabase path
    :param where_field: field on in_features to filter results by
    :return: intersection output path
    """
    source_where = f"{where_field} = '{source_key}'"
    source_layer = arcpy.management.MakeFeatureLayer(in_features, where_clause=source_where)
    target_where = f"{where_field} = '{target_key}'"
    target_layer = arcpy.management.MakeFeatureLayer(in_features, where_clause=target_where)

    intersect_output = os.path.join(gdb, intersection_output_name)

    arcpy.analysis.PairwiseIntersect([source_layer, target_layer], intersect_output)
    arcpy.management.Delete(source_layer)
    arcpy.management.Delete(target_layer)
    return intersect_output
