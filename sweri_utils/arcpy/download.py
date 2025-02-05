import json
import os
from arcpy.management import Delete
import arcpy.conversion
from arcpy import AddMessage, AddError, AddWarning, Exists
from sweri_utils.download import get_ids, get_all_features, get_fields


def fetch_and_create_featureclass(service_url, where, gdb, fc_name, geometry=None, geom_type=None, out_sr=3857,
                                  out_fields=None, chunk_size = 2000):
    """
    fetches and converts ESRI JSON features to a FeatureClass in a geodatabase
    :param service_url: REST endpoint of Esri service with feature access enabled
    :param where: where clause for filtering features to fetch
    :param gdb: output geodatabase for new FeatureClass
    :param fc_name: name of new FeatureClass
    :param geometry: geometry to use for query, if any
    :param geom_type: type of geometry used for query, required only if geometry is provided
    :param out_sr: out spatial reference WKID
    :param out_fields: out fields for query
    :return: new FeatureClass
    """
    # get count
    ids = get_ids(service_url, where, geometry, geom_type)
    out_features = []
    # get all features
    for f in get_all_features(service_url, ids, out_sr, out_fields, chunk_size):
        out_features += f
    if len(out_features) == 0:
        raise Exception(f'No features fetched for ids: {ids}')
    # save errors out if there is no geometry or attrbutes object
    fields = get_fields(service_url)
    fset = {'features': out_features, 'fields': fields, 'spatial_reference': {'wkid': out_sr}}
    # create a json file from the features

    with open(f'{fc_name}.json', 'w') as f:
        json.dump(fset, f)
    out_fc = os.path.join(gdb, fc_name)
    if Exists(out_fc):
        Delete(out_fc)
    out_fc = arcpy.conversion.JSONToFeatures(f.name, out_fc)
    # define the projection, without this FeaturesToJSON fails
    arcpy.management.DefineProjection(out_fc, arcpy.SpatialReference(out_sr))
    # delete JSON
    os.remove(f.name)
    return out_fc

