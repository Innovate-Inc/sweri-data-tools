import logging

import requests
import json
from urllib.parse import urljoin

def get_layer_definition(root_url, id, service_name, token):
    url = f'{root_url}/{id}'
    x = requests.get(url, params={'f': 'json', 'token': token})
    new_l_def = x.json()
    new_l_def['url'] = f"{url}?token={token}"
    new_l_def['adminLayerInfo'] = {
        "viewLayerDefinition": {
            "sourceServiceName": service_name,
            "sourceLayerId": id,
            "sourceLayerFields": "*"
        }
    }
    return new_l_def


def get_new_definition(root_url, new_service_name, token):
    new_service_url = urljoin(root_url, f'/arcgis/rest/services/Hosted/{new_service_name}/FeatureServer')
    based_info = requests.get(new_service_url, params={'f': 'json', 'token': token})
    based_info_json = based_info.json()
    layers = based_info_json.get('layers')
    tables = based_info_json.get('tables')
    new_definition = dict(tables=[], layers=[])
    for l in layers:
        new_definition['layers'].append(get_layer_definition(new_service_url, l['id'], new_service_name, token))
    for t in tables:
        new_definition['tables'].append(get_layer_definition(new_service_url, t['id'], new_service_name, token))
    return new_definition


def get_view_admin_url(root_url, service_name):
    return urljoin(root_url, f'/arcgis/rest/admin/services/Hosted/{service_name}/FeatureServer')


def clear_current_definition(view_url, token):
    r = requests.get(f'{view_url}', params={'f': 'json', 'token': token})
    current_def = r.json()
    deleted_def = dict(
        tables=[{'id': x['id']} for x in current_def['tables']],
        layers=[{'id': x['id']} for x in current_def['layers']],
    )
    r = requests.post(urljoin(view_url, f'/deleteFromDefinition'),
                      data={'f': 'json', 'token': token, 'deleteFromDefinition': json.dumps(deleted_def)})
    return r


def add_to_definition(view_url, new_definition, token):
    r = requests.post(urljoin(view_url, f'/addToDefinition'),
                      data={'f': 'json', 'token': token, 'addToDefinition': json.dumps(new_definition)})
    return r


def swizzle_service(root_url, view_name, new_service_name, token):
    """
    Updates a hosted view service by clearing its current definition and adding
    a new definition from another hosted service.

    Args:
        root_url (str): The base URL for the ArcGIS REST services.
        view_name (str): The name of the existing hosted view service.
        new_service_name (str): The name of the hosted service to copy from.
        token (str): The authentication token for ArcGIS REST requests.

    Returns:
        None
    """
    try:
        view_url = get_view_admin_url(root_url, view_name)
        new_def = get_new_definition(root_url, new_service_name, token)
        clear_current_definition(view_url, token)
        add_to_definition(view_url, new_def, token)
    except Exception as e:
        logging.error(e)
        raise e
