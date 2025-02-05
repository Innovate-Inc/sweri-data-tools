import os
from urllib.parse import urljoin
import zipfile
import requests

def get_disclaimer(out_dir):
    try:
        url = os.getenv('API_URL') if os.getenv('API_URL') else 'https://gis.reshapewildfire.org/cms/api/v2/'
        file_name = os.path.join(out_dir, 'disclaimer.html')

        response = requests.get(urljoin(url, 'snippets/download_disclaimer/'))
        if response.status_code == 200:
            response_dict = response.json()

            disclaimer_file = open(file_name, 'w')
            disclaimer_file.write(response_dict['content'])
            disclaimer_file.close()

            return file_name
        else:
            raise requests.exceptions.RequestException(response.status_code)
    except requests.exceptions.RequestException as e:
        return None

def create_zip(zip_dir, name, out_dir=None):
    """
    creates a zip file from files in the zip_dir directory
    :param zip_dir: directory with files to zip
    :param name: name of zip file
    :param out_dir: temp directory for out zip file
    :return: path to zip file
    """
    if not out_dir:
        out_dir = os.path.dirname(zip_dir)
    out_path = os.path.join(out_dir, f'{name}.zip')
    zip_f = zipfile.ZipFile(out_path, 'w', zipfile.ZIP_DEFLATED)
    abs_src = os.path.abspath(zip_dir)
    for root, dirs, files in os.walk(zip_dir):
        for file in files:
            if not file.endswith('.lock'):
                abs_name = os.path.abspath(os.path.join(root, file))
                arc_name = abs_name[len(abs_src) + 1:]
                zip_f.write(abs_name, arc_name)
    zip_f.close()
    return out_path

def download_file_from_url(url, destination_path):
    response = requests.get(url)
    with open(destination_path, 'wb') as file:
        file.write(response.content)

def extract_and_remove_zip_file(zip_filepath):
    with zipfile.ZipFile(zip_filepath, 'r') as zip_file:
        zip_file.extractall()
    os.remove(zip_filepath)

