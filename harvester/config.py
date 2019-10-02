import os
from slugify import slugify
import json


DATA_FOLDER_PATH = 'data'
SOURCE_NAME = ''  # the source nage, e.g. Dep of Agriculture
SOURCE_ID = ''  # the harvest source id
SOURCE_URL = ''  # url of the source file
LIMIT_DATASETS = 0  # Limit datasets to harvest on each source. Defualt=0 => no limit"

CKAN_CATALOG_URL = ''  # 'https://catalog.data.gov'
CKAN_API_KEY = ''
CKAN_OWNER_ORG = ''  # ID of the orginazion sharing their data to a CKAN instance


def get_base_path():

    nice_name = slugify(SOURCE_NAME)
    base_path = os.path.join(DATA_FOLDER_PATH, nice_name)

    if not os.path.isdir(base_path):
        os.makedirs(base_path)

    return base_path


def get_data_cache_path(create=True):
    """ local path for data.json source file """
    path =  os.path.join(get_base_path(), 'data.json')
    if not os.path.isfile(path):
        open(path, 'w').close()
    return path


def get_flow1_data_package_result_path(create=True):
    """ local path for flow1 file """
    path =  os.path.join(get_base_path(), 'flow1-data-package-result.json')
    if not os.path.isfile(path):
        open(path, 'w').close()
    return path


def get_flow2_data_package_result_path(create=True):
    """ local path for flow2 data packages results file """
    path = os.path.join(get_base_path(), 'flow2-data-package-result.json')
    if not os.path.isfile(path):
        open(path, 'w').close()
    return path


def get_flow1_datasets_result_path(create=True):
    """ local path for flow1 results file """
    path = os.path.join(get_base_path(), 'flow1-datasets-results.json')
    if not os.path.isfile(path):
        open(path, 'w').close()
    return path


def get_flow2_datasets_result_path(create=True):
    path = os.path.join(get_base_path(), 'flow2-datasets-results.json')
    if not os.path.isfile(path):
        open(path, 'w').close()
    return path


def get_errors_path(create=True):
    """ local path for errors """
    path =  os.path.join(get_base_path(), 'errors.json')
    if not os.path.isfile(path):
        open(path, 'w').close()
    return path


def get_ckan_results_cache_path(create=True):
    """ local path for ckan results file """
    path =  os.path.join(get_base_path(), 'ckan-results.json')
    if not os.path.isfile(path):
        open(path, 'w').close()
    return path

def get_comparison_results_path(create=True):
    """ local path for comparison results file """
    path =  os.path.join(get_base_path(), 'compare-results.csv')
    if not os.path.isfile(path):
        open(path, 'w').close()
    return path


def get_data_packages_folder_path():
    """ local path for datapackages """
    data_packages_folder_path = os.path.join(get_base_path(), 'data-packages')
    if not os.path.isdir(data_packages_folder_path):
        os.makedirs(data_packages_folder_path)

    return data_packages_folder_path


def get_flow2_data_package_folder_path():
    """ local path for flow2 file """
    flow2_data_package_folder_path = os.path.join(get_base_path(), 'flow2')
    if not os.path.isdir(flow2_data_package_folder_path):
        os.makedirs(flow2_data_package_folder_path)

    return flow2_data_package_folder_path


def get_harvest_sources_path(hs_name):
    base_path = os.path.join(DATA_FOLDER_PATH, 'harvest_sources/datasets')

    if not os.path.isdir(base_path):
        os.makedirs(base_path)

    final_path = os.path.join(base_path, f'harvest-source-{hs_name}.json')

    return final_path


def get_harvest_sources_data_folder(source_type, name):
    base_path = os.path.join(DATA_FOLDER_PATH, 'harvest_sources', source_type)

    if not os.path.isdir(base_path):
        os.makedirs(base_path)

    return base_path


def get_harvest_sources_data_path(source_type, name, file_name):
    base_path = get_harvest_sources_data_folder(source_type, name)
    final_path = os.path.join(base_path, file_name)

    return final_path


def get_json_data_or_none(path):
    if not os.path.isfile(path):
        return None
    else:
        f = open(path, 'r')
        try:
            j = json.load(f)
        except Exception as e:
            j = {'error': str(e)}
        f.close()
        return j


def get_report_files():
    # collect important files to write a final report
    data_file = get_data_cache_path(create=False)
    results_file = get_flow2_datasets_result_path(create=False)
    errors_file = get_errors_path(create=False)

    return {'data': get_json_data_or_none(data_file),
            'results': get_json_data_or_none(results_file),
            'errors': get_json_data_or_none(errors_file)
            }


def get_html_report_path():
    return os.path.join(get_base_path(), 'final-report.html')

def get_final_json_results_for_report_path():
    return os.path.join(get_base_path(), 'final-results.json')