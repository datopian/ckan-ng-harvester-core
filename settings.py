import os

CKAN_API_KEY = ''
CKAN_BASE_URL = ''  # http://localhost:5000
#e.g. '8d4de31c-979c-4b50-be6b-ea3c72453ff6' is the Dep Energy US Gov ar catalog.data.gov
HARVEST_SOURCE_ID = ''
CKAN_ORG_ID = ''  # create in you local instance and get the ID
CKAN_VALID_USER_ID = ''  # string, something like 13373386-636f-420f-aff0-0102087bfa28

# replace with you local values in this ignored file
from local_settings import *

# replace with you local values in this ignored file
from local_settings import *

# check for ENV variables
env_ckan_api_key = os.environ.get('CKAN_API_KEY', None)
if env_ckan_api_key is not None:
    CKAN_API_KEY = env_ckan_api_key

env_ckan_base_url = os.environ.get('CKAN_BASE_URL', None)
if env_ckan_base_url is not None:
    CKAN_BASE_URL = env_ckan_base_url

env_ckan_valid_user_id = os.environ.get('CKAN_VALID_USER_ID', None)
if env_ckan_valid_user_id is not None:
    CKAN_VALID_USER_ID = env_ckan_valid_user_id
