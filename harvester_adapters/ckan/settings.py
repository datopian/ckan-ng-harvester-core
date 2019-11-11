import os

MAX_NAME_LENGTH = 100
MAX_TAG_NAME_LENGTH = 100

# Previous harvester uses this list statically
# check if we need dynamically from
# /api/3/action/license_list
LICENCES = {
    'Creative Commons Attribution':'cc-by',
    'Creative Commons Attribution Share-Alike':'cc-by-sa',
    'Creative Commons CCZero':'cc-zero',
    'Creative Commons Non-Commercial (Any)':'cc-nc',
    'GNU Free Documentation License':'gfdl',
    'License Not Specified':'notspecified',
    'Open Data Commons Attribution License':'odc-by',
    'Open Data Commons Open Database License (ODbL)':'odc-odbl',
    'Open Data Commons Public Domain Dedication and License (PDDL)':'odc-pddl',
    'Other (Attribution)':'other-at',
    'Other (Non-Commercial)':'other-nc',
    'Other (Not Open)':'other-closed',
    'Other (Open)':'other-open',
    'Other (Public Domain)':'other-pd',
    'UK Open Government Licence (OGL)':'uk-ogl',

    # add more to complete the list
    'U.S. Public Domain Works':'us-pd',
    'www.usa.gov/publicdomain/label/1.0':'us-pd',

    # url (without protocol and trailing slash ) can also be used as key
    'creativecommons.org/licenses/by/4.0':'cc-by',
    'creativecommons.org/licenses/by-sa/4.0':'cc-by-sa',
    'creativecommons.org/publicdomain/zero/1.0':'cc-zero',
    'creativecommons.org/licenses/by-nc/4.0':'cc-nc',
    'www.gnu.org/copyleft/fdl.html':'gfdl',
    'opendatacommons.org/licenses/by/1-0':'odc-by',
    'opendatacommons.org/licenses/odbl':'odc-odbl',
    'opendatacommons.org/licenses/pddl':'odc-pddl',
    'project-open-data.cio.gov/unknown-license/#v1-legacy/other-at':'other-at',
    'project-open-data.cio.gov/unknown-license/#v1-legacy/other-nc':'other-nc',
    'project-open-data.cio.gov/unknown-license/#v1-legacy/other-closed':'other-closed',
    'project-open-data.cio.gov/unknown-license/#v1-legacy/other-open':'other-open',
    'creativecommons.org/publicdomain/mark/1.0/other-pd':'other-pd',
    'www.nationalarchives.gov.uk/doc/open-government-licence/version/3':'uk-ogl'
  }

CKAN_API_KEY = ''
CKAN_BASE_URL = ''  # http://localhost:5000
#e.g. '8d4de31c-979c-4b50-be6b-ea3c72453ff6' is the Dep Energy US Gov ar catalog.data.gov
HARVEST_SOURCE_ID = ''
CKAN_ORG_ID = ''  # create in you local instance and get the ID
CKAN_VALID_USER_ID = ''  # string, something like 13373386-636f-420f-aff0-0102087bfa28

# replace with you local values in this ignored file
try:
  from .local_settings import *
except:
  pass


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
