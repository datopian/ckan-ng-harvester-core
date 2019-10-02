''' transform datasets to CKAN datasets '''
from slugify import slugify
from abc import ABC, abstractmethod
from harvester.settings import ckan_settings


class CKANDatasetAdapter(ABC):
    ''' transform other datasets objects into CKAN datasets '''

    def __init__(self, original_dataset, schema='default'):
        self.schema = schema
        self.original_dataset = original_dataset
        self.required = ['name', 'private']
        self.ckan_dataset = self.get_base_ckan_dataset(schema=schema)
        self.errors = []

    def get_base_ckan_dataset(self, schema='default'):
        # creates the Dict base for a base CKAN dataset
        # Check for required fields: https://docs.ckan.org/en/2.8/api/#ckan.logic.action.create.package_create
        # This is the official version.
        # Note that some CKAN extensions modify this schemas

        pkg = {
            'name': '',  # no spaces, just lowercases, - and _
            'title': '',
            'owner_org': '',  # (string) – the id of the dataset’s owning organization, see organization_list() or organization_list_for_user() for available values. This parameter can be made optional if the config option ckan.auth.create_unowned_dataset is set to True.
            'private': False,
            'author': None,  # (string) – the name of the dataset’s author (optional)
            'author_email': None,  # (string) – the email address of the dataset’s author (optional)
            'maintainer': None,  # (string) – the name of the dataset’s maintainer (optional)
            'maintainer_email': None,  # (string) – the email address of the dataset’s maintainer (optional)
            # just aded when license exists
            # 'license_id': None,  # (license id string) – the id of the dataset’s license, see license_list() for available values (optional)
            'notes':  None,  # (string) – a description of the dataset (optional)
            'url': None,  # (string) – a URL for the dataset’s source (optional)
            'version': None,  # (string, no longer than 100 characters) – (optional)
            'state': 'active',  # (string) – the current state of the dataset, e.g. 'active' or 'deleted'
            'type': None,  # (string) – the type of the dataset (optional), IDatasetForm plugins associate themselves with different dataset types and provide custom dataset handling behaviour for these types
            'resources': None,  # (list of resource dictionaries) – the dataset’s resources, see resource_create() for the format of resource dictionaries (optional)
            'tags': None,  # (list of tag dictionaries) – the dataset’s tags, see tag_create() for the format of tag dictionaries (optional)
            'extras': [  # (list of dataset extra dictionaries) – the dataset’s extras (optional), extras are arbitrary (key: value) metadata items that can be added to datasets, each extra dictionary should have keys 'key' (a string), 'value' (a string)
                {'key': 'resource-type', 'value': 'Dataset'}
            ],
            'relationships_as_object': None,  # (list of relationship dictionaries) – see package_relationship_create() for the format of relationship dictionaries (optional)
            'relationships_as_subject': None,  # (list of relationship dictionaries) – see package_relationship_create() for the format of relationship dictionaries (optional)
            'groups': None,  # (list of dictionaries) – the groups to which the dataset belongs (optional), each group dictionary should have one or more of the following keys which identify an existing group: 'id' (the id of the group, string), or 'name' (the name of the group, string), to see which groups exist call group_list()
        }

        if schema == 'usmetadata':
            pkg = self.upgrade_usmetadata_schema(pkg)
            self.upgrade_usmetadata_required()

        return pkg

    def upgrade_usmetadata_required(self):
        # read about usmetadata schema here
        # https://github.com/GSA/USMetadata/blob/master/ckanext/usmetadata/plugin.py

        required = ['public_access_level', 'unique_id',
                    'contact_name', 'program_code',
                    'bureau_code', 'contact_email',
                    'publisher', 'modified',
                    'tag_string']
        self.required += required

    def upgrade_usmetadata_schema(self, pkg):
        to_remove = ['maintainer', 'maintainer_email']
        for k in to_remove:
            pkg.pop(k, None)

        new_keys = {
            'contact_name': None,
            'contact_email': None,
            'modified': None,
            'publisher': None,
            'public_access_level': None,
            'homepage_url': None,
            'unique_id': None,
            'contact_name': None,
            'spatial': None,
            'program_code': None,
            'bureau_code': None,
            'tag_string': None,
            'data_quality': None,
            'data_dictionary': None,
            'accrual_periodicity': None,
            'temporal': None,
            'system_of_records': None,
            'primary_it_investment_uii': None,
            'language': None,
            }

        pkg.update(new_keys)

        return pkg

    @abstractmethod
    def transform_to_ckan_dataset(self):
        pass

    def identify_origin_element(self, raw_field):
        # get the original value in original dict (the one to convert) to put in CKAN dataset.
        # Consider the __ separator
        # in 'contactPoint__hasEmail' gets in_dict['contactPoint']['hasEmail'] if exists
        # in 'licence' gets in_dict['licence'] if exists

        parts = raw_field.split('__')
        if parts[0] not in self.original_dataset:
            return None
        origin = self.original_dataset[parts[0]]
        if len(parts) > 1:
            for part in parts[1:]:
                if part in origin:
                    origin = origin[part]
                else:  # drop
                    return None
        return origin

    def validate_final_dataset(self):
        # check required https://docs.ckan.org/en/2.8/api/#ckan.logic.action.create.package_create
        # somo extensions changes this

        ok = True
        for req in self.required:
            if req not in self.ckan_dataset:
                error = f'"{req}" is a required field'
                self.errors.append(error)
                ok = False
            elif self.ckan_dataset[req] in [None, '']:
                error = f'"{req}" field could not be empty'
                self.errors.append(error)
                ok = False

        return ok

    def set_destination_element(self, raw_field, new_value):
        # in 'extras__issued' gets or creates self.ckan_dataset[extras][key][issued] and assing new_value to self.ckan_dataset[extras][value]
        # in 'title' assing new_value to self.ckan_dataset[title]
        # returns dict modified

        parts = raw_field.split('__')
        if parts[0] not in self.ckan_dataset:
            raise Exception('Not found field "{}" at CKAN destination dict'.format(parts[0]))
        if len(parts) == 1:
            # check if need to be fixed
            self.ckan_dataset[raw_field] = self.fix_fields(field=raw_field,
                                                             value=new_value)
            return self.ckan_dataset
        elif len(parts) == 2:
            if parts[0] != 'extras':
                raise Exception(f'Unknown field estructure: "{raw_field}" at CKAN destination dict')

            # check if extra already exists
            for extra in self.ckan_dataset['extras']:

                key = parts[1]
                if extra['key'] == key:
                    # check if need to be fixed
                    extra['value'] = self.fix_fields(field=f'extras__{key}', value=new_value)
                    return self.ckan_dataset

            key = parts[1]
            # this extra do not exists already
            new_extra = {'key': key, 'value': None}
            # check if need to be fixed
            new_extra['value'] = self.fix_fields(field=f'extras__{key}', value=new_value)
            self.ckan_dataset['extras'].append(new_extra)
            return self.ckan_dataset
        else:
            raise Exception(f'Unknown fields length estructure for "{raw_field}" at CKAN destination dict')

    def build_tags(self, tags):
        # create a CKAN tag
        # Help https://docs.ckan.org/en/2.8/api/#ckan.logic.action.create.tag_create
        ret = []
        for tag in tags:
            tag = tag.strip()
            if tag != '':
                tag = slugify(tag[:ckan_settings.MAX_TAG_NAME_LENGTH])
                ret.append({"name": tag})
        return ret

    def set_extra(self, key, value):
        found = False
        for extra in self.ckan_dataset['extras']:
            if extra['key'] == key:
                extra['value'] = value
                found = True
        if not found:
            self.ckan_dataset['extras'].append({'key': key, 'value': value})
        return self.ckan_dataset

    def get_extra(self, key):
        for extra in self.ckan_dataset['extras']:
            if extra['key'] == key:
                return extra['value']
        return None

    def generate_name(self, title):
        # names are unique in CKAN
        # old harvester do like this: https://github.com/GSA/ckanext-datajson/blob/07ca20e0b6dc1898f4ca034c1e073e0c27de2015/ckanext/datajson/harvester_base.py#L747

        name = slugify(title)
        cut_at = ckan_settings.MAX_NAME_LENGTH - 5  # max length is 100
        if len(name) > cut_at:
            name = name[:cut_at]

        # TODO check if the name MUST be a new unexisting one
        # TODO check if it's an existing resource and we need to read previos name using the identifier

        return name

    def get_accrual_periodicity(self, value, reverse=True):
        accrual_periodicity_dict = {
                'completely irregular': 'irregular',
                'decennial': 'R/P10Y',
                'quadrennial': 'R/P4Y',
                'annual': 'R/P1Y',
                'bimonthly': 'R/P2M',  # or R/P0.5M
                'semiweekly': 'R/P3.5D',
                'daily': 'R/P1D',
                'biweekly': 'R/P2W',  # or R/P0.5W
                'semiannual': 'R/P6M',
                'biennial': 'R/P2Y',
                'triennial': 'R/P3Y',
                'three times a week': 'R/P0.33W',
                'three times a month': 'R/P0.33M',
                'continuously updated': 'R/PT1S',
                'monthly': 'R/P1M',
                'quarterly': 'R/P3M',
                'every five years': 'R/P5Y',
                'every eight years': 'R/P8Y',
                'semimonthly': 'R/P0.5M',
                'three times a year': 'R/P4M',
                'weekly': 'R/P1W',
                'hourly': 'R/PT1H',
                'continual': 'R/PT1S',
                'fortnightly': 'R/P0.5M',
                'annually': 'R/P1Y',
                'biannualy': 'R/P0.5Y',
                'asneeded': 'irregular',
                'irregular': 'irregular',
                'notplanned': 'irregular',
                'unknown': 'irregular',
                'not updated': 'irregular'
            }

        if reverse:
            accrual_periodicity_dict = {v: k for k, v in accrual_periodicity_dict.items()}

        return accrual_periodicity_dict.get(value, None)

