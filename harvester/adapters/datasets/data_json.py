from harvester.adapters.ckan_dataset_adapters import CKANDatasetAdapter
from harvester.logs import logger
from slugify import slugify
import json
from harvester.adapters.resources.data_json import DataJSONDistribution
from harvester.settings import ckan_settings


class DataJSONSchema1_1(CKANDatasetAdapter):
    ''' Data.json dataset from Schema 1.1'''

    # https://github.com/GSA/ckanext-datajson/blob/07ca20e0b6dc1898f4ca034c1e073e0c27de2015/ckanext/datajson/harvester_base.py#L478
    # value at data.json -> value at CKAN dataset

    ckan_owner_org_id = None  # required, the client must inform which existing org

    def __init__(self, original_dataset, schema='default'):
        super().__init__(original_dataset, schema=schema)
        self.mapped_fields = self.get_field_mapping(schema=schema)
        self.load_default_values(schema=schema)

    def get_field_mapping(self, schema='default'):

        default_fields = {
            'name': 'name',
            'title': 'title',
            'description': 'notes',
            'keyword': 'tags',
            'modified': 'extras__modified',  # ! revision_timestamp
            # requires extra work 'publisher': 'extras__publisher',  # !owner_org
            'contactPoint__fn': 'maintainer',
            'contactPoint__hasEmail': 'maintainer_email',
            'identifier': 'extras__identifier',  # !id
            'accessLevel': 'extras__accessLevel',
            'bureauCode': 'extras__bureauCode',
            'programCode': 'extras__programCode',
            'rights': 'extras__rights',
            'license': 'extras__license',  # !license_id
            'spatial': 'extras__spatial',  # Geometry not valid GeoJSON, not indexing
            'temporal': 'extras__temporal',
            'theme': 'extras__theme',
            'dataDictionary': 'extras__dataDictionary',  # !data_dict
            'dataQuality': 'extras__dataQuality',
            'accrualPeriodicity':'extras__accrualPeriodicity',
            'landingPage': 'extras__landingPage',
            'language': 'extras__language',
            'primaryITInvestmentUII': 'extras__primaryITInvestmentUII',  # !PrimaryITInvestmentUII
            'references': 'extras__references',
            'issued': 'extras__issued',
            'systemOfRecords': 'extras__systemOfRecords',
            # 'distribution': 'resources'  # transformed with a custom adapter

            'harvest_ng_source_title': 'extras__harvest_source_title',
            'harvest_ng_source_id': 'extras__harvest_source_id',

            'harvest_source_title': 'extras__harvest_source_title',
            'harvest_source_id': 'extras__harvest_source_id',
            'source_schema_version': 'extras__source_schema_version',  # 1.1 or 1.0
            'source_hash': 'extras__source_hash',

            'catalog_@context': 'extras__catalog_@context',
            'catalog_@id': 'extras__catalog_@id',
            'catalog_conformsTo': 'extras__catalog_conformsTo',
            'catalog_describedBy': 'extras__catalog_describedBy',

            'is_collection': 'extras__is_collection',
            'collection_pkg_id': 'extras__collection_package_id',  # don't like pkg vs package
        }

        if schema == 'usmetadata':
            default_fields = self.upgrade_usmetadata_default_fields(default_fields)

        return default_fields

    def load_default_values(self, schema='default'):

        defvalues = {}
        if schema == 'usmetadata':
            newdefs = {'accessLevel': 'public'}
            defvalues.update(newdefs)

            for key, value in defvalues.items():
                if key not in self.original_dataset:
                    self.original_dataset[key] = value
                elif self.original_dataset[key] == '':
                    self.original_dataset[key] = value

    def upgrade_usmetadata_default_fields(self, default_fields):
        # if endswith [] means it contains a list and must be = ','.join(value)
        default_fields['modified'] = 'modified'
        default_fields['publisher'] = 'publisher'
        default_fields['contactPoint__fn'] = 'contact_name'
        default_fields['contactPoint__hasEmail'] = 'contact_email'
        default_fields['identifier'] = 'unique_id'
        default_fields['accessLevel'] = 'public_access_level'
        default_fields['bureauCode'] = 'bureau_code'
        default_fields['programCode'] = 'program_code'
        default_fields['spatial'] = 'spatial'
        default_fields['temporal'] = 'temporal'
        default_fields['dataDictionary'] = 'data_dictionary'
        default_fields['dataQuality'] = 'data_quality'
        default_fields['accrualPeriodicity'] = 'accrual_periodicity'
        default_fields['landingPage'] = 'homepage_url'
        default_fields['language'] = 'language'
        default_fields['primaryITInvestmentUII'] = 'primary_it_investment_uii'
        default_fields['systemOfRecords'] = 'system_of_records'

        return default_fields

    def validate_origin_dataset(self):
        # check required https://docs.ckan.org/en/2.8/api/#ckan.logic.action.create.package_create

        if self.ckan_owner_org_id is None:
            error = 'Owner organization ID is required'
            self.errors.append(error)
            return False

        requireds = []

        if self.schema == 'usmetadata':
            requireds += ['accessLevel', 'identifier',
                          'contactPoint__fn', 'programCode',
                          'bureauCode', 'contactPoint__hasEmail',
                          'publisher', 'modified', 'keyword']

        ok = True
        for req in requireds:
            # read fields considering the __ separator
            identified = self.identify_origin_element(raw_field=req)
            if identified in [None, '']:
                error = f'"{req}" field could not be empty at origin dataset'
                self.errors.append(error)
                ok = False

        if not ok:
            logger.info(f'requires failed on {self.original_dataset}: {self.errors}')
        return ok

    def fix_fields(self, field, value):
        # some fields requires extra work
        if field == 'tags':
            return self.build_tags(value)
        elif field in ['contact_email', 'maintainer_email']:  # TODO schemas need to be separated
            if value.startswith('mailto:'):
                value = value.replace('mailto:', '')
            return value
        # for usmetadataschema elif field in ['bureau_code', 'program_code', 'language', 'extras__bureauCode']:
        elif field in ['extras__bureauCode', 'extras__programCode']:
            if type(value) == list:
                value = ','.join(value)
            return value
        elif field == 'accrual_periodicity':
            return self.get_accrual_periodicity(value, reverse=True)
        else:
            return value

    def infer_resources(self):
        # if _distribution_ is empty then we try to create them from "accessURL" or "webService" URLs
        datajson_dataset = self.original_dataset
        distribution = []
        for field in ['accessURL', 'webService']:
            url = datajson_dataset.get(field, '').strip()

            if url != '':
                fmt = datajson_dataset.get('format', '')
                distribution.append({field: url, 'format': fmt, 'mimetype': fmt})

        return distribution

    def transform_resources(self, distribution):
        ''' Transform the distribution list in list of resources '''
        if type(distribution) == dict:
            distribution = [distribution]

        resources = []
        for original_resource in distribution:
            try:
                cra = DataJSONDistribution(original_resource=original_resource)
                resource_transformed = cra.transform_to_ckan_resource()
            except Exception as e:
                resource_transformed = {'error': e}
            resources.append(resource_transformed)

        return resources

    def transform_to_ckan_dataset(self, existing_resources=None):
        # check how to parse
        # https://github.com/GSA/ckanext-datajson/blob/07ca20e0b6dc1898f4ca034c1e073e0c27de2015/ckanext/datajson/parse_datajson.py#L5
        # if we are updating existing dataset we need to merge resources

        logger.info('Transforming data.json dataset {}'.format(self.original_dataset.get('identifier', '')))
        valid = self.validate_origin_dataset()
        if not valid:
            # raise Exception(f'Error validating origin dataset: {error}')
            return None

        datajson_dataset = self.original_dataset
        self.ckan_dataset['tag_string'] = ','.join(datajson_dataset.get('keyword', []))

        # previous transformations at origin
        for old_field, field_ckan in self.mapped_fields.items():
            logger.debug(f'Connecting fields "{old_field}", "{field_ckan}"')
            # identify origin and set value to destination
            origin = self.identify_origin_element(raw_field=old_field)
            if origin is None:
                logger.debug(f'No data in origin for "{old_field}"')
            else:
                self.set_destination_element(raw_field=field_ckan, new_value=origin)
                logger.debug(f'Connected OK fields "{old_field}"="{origin}"')

        # transform distribution into resources
        distribution = datajson_dataset['distribution'] if 'distribution' in datajson_dataset else []
        # if _distribution_ is empty then we try to create them from "accessURL" or "webService" URLs
        if distribution is None or distribution == []:
            distribution = self.infer_resources()

        self.ckan_dataset['resources'] = self.transform_resources(distribution)

        # move out the resources with validation errores
        # and log the error as a dataset error
        final_resources = []
        for resource in self.ckan_dataset['resources']:
            if 'error' in resource:
                self.errors.append(resource)
            else:
                final_resources.append(resource)
        self.ckan_dataset['resources'] = final_resources

        if existing_resources is not None:
            res = self.merge_resources(existing_resources=existing_resources, new_resources=self.ckan_dataset['resources'])
            self.ckan_dataset['resources'] = res

        # add custom extras
        # add source_datajson_identifier = {"key": "source_datajson_identifier", "value": True}
        self.set_destination_element(raw_field='extras__source_datajson_identifier', new_value=True)

        # define name (are uniques in CKAN instance)
        if 'name' not in self.ckan_dataset or self.ckan_dataset['name'] == '':
            name = self.generate_name(title=self.ckan_dataset['title'])
            self.ckan_dataset['name'] = name

        # mandatory
        self.ckan_dataset['owner_org'] = self.ckan_owner_org_id

        # check for license
        if datajson_dataset.get('license', None) not in [None, '']:
            original_license = datajson_dataset['license']
            original_license = original_license.replace('http://', '')
            original_license = original_license.replace('https://', '')
            original_license = original_license.rstrip('/')
            license_id = ckan_settings.LICENCES.get(original_license, "other-license-specified")
            self.ckan_dataset['license_id'] = license_id

        # define publisher as extras as we expect
        publisher = datajson_dataset.get('publisher', None)
        if publisher is not None:
            publisher_name = publisher.get('name', '')
            
            # TODO check which place we are going to use 
            self.set_extra('publisher', publisher_name)
            # self.ckan_dataset['publisher'] = publisher_name

            parent_publisher = publisher.get('subOrganizationOf', None)
            if parent_publisher is not None:
                publisher_hierarchy = [publisher_name]
                while parent_publisher:
                    parent_name = parent_publisher.get('name', '')
                    parent_publisher = parent_publisher.get('subOrganizationOf', None)
                    publisher_hierarchy.append(parent_name)

                publisher_hierarchy.reverse()
                publisher_hierarchy = " > ".join(publisher_hierarchy)
                self.set_extra('publisher_hierarchy', publisher_hierarchy)

        # clean all empty unused values (can't pop keys while iterating)
        ckan_dataset_copy = self.ckan_dataset.copy()
        for k, v in self.ckan_dataset.items():
            if v is None:
                ckan_dataset_copy.pop(k)
        self.ckan_dataset = ckan_dataset_copy

        valid = self.validate_final_dataset()
        if valid is None:
            return None

        logger.info('Dataset transformed {} OK'.format(self.original_dataset.get('identifier', '')))
        return ckan_dataset_copy

    def merge_resources(self, existing_resources, new_resources):
        # if we are updating datasets we need to check if the resources exists and merge them
        # https://github.com/GSA/ckanext-datajson/blob/07ca20e0b6dc1898f4ca034c1e073e0c27de2015/ckanext/datajson/harvester_base.py#L681

        merged_resources = []

        for res in new_resources:
            for existing_res in existing_resources:
                if res["url"] == existing_res["url"]:
                    # in CKAN exts maybe the have an ID because a local show
                    res["id"] = existing_res["id"]
            merged_resources.append(res)

        return merged_resources