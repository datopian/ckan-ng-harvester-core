from harvester.adapters.ckan_dataset_adapters import CKANDatasetAdapter
from harvester.logs import logger
from slugify import slugify
from urllib.parse import urlparse
import json
from harvester.adapters.resources.csw import CSWResource
from harvester.settings import ckan_settings


class CSWDataset(CKANDatasetAdapter):
    ''' CSW dataset '''

    # check the get_package_dict function
    # https://github.com/GSA/ckanext-spatial/blob/2a25f8d60c31add77e155c4136f2c0d4e3b86385/ckanext/spatial/harvesters/base.py#L169

    ckan_owner_org_id = None  # required, the client must inform which existing org

    def __init__(self, original_dataset, schema='default'):
        super().__init__(original_dataset, schema=schema)
        self.mapped_fields = self.get_field_mapping(schema=schema)
        self.load_default_values(schema=schema)

    def get_field_mapping(self, schema='default'):

        default_fields = {
            'name': 'name',
            'title': 'title',
            'tags': 'tags',
            'abstract': 'notes',
            'progress': 'extras__progress',
            'resource-type': 'extras__resource-type',

            # Essentials
            'spatial-reference-system': 'extras__spatial-reference-system',
            'guid': 'extras__guid',
            # Usefuls
            'dataset-reference-date': 'extras__dataset-reference-date',
            'metadata-language': 'extras__metadata-language',  # Language
            'metadata-date': 'extras__metadata-date',  # Released
            'coupled-resource': 'extras__coupled-resource',
            'contact-email': 'extras__contact-email',
            'frequency-of-update': 'extras__frequency-of-update',
            'spatial-data-service-type': 'extras__spatial-data-service-type',

            'limitations-on-public-access': 'extras__access_constraints',
            'harvest_ng_source_title': 'extras__harvest_source_title',
            'harvest_ng_source_id': 'extras__harvest_source_id',
            'harvest_source_title': 'extras__harvest_source_title',
            'harvest_source_id': 'extras__harvest_source_id',
            'source_hash': 'extras__source_hash',

            'use-constraints': 'extras__licence',
            }
        if schema == 'usmetadata':
            default_fields = self.upgrade_usmetadata_default_fields(default_fields)

        return default_fields

    def load_default_values(self, schema='default'):

        defvalues = {}
        if schema == 'usmetadata':
            newdefs = {
                'accessLevel': 'public',
                'bureauCode': '000:00',
                'programCode': '000:000',
                'spatial': '{"type": "Point", "coordinates": (0.0, 0.0)}',
                'dataDictionary': 'http://missing.data.dictionary.com',
                'dataQuality': 'false',
                'accrualPeriodicity': 'irregular',
                'primaryITInvestmentUII': '000-000000000',
                'systemOfRecords': None,
                'publisher': 'no data',
                'tags': ['no tags'],
                }

            defvalues.update(newdefs)

            for key, value in defvalues.items():
                if key not in self.original_dataset:
                    self.original_dataset[key] = value
                elif self.original_dataset[key] == '':
                    self.original_dataset[key] = value

    def upgrade_usmetadata_default_fields(self, default_fields):
        # if endswith [] means it contains a list and must be = ','.join(value)
        default_fields['metadata-date'] = 'modified'
        # TODO check for the "date-released", "date-updated" and "date-created" fields
        default_fields['guid'] = 'unique_id'
        default_fields['accessLevel'] = 'public_access_level'
        default_fields['contact-email'] = 'contact_email'
        default_fields['publisher'] = 'publisher'
        default_fields['contact'] = 'contact_name'
        default_fields['url'] = 'homepage_url'
        default_fields['language'] = 'language'

        # we need to get this fields
        default_fields['bureauCode'] = 'bureau_code'
        default_fields['programCode'] = 'program_code'
        default_fields['spatial'] = 'spatial'
        default_fields['temporal'] = 'temporal'
        default_fields['dataDictionary'] = 'data_dictionary'
        default_fields['dataQuality'] = 'data_quality'
        default_fields['accrualPeriodicity'] = 'accrual_periodicity'
        default_fields['primaryITInvestmentUII'] = 'primary_it_investment_uii'
        default_fields['systemOfRecords'] = 'system_of_records'

        return default_fields

    def fix_fields(self, field, value):
        # some fields requires extra work
        if field == 'tags':
            return self.build_tags(value)
        elif field in ['extras__progress', 'extras__resource-type']:  # previous harvester take just the first one
            if type(value) == list and len(value) > 0:
                return value[0]
            else:
                return ''
        elif field == 'accrual_periodicity':
            if value is None or value == 'irregular' or value == '':
                value = 'unknown'
            else:
                return self.get_accrual_periodicity(value, reverse=True)
        elif field == 'extras__dataset-reference-date':
            # we expect someting like
            # [{'type': 'publication','value': '2010-12-01T12:00:00Z'}]
            if type(value) == list:
                v0 = value[0]
                if type(v0) == dict:
                    return v0.get('value', None)
            else:
                return value
        elif field in ['extras__access_constraints', 'extras__coupled-resource']:
            return str(value) if type(value) == list else value
        else:
            return value

    def validate_origin_dataset(self):
        # check required https://docs.ckan.org/en/2.8/api/#ckan.logic.action.create.package_create

        if self.ckan_owner_org_id is None:
            return False, 'Owner organization ID is required'

        return True, None

    def transform_to_ckan_dataset(self, existing_resources=None):

        valid, error = self.validate_origin_dataset()
        if not valid:
            raise Exception(f'Error validating origin dataset: {error}')

        dataset = self.original_dataset.get('iso_values', {})
        tags = dataset.get('tags', ['no tags'])
        self.ckan_dataset['tag_string'] = ','.join(tags)

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

        self.infer_resources()
        self.ckan_dataset['resources'] = self.transform_resources()

        # custom changes
        self.fix_licence_url()
        self.set_browse_graphic()
        self.set_temporal_extent()
        self.set_responsible_party()
        self.set_bbox()

        # define name (are uniques in CKAN instance)
        if 'name' not in self.ckan_dataset or self.ckan_dataset['name'] == '':
            self.ckan_dataset['name'] = self.generate_name(title=self.ckan_dataset['title'])

        # mandatory
        self.ckan_dataset['owner_org'] = self.ckan_owner_org_id

        # clean all empty unused values (can't pop keys while iterating)
        ckan_dataset_copy = self.ckan_dataset.copy()
        for k, v in self.ckan_dataset.items():
            if v is None:
                ckan_dataset_copy.pop(k)
        self.ckan_dataset = ckan_dataset_copy

        valid = self.validate_final_dataset()
        if not valid:
            raise Exception(f'Error validating final dataset: {self.errors} from {self.original_dataset}')

        logger.info('Dataset transformed {} OK'.format(self.original_dataset.get('identifier', '')))
        return self.ckan_dataset

    def infer_resources(self):
        # TODO move to the CSWResource adapter since there is no one-to-one resource relationship
        # extract info about internal resources
        self.resources = []

        # the way that previous harvester work is complex
        # https://github.com/GSA/ckanext-spatial/blob/2a25f8d60c31add77e155c4136f2c0d4e3b86385/ckanext/spatial/harvesters/base.py#L350-L411
        # That plugin uses these four elements

        resource_locator_groups = self.original_dataset.get('resource-locator-group', [])
        distributor_data_format = self.original_dataset.get('distributor-data-format', '')
        distribution_data_formats = self.original_dataset.get('distribution-data-format', [])

        total_data_formats = len(distribution_data_formats)

        zipit = False
        if distributor_data_format != '':
            universal_format = distributor_data_format
        elif total_data_formats == 1:
            universal_format = distribution_data_formats[0]
        elif total_data_formats == 0:
            universal_format = None
        elif total_data_formats != len(resource_locator_groups):
            universal_format = None
        else:
            zipit = True

        if zipit:
            resource_locator_group_data_format = zip(resource_locator_groups, distribution_data_formats)
        else:
            # rlg: resource_locator_group
            rldf = [(rlg, universal_format) for rlg in resource_locator_groups]
            resource_locator_group_data_format = rldf

        # we need to read more but we have two kind of resources
        for resource in resource_locator_group_data_format:
            res = {'type': 'resource_locator_group_data_format', 'data': resource}
            self.resources.append(res)

        resource_locators = self.original_dataset.get('resource-locator-identification', [])

        for resource in resource_locators:
            res = {'type': 'resource_locator', 'data': resource}
            self.resources.append(res)

        return self.resources

    def transform_resources(self):
        ''' Transform this resources in list of resources '''

        resources = []
        for original_resource in self.resources:
            cra = CSWResource(original_resource=original_resource)
            resource_transformed = cra.transform_to_ckan_resource()
            if resource_transformed is not None:
                resources.append(resource_transformed)
            else:
                # TODO is this an error?
                pass

        self.resources = resources
        return self.resources

    def set_bbox(self):
        bbx = self.original_dataset.get('bbox', None)
        if bbx is None:
            self.set_extra('spatial', None)
            return

        if type(bbx) != list or len(bbx) == 0:
            self.set_extra('spatial', None)
            return

        bbox = bbx[0]
        self.set_extra('bbox-east-long', bbox['east'])
        self.set_extra('bbox-north-lat', bbox['north'])
        self.set_extra('bbox-south-lat', bbox['south'])
        self.set_extra('bbox-west-long', bbox['west'])

        try:
            xmin = float(bbox['west'])
            xmax = float(bbox['east'])
            ymin = float(bbox['south'])
            ymax = float(bbox['north'])
        except ValueError as e:
            self.set_extra('spatial', None)
        else:
            # Construct a GeoJSON extent so ckanext-spatial can register the extent geometry

            # Some publishers define the same two corners for the bbox (ie a point),
            # that causes problems in the search if stored as polygon
            if xmin == xmax or ymin == ymax:
                extent_string = '{"type": "Point", "coordinates": [{}, {}]}'.format(xmin, ymin)
            else:
                coords = '[[[{xmin}, {ymin}], [{xmax}, {ymin}], [{xmax}, {ymax}], [{xmin}, {ymax}], [{xmin}, {ymin}]]]'.format(xmin=xmin, ymin=ymin, xmax=xmax, ymax=ymax)
                extent_string = '{{"type": "Polygon", "coordinates": {coords}}}'.format(coords=coords)

            self.set_extra('spatial', extent_string.strip())

    def set_responsible_party(self):
        ro = self.original_dataset.get('responsible-organisation', None)
        if ro is None:
            return

        parties = {}
        for party in ro:
            if party['organisation-name'] in parties:
                if not party['role'] in parties[party['organisation-name']]:
                    parties[party['organisation-name']].append(party['role'])
            else:
                parties[party['organisation-name']] = [party['role']]

        rp = [{'name': k, 'roles': v} for k, v in parties.items()]

        ret = ['{} ({})'.format(r['name'], ', '.join(r['roles'])) for r in rp]

        self.set_extra('responsible-party', '; '.join(ret))

    def fix_licence_url(self):
        # https://github.com/GSA/ckanext-spatial/blob/2a25f8d60c31add77e155c4136f2c0d4e3b86385/ckanext/spatial/harvesters/base.py#L278
        licences = self.get_extra('licence')
        if licences != '' and licences is not None:
            if type(licences) == list:
                for licence in licences:
                    u = urlparse(licence)
                    if u.scheme and u.netloc:
                        self.set_extra(key='licence_url', value=licence)
                self.set_extra(key='licence', value=str(licences))

    def set_browse_graphic(self):
        browse_graphic = self.original_dataset.get('browse-graphic', None)
        if browse_graphic is None:
            return

        if type(browse_graphic) != list or len(browse_graphic) == 0:
            return

        browse_graphic = browse_graphic[0]
        pf = browse_graphic.get('file', None)
        if pf is not None:
            self.set_extra('graphic-preview-file', pf)

        descr = browse_graphic.get('description', None)
        if descr is not None:
            self.set_extra('graphic-preview-description', descr)

        pt = browse_graphic.get('type', None)
        if pt is not None:
            self.set_extra('graphic-preview-type', pt)

    def set_temporal_extent(self):
        for key in ['temporal-extent-begin', 'temporal-extent-end']:
            te = self.original_dataset.get(key, None)
            if te is not None:
                if type(te) == list and len(te) > 0:
                    self.set_extra(key, te[0])
