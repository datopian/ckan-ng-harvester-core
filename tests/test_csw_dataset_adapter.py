import pytest
from harvester.adapters.datasets.csw import CSWDataset


class TestCKANDatasetAdapter(object):

    test_dataset = {
            'title': 'CSW dataset',
            'abstract': 'Some notes about this dataset. Bla, bla, bla',
            'tags': ['Electrity', 'Nuclear energy', 'Investment'],

            'spatial-reference-system': 'EPSG:27700',
            'guid' : 'unique ID 971897198',
            # Usefuls
            'dataset-reference-date': [{
                    'type': 'publication',
                    'value': '2010-12-01T12:00:00Z'
                }],
            'metadata-language': 'en',
            'metadata-date': '2019-02-02',
            'coupled-resource': 'coup res',
            'contact-email': 'some@email.com',
            'frequency-of-update': 'WEEKLY',
            'spatial-data-service-type': 'other',
            'use-constraints': ['CC-BY', 'http://licence.com'],
            'browse-graphic': [
                {
                    'file': 'some',
                    'description': 'some descr',
                    'type': 'some type'
                }
                ],
            'temporal-extent-begin': ['teb1', 'teb2'],
            'temporal-extent-end': ['tee1', 'tee2'],
            'responsible-organisation': [
                {'organisation-name': 'GSA','role': 'admin'},
                {'organisation-name': 'GSA','role': 'admin2'},
                {'organisation-name': 'NASA','role': 'moon'}
            ],
            'bbox': [
                {'east': -61.9, 'north': -33.1, 'west': 34.3, 'south': 51.8}
            ]

        }

    def test_csw_to_ckan(self):

        dst = CSWDataset(original_dataset=self.test_dataset)
        # ORG is required!
        dst.ckan_owner_org_id = 'XXXX'

        ckan_dataset = dst.transform_to_ckan_dataset()

        assert ckan_dataset['owner_org'] == 'XXXX'
        assert ckan_dataset['notes'] == 'Some notes about this dataset. Bla, bla, bla'
        # TODO assert len(ckan_dataset['resources']) == 2
        # TODO assert ckan_dataset['maintainer_email'] == 'Fred.Teensma@ams.usda.gov'
        assert len(ckan_dataset['tags']) == 3
        # TODO assert ckan_dataset['license_id'] == 'cc-by'  # transformation

        # test *Code
        # TODO assert [['005:45']] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'bureauCode']
        # TODO assert [['005:047']] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'programCode']

        assert ['EPSG:27700'] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'spatial-reference-system']
        assert ['unique ID 971897198'] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'guid']
        assert ['other'] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'spatial-data-service-type']
        assert ['WEEKLY'] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'frequency-of-update']
        assert ['some@email.com'] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'contact-email']
        assert ['coup res'] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'coupled-resource']
        assert ['2019-02-02'] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'metadata-date']
        assert ['en'] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'metadata-language']
        assert ['2010-12-01T12:00:00Z'] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'dataset-reference-date']

        assert ["['CC-BY', 'http://licence.com']"] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'licence']
        assert ['http://licence.com'] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'licence_url']

        assert ['some'] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'graphic-preview-file']
        assert ['some descr'] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'graphic-preview-description']
        assert ['some type'] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'graphic-preview-type']

        assert ['teb1'] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'temporal-extent-begin']
        assert ['tee1'] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'temporal-extent-end']

        # rp = [{'name': 'GSA', 'roles': ['admin', 'admin2']}, {'name': 'NASA', 'roles': ['moon']}]
        rp = 'GSA (admin, admin2); NASA (moon)'
        assert [rp] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'responsible-party']

        coords = '[[[{xmin}, {ymin}], [{xmax}, {ymin}], [{xmax}, {ymax}], [{xmin}, {ymax}], [{xmin}, {ymin}]]]'.format(xmax=-61.9, ymax=-33.1, xmin=34.3, ymin=51.8)
        spatial = '{{"type": "Polygon", "coordinates": {coords}}}'.format(coords=coords)

        assert [spatial] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'spatial']

    def test_collections(self):
        pass

    def test_required_fields(self):

        dataset = self.test_dataset
        # drop required keys
        dst = CSWDataset(original_dataset=dataset)
        # ORG is required!

        with pytest.raises(Exception):
            ckan_dataset = dst.transform_to_ckan_dataset()

        dst.ckan_owner_org_id = 'XXXX'
        ckan_dataset = dst.transform_to_ckan_dataset()
        del ckan_dataset['name']

        ret = dst.validate_final_dataset()
        assert ret == False
        assert '"name" is a required field' in ','.join(dst.errors)

    def test_resources(self):
        dst = CSWDataset(original_dataset=self.test_dataset)

        pass