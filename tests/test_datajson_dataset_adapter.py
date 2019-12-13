import pytest
from harvesters.datajson.ckan.dataset import DataJSONSchema1_1

class TestCKANDatasetAdapter(object):

    def test_datajson_1_1_to_ckan(self, test_datajson_dataset):

        djss = DataJSONSchema1_1(original_dataset=test_datajson_dataset)
        # ORG is required!
        djss.ckan_owner_org_id = 'XXXX'

        ckan_dataset = djss.transform_to_ckan_dataset()

        assert ckan_dataset['owner_org'] == 'XXXX'
        assert ckan_dataset['notes'] == 'Some notes ...'
        assert len(ckan_dataset['resources']) == 2

        if djss.schema == 'usmetadata':
            assert ckan_dataset['contact_email'] == 'Fred.Teensma@ams.usda.gov'
            # test *Code
            assert ckan_dataset['bureau_code'] == '005:45'
            assert ckan_dataset['program_code'] == '005:047'
            assert ckan_dataset['publisher'] == 'Agricultural Marketing Service'
        else:
            assert ckan_dataset['maintainer_email'] == 'Fred.Teensma@ams.usda.gov'
            # test *Code
            # TODO check what we expect here
            # assert [['005:45']] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'bureauCode']
            # assert [['005:047']] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'programCode']
            assert ['005:45'] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'bureauCode']
            assert ['005:047'] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'programCode']
            # test publisher processor
            assert ['Agricultural Marketing Service'] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'publisher']

        assert len(ckan_dataset['tags']) == 2
        assert ckan_dataset['license_id'] == 'cc-by'  # transformation
        assert [] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'publisher_hierarchy']

        # test publisher subOrganizationOf
        t2 = test_datajson_dataset
        t2['publisher']['subOrganizationOf'] = {
                        "@type": "org:Organization",
                        "name": "Department of Agriculture"
                        }
        djss.original_dataset = t2
        ckan_dataset = djss.transform_to_ckan_dataset()

        if djss.schema == 'usmetadata':
            assert ckan_dataset['publisher'] == 'Agricultural Marketing Service'
        else:
            assert ['Agricultural Marketing Service'] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'publisher']

        assert ['Department of Agriculture > Agricultural Marketing Service'] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'publisher_hierarchy']

        t2['publisher']['subOrganizationOf']['subOrganizationOf'] = {
                        "@type": "org:Organization",
                        "name": "USA GOV"
                        }
        djss.original_dataset = t2
        ckan_dataset = djss.transform_to_ckan_dataset()

        if djss.schema == 'usmetadata':
            assert ckan_dataset['publisher'] == 'Agricultural Marketing Service'
        else:
            assert ['Agricultural Marketing Service'] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'publisher']

        assert ['USA GOV > Department of Agriculture > Agricultural Marketing Service'] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'publisher_hierarchy']

        t2 = test_datajson_dataset
        t2['harvest_source_id'] = 'XXXXX'

        djss.original_dataset = t2
        ckan_dataset = djss.transform_to_ckan_dataset()
        assert ['XXXXX'] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'harvest_source_id']

    def test_collections(self, test_datajson_dataset):
        djss = DataJSONSchema1_1(original_dataset=test_datajson_dataset, schema='usmetadata')
        # ORG is required!
        djss.ckan_owner_org_id = 'XXXX'
        ckan_dataset = djss.transform_to_ckan_dataset()
        assert [] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'is_collection']
        t2 = test_datajson_dataset
        t2['is_collection'] = True
        djss.original_dataset = t2
        ckan_dataset = djss.transform_to_ckan_dataset()
        assert [True] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'is_collection']

        assert [] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'collection_package_id']
        t2['collection_pkg_id'] = 'XXXXX'
        djss.original_dataset = t2
        ckan_dataset = djss.transform_to_ckan_dataset()
        assert ['XXXXX'] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'collection_package_id']

    def test_catalog_extras(self, test_datajson_dataset):
        djss = DataJSONSchema1_1(original_dataset=test_datajson_dataset, schema='usmetadata')
        # ORG is required!
        djss.ckan_owner_org_id = 'XXXX'
        ckan_dataset = djss.transform_to_ckan_dataset()

        t2 = test_datajson_dataset
        t2['catalog_@context'] = "https://project-open-data.cio.gov/v1.1/schema/catalog.jsonld"
        t2['catalog_describedBy']  = "https://project-open-data.cio.gov/v1.1/schema/catalog.json"
        t2['catalog_conformsTo'] = "https://project-open-data.cio.gov/v1.1/schema"
        t2['catalog_@id'] = 'https://healthdata.gov/data.json'

        djss.original_dataset = t2
        ckan_dataset = djss.transform_to_ckan_dataset()
        assert ["https://project-open-data.cio.gov/v1.1/schema/catalog.jsonld"] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'catalog_@context']
        assert ["https://project-open-data.cio.gov/v1.1/schema/catalog.json"] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'catalog_describedBy']
        assert ["https://project-open-data.cio.gov/v1.1/schema"] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'catalog_conformsTo']
        assert ['https://healthdata.gov/data.json'] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'catalog_@id']

    def test_required_fields(self, test_datajson_dataset):

        dataset = test_datajson_dataset
        # drop required keys
        djss = DataJSONSchema1_1(original_dataset=dataset, schema='usmetadata')
        # ORG is required!

        ckan_dataset = djss.transform_to_ckan_dataset()
        assert ckan_dataset is None
        assert 'Owner organization ID is required' in djss.errors

        djss.ckan_owner_org_id = 'XXXX'
        ckan_dataset = djss.transform_to_ckan_dataset()
        del ckan_dataset['name']

        ret = djss.validate_final_dataset()
        assert not ret
        assert '"name" is a required field' in djss.errors

    def test_resources(self, test_datajson_dataset):
        djss = DataJSONSchema1_1(original_dataset=test_datajson_dataset, schema='usmetadata')
        # ORG is required!
        djss.ckan_owner_org_id = 'XXXX'

        # sample from CKAN results
        existing_resources = [
                { # the first is a real CKAN result from a data.json distribution/resource on test_datajsoin_dataset
                "conformsTo": "https://management.cio.gov/schema/",
                "cache_last_updated": None,
                "describedByType": "application/json",
                "package_id": "d84cac16-307f-4ed9-8353-82d303e2b581",
                "webstore_last_updated": None,
                "id": "d0eb660c-7734-4fe1-b106-70f817f1c99d",
                "size": None,
                "state": "active",
                "describedBy": "https://management.cio.gov/schemaexamples/costSavingsAvoidanceSchema.json",
                "hash": "",
                "description": "costsavings.json",
                "format": "JSON",
                "tracking_summary": {
                "total": 20,
                "recent": 1
                },
                "mimetype_inner": None,
                "url_type": None,
                "revision_id": "55598e72-79d2-4679-8095-aa4b1e67b2f5",
                "mimetype": "application/json",
                "cache_url": None,
                "name": "JSON File",
                "created": "2018-02-03T23:39:07.247009",
                "url": "http://www.usda.gov/digitalstrategy/costsavings.json",
                "webstore_url": None,
                "last_modified": None,
                "position": 0,
                "no_real_name": "True",
                "resource_type": None
                },
                {
                "cache_last_updated": None,
                "package_id": "6fdad934-75a4-44d3-aced-2a69a289356d",
                "webstore_last_updated": None,
                "id": "280dff75-cace-458a-bc4d-ff7c67a8366c",
                "size": None,
                "state": "active",
                "hash": "",
                "description": "Query tool",
                "format": "HTML",
                "tracking_summary": {
                "total": 1542,
                "recent": 41
                },
                "last_modified": None,
                "url_type": None,
                "mimetype": "text/html",
                "cache_url": None,
                "name": "Poverty",
                "created": "2018-02-04T00:02:06.320564",
                "url": "http://www.ers.usda.gov/data-products/county-level-data-sets/poverty.aspx",
                "webstore_url": None,
                "mimetype_inner": None,
                "position": 0,
                "revision_id": "ffb7058b-2606-4a13-9669-ccfde2547ff7",
                "resource_type": None
                }]

        ckan_dataset = djss.transform_to_ckan_dataset(existing_resources=existing_resources)

        assert len(ckan_dataset['resources']) == 2

        # we expect for one dataset with an ID (merged)
        for resource in ckan_dataset['resources']:
            if resource['url'] == 'http://marketnews.usda.gov/':
                assert resource['format'] == 'text/html'
                assert resource['mimetype'] == 'text/html'
                assert resource['description'] == ''
                assert resource['name'] == 'Web Page'
            elif resource['url'] == "http://www.usda.gov/digitalstrategy/costsavings.json":
                assert resource['format'] == 'application/json'
                assert resource['mimetype'] == 'application/json'
                assert resource['description'] == ''
                assert 'name' not in resource
            else:
                assert 'Unexpected URL' == False

    def test_drop_distribution(self, test_datajson_dataset):

        dataset = test_datajson_dataset
        # drop required keys
        djss = DataJSONSchema1_1(original_dataset=dataset, schema='usmetadata')
        djss.ckan_owner_org_id = 'XXXX'
        ckan_dataset = djss.transform_to_ckan_dataset()

        del dataset['distribution']
        djss = DataJSONSchema1_1(original_dataset=dataset, schema='usmetadata')
        djss.ckan_owner_org_id = 'XXXX'
        ckan_dataset = djss.transform_to_ckan_dataset()

        assert ckan_dataset['resources'] == []

    def test_get_base_ckan_dataset(self, test_datajson_dataset, base_ckan_dataset, base_ckan_dataset_usmetadata):
        datajson = DataJSONSchema1_1(original_dataset=test_datajson_dataset)
        assert datajson.get_base_ckan_dataset(schema='default') == base_ckan_dataset

        datajson = DataJSONSchema1_1(original_dataset=test_datajson_dataset)
        assert datajson.get_base_ckan_dataset(schema='usmetadata') == base_ckan_dataset_usmetadata

    def test_identify_origin_element(self, test_datajson_dataset):
        datajson = DataJSONSchema1_1(original_dataset=test_datajson_dataset)
        fn = datajson.identify_origin_element('contactPoint__fn')
        hasEmail = datajson.identify_origin_element('contactPoint__hasEmail')
        assert fn == 'Fred Teensma'
        assert hasEmail == 'mailto:Fred.Teensma@ams.usda.gov'

    def test_validate_final_dataset(self):
        pass

    def test_set_destination_element(self):
        pass

    def test_build_tags(self):
        pass

    def test_set_extra(self):
        pass

    def test_get_extra(self):
        pass

    def test_generate_name(self):
        pass

    def test_get_accrual_periodicity(self):
        pass