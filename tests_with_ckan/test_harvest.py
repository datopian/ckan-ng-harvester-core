import pytest
from harvesters.logs import logger
from harvesters.datajson.ckan.dataset import DataJSONSchema1_1
from harvester_adapters.ckan.api import CKANPortalAPI
from harvester_adapters.ckan.settings import (HARVEST_SOURCE_ID,
                                              CKAN_API_KEY,
                                              CKAN_BASE_URL,
                                              CKAN_ORG_ID,
                                              CKAN_VALID_USER_ID
                                             )


class TestCKANHarvest(object):
    """ test transform datasets """

    test_datajson_dataset = {
            "identifier": "USDA-26521",
            "accessLevel": "public",
            "contactPoint": {
                "hasEmail": "mailto:Fred.Teensma@ams.usda.gov",
                "@type": "vcard:Contact",
                "fn": "Fred Teensma"
                },
            "programCode": ["005:047"],
            "description": "Some notes ...",
            "title": "Fruit and Vegetable Market News Search",
            "distribution": [
                {
                "@type": "dcat:Distribution",
                "downloadURL": "http://marketnews.usda.gov/",
                "mediaType": "text/html",
                "title": "Web Page"
                },
                {
                "@type": "dcat:Distribution",
                "downloadURL": "http://www.usda.gov/digitalstrategy/costsavings.json",
                "describedBy": "https://management.cio.gov/schemaexamples/costSavingsAvoidanceSchema.json",
                "mediaType": "application/json",
                "conformsTo": "https://management.cio.gov/schema/",
                "describedByType": "application/json"
                }
            ],
            "license": "https://creativecommons.org/licenses/by/4.0",
            "bureauCode": ["005:45"],
            "modified": "2014-12-23",
            "publisher": {
                "@type": "org:Organization",
                "name": "Agricultural Marketing Service"
                },
            "keyword": ["FOB", "wholesale market"],
            "headers": {
            "@type": "dcat:Catalog",
            "describedBy": "https://project-open-data.cio.gov/v1.1/schema/catalog.json",
            "conformsTo": "https://project-open-data.cio.gov/v1.1/schema",
            "@context": "https://project-open-data.cio.gov/v1.1/schema/catalog.jsonld"
            }
        }

    def test_create_package_with_tags(self):

        # djss = DataJSONSchema1_1(original_dataset=self.test_datajson_dataset, schema='usmetadata')
        djss = DataJSONSchema1_1(original_dataset=self.test_datajson_dataset)
        djss.ckan_owner_org_id = CKAN_ORG_ID
        package = djss.transform_to_ckan_dataset()
        assert 'extras' in package
        # TODO check what we expect here
        # assert [['005:45']] == [extra['value'] for extra in package['extras'] if extra['key'] == 'bureauCode']
        # assert [['005:047']] == [extra['value'] for extra in package['extras'] if extra['key'] == 'programCode']
        assert ['005:45'] == [extra['value'] for extra in package['extras'] if extra['key'] == 'bureauCode']
        assert ['005:047'] == [extra['value'] for extra in package['extras'] if extra['key'] == 'programCode']

        cpa = CKANPortalAPI(base_url=CKAN_BASE_URL, api_key=CKAN_API_KEY)
        res = cpa.create_package(ckan_package=package, on_duplicated='DELETE')
        assert res['success'] == True
        result = res['result']

        # read it
        res = cpa.show_package(ckan_package_id_or_name=result['id'])
        assert res['success'] == True
        ckan_dataset = res['result']

        assert 'extras' in ckan_dataset
        assert ['005:45'] == [extra['value'] for extra in package['extras'] if extra['key'] == 'bureauCode']
        assert ['005:047'] == [extra['value'] for extra in package['extras'] if extra['key'] == 'programCode']