import pytest
from harvesters.datajson.harvester import DataJSON

base_url = 'https://datopian.gitlab.io/ckan-ng-harvest'

test_original_datajson_datasets = {
"@type": "dcat:Catalog",
"describedBy": "https://project-open-data.cio.gov/v1.1/schema/catalog.json",
"conformsTo": "https://project-open-data.cio.gov/v1.1/schema",
"@context": "https://project-open-data.cio.gov/v1.1/schema/catalog.jsonld",
"dataset": [
    {
    "identifier": "USDA-26522",
    "accessLevel": "public",
    "isPartOf": 'USDA-26521',
    "contactPoint": {
        "hasEmail": "mailto:dataset2@usda.gov",
        "@type": "vcard:Contact",
        "fn": "Dataset Two"
        },
    "programCode": ["005:044"],
    "description": "Some notes dataset 2 ...",
    "title": "Dataset 2 Title",
    "distribution": [
        {
        "@type": "dcat:Distribution",
        "downloadURL": "http://dataset2.usda.gov/",
        "mediaType": "text/html",
        "title": "Web Page Datset 2"
        },
        {
        "@type": "dcat:Distribution",
        "downloadURL": "http://dataset2.usda.gov/costsavings.json",
        "describedBy": "https://management.cio.gov/schemaexamples/costSavingsAvoidanceSchema.json",
        "mediaType": "application/json",
        "conformsTo": "https://management.cio.gov/schema/",
        "describedByType": "application/json"
        }
    ],
    "license": "https://creativecommons.org/licenses/by/4.0",
    "bureauCode": ["005:41"],
    "modified": "2018-12-23",
    "publisher": {
        "@type": "org:Organization",
        "name": "Agricultural Marketing Service"
        },
    "keyword": ["Datset2", "wholesale market"]
    },

    {
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

    }
    ]
}


@pytest.mark.vcr()
def test_load_from_url():
    dj = DataJSON()

    with pytest.raises(Exception):
        dj.fetch()

    dj.url = f'{base_url}/DO-NOT-EXISTS.json'
    with pytest.raises(Exception):
        dj.fetch()

    dj.url = f'{base_url}/bad.json'
    dj.fetch()  # URL exists but it's a bad JSON, do not fails, it's downloadable (OK)
    valid = dj.validate()
    assert not valid


@pytest.mark.vcr()
def test_read_json():
    dj = DataJSON()

    dj.url = f'{base_url}/bad.json'
    dj.fetch()

    valid = dj.validate()
    assert not valid  # bad json

    dj.url = f'{base_url}/good-but-not-data.json'
    dj.fetch()
    valid = dj.validate()
    assert not valid  # it's good as JSON
    assert 'Missing "dataset" KEY' in ', '.join(dj.errors)


@pytest.mark.vcr()
def test_validate_json1():

    dj = DataJSON()

    dj.url = f'{base_url}/good-but-not-data.json'
    dj.fetch()
    
    valid = dj.validate()
    assert not valid  # no schema


@pytest.mark.vcr()
def test_validate_json2():
    # data.json without errors
    dj = DataJSON()
    dj.url = f'{base_url}/usda.gov.data.json'
    dj.fetch()
    valid = dj.validate()
    assert valid  # schema works without errors
    assert dj.errors == []


@pytest.mark.vcr()
def test_validate_json3():
    # data.json with some errors
    dj = DataJSON()
    dj.url = f'{base_url}/healthdata.gov.data.json'
    dj.fetch()
    valid = dj.validate()
    assert len(dj.errors) == 1


@pytest.mark.vcr()
def test_load_from_data_json_object():
    # test loading a data.json dict
    dj = DataJSON()
    dj.read_dict_data_json(data_json_dict=test_original_datajson_datasets)
    valid = dj.validate()
    dj.post_fetch()
    
    for dataset in dj.datasets:
        if dataset['identifier'] == 'USDA-26521':
            assert dataset['is_collection'] == True
        if dataset['identifier'] == 'USDA-26522':
            assert dataset['collection_pkg_id'] == ''


@pytest.mark.vcr()
def test_catalog_extras():
    dj = DataJSON()
    dj.url = f'{base_url}/usda.gov.data.json'
    dj.fetch()
    valid = dj.validate()
    dj.post_fetch()
    print(dj.catalog_extras)
    assert dj.catalog_extras['catalog_@context'] == 'https://project-open-data.cio.gov/v1.1/schema/catalog.jsonld'
    assert 'catalog_@id' not in dj.catalog_extras
    assert dj.catalog_extras['catalog_conformsTo'] == 'https://project-open-data.cio.gov/v1.1/schema'
    assert dj.catalog_extras['catalog_describedBy'] == 'https://project-open-data.cio.gov/v1.1/schema/catalog.json'

