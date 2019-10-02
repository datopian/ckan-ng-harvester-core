import pytest
from harvester.data_json import DataJSON

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

    ret, error = dj.download_data_json()
    assert ret is False  # No URL

    dj.url = f'{base_url}/DO-NOT-EXISTS.json'
    ret, error = dj.download_data_json()
    assert ret is False  # URL do not exists (404)

    dj.url = f'{base_url}/bad.json'
    ret, error = dj.download_data_json()
    assert ret is True  # URL exists but it's a bad JSON, do not fails, it's downloadable (OK)


@pytest.mark.vcr()
def test_read_json():
    dj = DataJSON()

    dj.url = f'{base_url}/bad.json'
    ret, error = dj.download_data_json()

    ret, error = dj.load_data_json()
    assert ret is False  # it's a bad JSON

    dj.url = f'{base_url}/good-but-not-data.json'
    ret, error = dj.download_data_json()
    ret, error = dj.load_data_json()
    assert ret is True  # it's a good JSON


@pytest.mark.vcr()
def test_validate_json1():

    dj = DataJSON()

    dj.url = f'{base_url}/good-but-not-data.json'
    ret, error = dj.download_data_json()
    ret, error = dj.load_data_json()
    ret, errors = dj.validate_json()
    assert ret is False  # no schema


@pytest.mark.vcr()
def test_validate_json2():
    # data.json without errors
    dj = DataJSON()

    dj.url = f'{base_url}/usda.gov.data.json'
    ret, error = dj.download_data_json()
    ret, error = dj.load_data_json()
    ret, errors = dj.validate_json()

    assert ret is True  # schema works without errors
    assert errors is None


@pytest.mark.vcr()
def test_validate_json3():
    # data.json with some errors
    dj = DataJSON()

    dj.url = f'{base_url}/healthdata.gov.data.json'
    ret, error = dj.download_data_json()
    ret, error = dj.load_data_json()
    ret, errors = dj.validate_json()

    assert ret is False  # schema works but has errors
    assert len(errors) == 1


@pytest.mark.vcr()
def test_load_from_data_json_object():
    # test loading a data.json dict
    dj = DataJSON()
    dj.read_dict_data_json(data_json_dict=test_original_datajson_datasets)
    ret, error = dj.validate_json()
    print(error)

    for dataset in dj.datasets:
        if dataset['identifier'] == 'USDA-26521':
            assert dataset['is_collection'] == True
        if dataset['identifier'] == 'USDA-26522':
            assert dataset['collection_pkg_id'] == ''


@pytest.mark.vcr()
def test_catalog_extras():
    dj = DataJSON()
    dj.url = f'{base_url}/usda.gov.data.json'
    ret, error = dj.download_data_json()
    ret, error = dj.load_data_json()
    ret, errors = dj.validate_json()
    print(dj.catalog_extras)
    assert dj.catalog_extras['catalog_@context'] == 'https://project-open-data.cio.gov/v1.1/schema/catalog.jsonld'
    assert 'catalog_@id' not in dj.catalog_extras
    assert dj.catalog_extras['catalog_conformsTo'] == 'https://project-open-data.cio.gov/v1.1/schema'
    assert dj.catalog_extras['catalog_describedBy'] == 'https://project-open-data.cio.gov/v1.1/schema/catalog.json'

