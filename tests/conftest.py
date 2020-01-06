import pytest

@pytest.fixture
def test_datajson_dataset():
  return {
      "identifier": "USDA-26521",
      "accessLevel": "",
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


@pytest.fixture
def datajson_mapped_fields():
    return {'name': 'name',
            'title': 'title',
            'description': 'notes',
            'keyword': 'tags',
            'modified': 'extras__modified',
            'contactPoint__fn': 'maintainer',
            'contactPoint__hasEmail': 'maintainer_email',
            'identifier': 'extras__identifier',
            'accessLevel': 'extras__accessLevel',
            'bureauCode': 'extras__bureauCode',
            'programCode': 'extras__programCode',
            'rights': 'extras__rights',
            'license': 'extras__license',
            'spatial': 'extras__spatial',
            'temporal': 'extras__temporal',
            'theme': 'extras__theme',
            'dataDictionary': 'extras__dataDictionary',
            'dataQuality': 'extras__dataQuality',
            'accrualPeriodicity': 'extras__accrualPeriodicity',
            'landingPage': 'extras__landingPage',
            'language': 'extras__language',
            'primaryITInvestmentUII': 'extras__primaryITInvestmentUII',
            'references': 'extras__references',
            'issued': 'extras__issued',
            'systemOfRecords': 'extras__systemOfRecords',
            'harvest_ng_source_title': 'extras__harvest_ng_source_title',
            'harvest_ng_source_id': 'extras__harvest_ng_source_id',
            'harvest_source_title': 'extras__harvest_source_title',
            'harvest_source_id': 'extras__harvest_source_id',
            'source_schema_version': 'extras__source_schema_version',
            'source_hash': 'extras__source_hash',
            'catalog_@context': 'extras__catalog_@context',
            'catalog_@id': 'extras__catalog_@id',
            'catalog_conformsTo': 'extras__catalog_conformsTo',
            'catalog_describedBy': 'extras__catalog_describedBy',
            'is_collection': 'extras__is_collection',
            'collection_pkg_id': 'extras__collection_package_id'
            }


@pytest.fixture
def datajson_usmetadata_mapped_fields():
    return {'name': 'name',
            'title': 'title',
            'description': 'notes',
            'keyword': 'tags',
            'modified': 'modified',
            'contactPoint__fn': 'contact_name',
            'contactPoint__hasEmail': 'contact_email',
            'identifier': 'unique_id',
            'accessLevel': 'public_access_level',
            'bureauCode': 'bureau_code',
            'programCode': 'program_code',
            'rights': 'extras__rights',
            'license': 'extras__license',
            'spatial': 'spatial',
            'temporal': 'temporal',
            'theme': 'extras__theme',
            'dataDictionary': 'data_dictionary',
            'dataQuality': 'data_quality',
            'accrualPeriodicity': 'accrual_periodicity',
            'landingPage': 'homepage_url',
            'language': 'language',
            'primaryITInvestmentUII': 'primary_it_investment_uii',
            'references': 'extras__references',
            'issued': 'extras__issued',
            'systemOfRecords': 'system_of_records',
            'harvest_ng_source_title': 'extras__harvest_ng_source_title',
            'harvest_ng_source_id': 'extras__harvest_ng_source_id',
            'harvest_source_title': 'extras__harvest_source_title',
            'harvest_source_id': 'extras__harvest_source_id',
            'source_schema_version': 'extras__source_schema_version',
            'source_hash': 'extras__source_hash',
            'catalog_@context': 'extras__catalog_@context',
            'catalog_@id': 'extras__catalog_@id',
            'catalog_conformsTo': 'extras__catalog_conformsTo',
            'catalog_describedBy': 'extras__catalog_describedBy',
            'is_collection': 'extras__is_collection',
            'collection_pkg_id': 'extras__collection_package_id',
            'publisher': 'publisher'}

@pytest.fixture
def base_ckan_dataset():
    return {'name': '',
            'title': '',
            'owner_org': '',
            'private': False,
            'author': None,
            'author_email': None,
            'maintainer': None,
            'maintainer_email': None,
            'notes': None,
            'url': None,
            'version': None,
            'state': 'active',
            'type': None,
            'resources': None,
            'tags': None,
            'extras': [{'key': 'resource-type', 'value': 'Dataset'}],
            'relationships_as_object': None,
            'relationships_as_subject': None,
            'groups': None}

@pytest.fixture
def base_ckan_dataset_usmetadata():
    return {'name': '',
            'title': '',
            'owner_org': '',
            'private': False,
            'author': None,
            'author_email': None,
            'notes': None,
            'url': None,
            'version': None,
            'state': 'active',
            'type': None,
            'resources': None,
            'tags': None,
            'extras': [{'key': 'resource-type', 'value': 'Dataset'}],
            'relationships_as_object': None,
            'relationships_as_subject': None,
            'groups': None,
            'contact_name': None,
            'contact_email': None,
            'modified': None,
            'publisher': None,
            'public_access_level': None,
            'homepage_url': None,
            'unique_id': None,
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
            'language': None}