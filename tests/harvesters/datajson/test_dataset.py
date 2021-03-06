import pytest
from harvesters.datajson.ckan.dataset import DataJSONSchema1_1


class TestDataJSONDataset(object):

    def test_get_field_mapping(self, test_datajson_dataset, datajson_mapped_fields):
        djs = DataJSONSchema1_1(original_dataset=test_datajson_dataset)
        assert djs.mapped_fields == datajson_mapped_fields

    def test_load_default_values(self, test_datajson_dataset):
        djs = DataJSONSchema1_1(original_dataset=test_datajson_dataset)
        assert djs.original_dataset['accessLevel'] == ''

        djs_usmetadata = DataJSONSchema1_1(original_dataset=test_datajson_dataset, schema='usmetadata')
        assert djs_usmetadata.original_dataset['accessLevel'] == 'public'
    
        del test_datajson_dataset['accessLevel']
        djs_usmetadata = DataJSONSchema1_1(original_dataset=test_datajson_dataset, schema='usmetadata')
        assert djs_usmetadata.original_dataset['accessLevel'] == 'public'

    def test_upgrade_usmetadata_default_fields(self, test_datajson_dataset, datajson_usmetadata_mapped_fields):
      djs = DataJSONSchema1_1(original_dataset=test_datajson_dataset, schema='usmetadata')
      usmetadata_default_fields = djs.upgrade_usmetadata_default_fields(djs.mapped_fields)
      assert usmetadata_default_fields == datajson_usmetadata_mapped_fields

    def test_validate_origin_dataset(self, test_datajson_dataset):
      djs = DataJSONSchema1_1(original_dataset=test_datajson_dataset)
      valid = djs.validate_origin_dataset()
      assert valid == False
      assert djs.errors == ['Owner organization ID is required']

      del test_datajson_dataset['accessLevel']
      del test_datajson_dataset['contactPoint']
      del test_datajson_dataset['identifier']
      del test_datajson_dataset['programCode']
      del test_datajson_dataset['bureauCode']
      del test_datajson_dataset['publisher']
      del test_datajson_dataset['modified']
      del test_datajson_dataset['keyword']
      djsumd = DataJSONSchema1_1(original_dataset=test_datajson_dataset, schema='usmetadata')
      djsumd.ckan_owner_org_id = 'XXXXX' 

      valid = djsumd.validate_origin_dataset()

      assert valid == False

      # accessLevel does not error because it is added in the load_default_values method
      assert djsumd.errors == ['"identifier" field could not be empty at origin dataset', 
                               '"contactPoint__fn" field could not be empty at origin dataset', 
                               '"programCode" field could not be empty at origin dataset', 
                               '"bureauCode" field could not be empty at origin dataset', 
                               '"contactPoint__hasEmail" field could not be empty at origin dataset', 
                               '"publisher" field could not be empty at origin dataset', 
                               '"modified" field could not be empty at origin dataset', 
                               '"keyword" field could not be empty at origin dataset']

    def test_fix_fields(self, test_datajson_dataset):
      djsumd = DataJSONSchema1_1(original_dataset=test_datajson_dataset, schema='usmetadata')
      djsumd.ckan_owner_org_id = 'XXXXX' 

      fields = djsumd.fix_fields('tags', ['FOB', 'wholesale market'])
      assert fields == [{'name': 'fob'}, {'name': 'wholesale-market'}]

      fields = djsumd.fix_fields('contact_email', 'mailto:Fred.Teensma@ams.usda.gov')
      assert fields == 'Fred.Teensma@ams.usda.gov'

      fields = djsumd.fix_fields('maintainer_email', 'mailto:Fred.Teensma@ams.usda.gov')
      assert fields == 'Fred.Teensma@ams.usda.gov'

      fields = djsumd.fix_fields('extras__bureauCode', ['list', 'items'])
      assert fields == 'list,items'

      fields = djsumd.fix_fields('extras__programCode', ['list', 'items'])
      assert fields == 'list,items'

      fields = djsumd.fix_fields('accrual_periodicity', 'irregular')
      assert fields == 'not updated'
    
    def test_infer_resources(self, test_datajson_dataset):
      del test_datajson_dataset['distribution']
      djsumd = DataJSONSchema1_1(original_dataset=test_datajson_dataset, schema='usmetadata')
      djsumd.ckan_owner_org_id = 'XXXXX'

      djsumd.original_dataset['accessURL'] = "http://urlwithspaces.com  "
      #TODO check why we transform webService if its not used
      djsumd.original_dataset['webService'] = "http://webService.com  "
      djsumd.original_dataset['format'] = "distribution format"

      distribution = djsumd.infer_resources()

      assert distribution == [{'accessURL': 'http://urlwithspaces.com', 'format': 'distribution format', 'mimetype': 'distribution format'}, {'webService': 'http://webService.com', 'format': 'distribution format', 'mimetype': 'distribution format'}]

    def test_transform_resources(self, test_datajson_dataset):
      djsumd = DataJSONSchema1_1(original_dataset=test_datajson_dataset, schema='usmetadata')
      djsumd.ckan_owner_org_id = 'XXXXX'

      distribution = {'@type': 'dcat:Distribution',
                      'downloadURL': 'http://marketnews.usda.gov/',
                      'mediaType': 'text/html',
                      'title': 'Web Page'}

      result = djsumd.transform_resources(distribution)
      assert result == [{'url': 'http://marketnews.usda.gov/',
                         'description': '',
                         'format': 'text/html',
                         'name': 'Web Page',
                         'mimetype': 'text/html'}]

    def test_transform_to_ckan_dataset(self, test_datajson_dataset, caplog):
      djs = DataJSONSchema1_1(original_dataset=test_datajson_dataset)
      result = djs.transform_to_ckan_dataset()

      assert result == None

      djs.ckan_owner_org_id = 'XXXXX'
      result = djs.transform_to_ckan_dataset(existing_resources=[{'url': 'http://marketnews.usda.gov/', 'id': '1'}])

      assert 'Transforming data.json dataset USDA-26521' in caplog.text
      assert 'Dataset transformed USDA-26521 OK' in caplog.text
      assert 'Connecting fields "name", "name"' in caplog.text
      assert 'No data in origin for "name"' in caplog.text
      assert 'Connected OK fields "title"="Fruit and Vegetable Market News Search"' in caplog.text
      assert result == {'name': 'fruit-and-vegetable-market-news-search',
                        'title': 'Fruit and Vegetable Market News Search',
                        'owner_org': 'XXXXX',
                        'private': False,
                        'maintainer': 'Fred Teensma',
                        'maintainer_email': 'Fred.Teensma@ams.usda.gov',
                        'notes': 'Some notes ...',
                        'state': 'active',
                        'resources': [{'url': 'http://marketnews.usda.gov/',
                                       'description': '',
                                       'format': 'text/html',
                                       'name': 'Web Page',
                                       'mimetype': 'text/html',
                                       'id': '1'},
                                       {'url': 'http://www.usda.gov/digitalstrategy/costsavings.json',
                                       'description': '',
                                       'format': 'application/json',
                                       'mimetype': 'application/json',
                                       'conformsTo': 'https://management.cio.gov/schema/',
                                       'describedBy': 'https://management.cio.gov/schemaexamples/costSavingsAvoidanceSchema.json',
                                       'describedByType': 'application/json'}],
                        'tags': [{'name': 'fob'},
                                 {'name': 'wholesale-market'}],
                        'extras': [{'key': 'resource-type', 'value': 'Dataset'},
                                   {'key': 'modified', 'value': '2014-12-23'},
                                   {'key': 'identifier', 'value': 'USDA-26521'},
                                   {'key': 'accessLevel', 'value': ''},
                                   {'key': 'bureauCode', 'value': '005:45'},
                                   {'key': 'programCode', 'value': '005:047'},
                                   {'key': 'license', 'value': 'https://creativecommons.org/licenses/by/4.0'},
                                   {'key': 'source_datajson_identifier', 'value': True},
                                   {'key': 'publisher', 'value': 'Agricultural Marketing Service'}],
                        'tag_string': 'fob,wholesale-market', 'license_id': 'cc-by'}

    def test_merge_resources(self, test_datajson_dataset):
      djs = DataJSONSchema1_1(original_dataset=test_datajson_dataset)
      djs.ckan_owner_org_id = 'XXXXX'
      existing_resources = [{'url': 'http://marketnews.usda.gov/', 'id': '4'}]
      new_resources = [{'url': 'http://marketnews.usda.gov/', 'description': '', 'format': 'text/html', 'name': 'Web Page', 'mimetype': 'text/html'}]
      result = djs.merge_resources(existing_resources=existing_resources, new_resources=new_resources)

      assert result == [{'url': 'http://marketnews.usda.gov/', 'description': '', 'format': 'text/html', 'name': 'Web Page', 'mimetype': 'text/html', 'id': '4'}]
    
