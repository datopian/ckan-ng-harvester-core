import pytest
from harvesters.datajson.ckan.dataset import DataJSONSchema1_1


class TestDataJSONDataset(object):

    def test_get_field_mapping(self, test_datajson_dataset, datajson_mapped_fields, datajson_usmetadata_mapped_fields):
        djs = DataJSONSchema1_1(original_dataset=test_datajson_dataset)
        assert djs.mapped_fields == datajson_mapped_fields

        djs_usmetadata = DataJSONSchema1_1(original_dataset=test_datajson_dataset, schema='usmetadata')
        assert djs_usmetadata.mapped_fields == datajson_usmetadata_mapped_fields

    def test_load_default_values(self, test_datajson_dataset):
        djs = DataJSONSchema1_1(original_dataset=test_datajson_dataset)
        assert djs.original_dataset['accessLevel'] == ''

        djs_usmetadata = DataJSONSchema1_1(original_dataset=test_datajson_dataset, schema='usmetadata')
        assert djs_usmetadata.original_dataset['accessLevel'] == 'public'
    
        del test_datajson_dataset['accessLevel']
        djs_usmetadata = DataJSONSchema1_1(original_dataset=test_datajson_dataset, schema='usmetadata')
        assert djs_usmetadata.original_dataset['accessLevel'] == 'public'

    def test_validate_origin_dataset(self, test_datajson_dataset):
      djs = DataJSONSchema1_1(original_dataset=test_datajson_dataset)
      djs.transform_to_ckan_dataset()
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

      assert test_datajson_dataset['distribution'][0] == {'@type': 'dcat:Distribution',
                                                          'downloadURL': 'http://marketnews.usda.gov/',
                                                          'mediaType': 'text/html',
                                                          'title': 'Web Page'}
      # test that method will convert dictionary to list by passing dictionary
      result = djsumd.transform_resources(test_datajson_dataset['distribution'][0])
      assert result == [{'url': 'http://marketnews.usda.gov/',
                         'description': '',
                         'format': 'text/html',
                         'name': 'Web Page',
                         'mimetype': 'text/html'}]

    def test_validate_origin_distribution(self):
      pass

    def test_transform_to_ckan_resource(self):
      pass

    def test_transform_to_ckan_dataset(self, test_datajson_dataset):
      pass

    def test_merge_resources(self):
      pass
