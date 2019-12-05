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

      djsumd.transform_to_ckan_dataset()

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

      djsumd.transform_to_ckan_dataset()

      #TODO tags field
      #TODO extras__bureauCode & extras__programCode fields
      #TODO test accrual_periodicity field
      assert djsumd.ckan_dataset['contact_email'] == 'Fred.Teensma@ams.usda.gov'
    
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

    def test_transform_resources(self):
      pass

    def test_transform_to_ckan_dataset(self, test_datajson_dataset):
      pass

    def test_merge_resources(self):
      pass
