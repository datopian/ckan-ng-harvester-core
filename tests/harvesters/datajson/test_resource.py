import pytest
import pytest
from harvesters.datajson.ckan.resource import DataJSONDistribution

class TestDataJSONResource(object):

  def test_validate_origin_distribution(self, test_datajson_dataset):
    original_resource = {'downloadURL': 'http://downloadURL.usda.gov/   '}
    cra = DataJSONDistribution(original_resource=original_resource)
    result = cra.validate_origin_distribution()
    assert result == (True, None)

    original_resource = {'accessURL': 'http://accessURL.usda.gov/   '}
    cra = DataJSONDistribution(original_resource=original_resource)
    result = cra.validate_origin_distribution()
    assert result == (True, None)

    original_resource = {}
    cra = DataJSONDistribution(original_resource=original_resource)
    result = cra.validate_origin_distribution()
    assert result == (False, 'You need "downloadURL" or "accessURL" to conform a final url')

  def test_validate_final_resource(self):
    pass

  def test_transform_to_ckan_resource(self):
    pass
