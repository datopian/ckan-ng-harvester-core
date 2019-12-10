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
    ckan_resource = {'url': 'has a url'}
    cra = DataJSONDistribution(original_resource=ckan_resource)
    result = cra.validate_final_resource(ckan_resource)
    result == (True, None)

    ckan_resource = {}
    cra = DataJSONDistribution(original_resource=ckan_resource)
    result = cra.validate_final_resource(ckan_resource)
    result == (False, 'url is a required field')


  def test_transform_to_ckan_resource(self):
    original_resource = {'@type': 'dcat:Distribution',
                         'accessURL': 'http://marketnews.usda.gov/    ',
                         'downloadURL': 'http://marketnews.usda.gov/    ',
                         'mediaType': 'text/html',
                         'title': 'Web Page',
                         'conformsTo': ''}
    cra = DataJSONDistribution(original_resource=original_resource)
    resource_transformed = cra.transform_to_ckan_resource()
    assert resource_transformed == {'url': 'http://marketnews.usda.gov/',
                                    'accessURL': 'http://marketnews.usda.gov/',
                                    'description': '',
                                    'format': 'text/html',
                                    'name': 'Web Page',
                                    'mimetype': 'text/html'}

    original_resource2 = {'@type': 'dcat:Distribution',
                         'downloadURL': 'http://marketnews.usda.gov/    ',
                         'mediaType': 'text/html',
                         'title': 'Web Page',
                         'conformsTo': 'something',
                         'describedBy': 'something',
                         'describedByType': 'something'}
    cra = DataJSONDistribution(original_resource=original_resource2)
    resource_transformed = cra.transform_to_ckan_resource()
    assert resource_transformed == {'url': 'http://marketnews.usda.gov/',
                                    'description': '',
                                    'format': 'text/html',
                                    'name': 'Web Page',
                                    'mimetype': 'text/html',
                                    'conformsTo': 'something',
                                    'describedBy': 'something',
                                    'describedByType': 'something'}

    cra = DataJSONDistribution(original_resource={})
    with pytest.raises(Exception) as e:
        assert cra.transform_to_ckan_resource()
    assert str(e.value) == 'Error validating origin resource/distribution: You need "downloadURL" or "accessURL" to conform a final url'
