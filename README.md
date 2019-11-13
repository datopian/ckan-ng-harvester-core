# Harvester Next Generation for CKAN

## Install

```
pip install ckan-harvesters
```


### Use data.json sources

```python
from harvesters.datajson.harvester import DataJSON
dj = DataJSON()
dj.url = 'https://data.iowa.gov/data.json'
try:
	dj.fetch()
except Exception as e:
	print(e)

valid = dj.validate()
print(dj.errors)
# ['Error validating JsonSchema: \'bureauCode\' is a required property ...

# full dict with the source
print(dj.as_json())
"""
{
	'@context': 'https://project-open-data.cio.gov/v1.1/schema/catalog.jsonld',
	'@id': 'https://data.iowa.gov/data.json',
	'@type': 'dcat:Catalog',
	'conformsTo': 'https://project-open-data.cio.gov/v1.1/schema',
	'describedBy': 'https://project-open-data.cio.gov/v1.1/schema/catalog.json',
	'dataset': [{
		'accessLevel': 'public',
		'landingPage': 'https://data.iowa.gov/d/23jk-3uwr',
		'issued': '2017-01-30',
		'@type': 'dcat:Dataset',

        ... 
"""
# just headers
print(dj.headers)

"""
{
'@context': 'https://project-open-data.cio.gov/v1.1/schema/catalog.jsonld',
'@id': 'https://data.iowa.gov/data.json',
'@type': 'dcat:Catalog',
'conformsTo': 'https://project-open-data.cio.gov/v1.1/schema',
'describedBy': 'https://project-open-data.cio.gov/v1.1/schema/catalog.json',
}
"""

for dataset in dj.datasets:
    print(dataset['title'])

Impaired Streams 2014
2009-2010 Iowa Public School District Boundaries
2015 - 2016 Iowa Public School District Boundaries
Impaired Streams 2010
Impaired Lakes 2014
2007-2008 Iowa Public School District Boundaries
Impaired Streams 2012
2011-2012 Iowa Public School District Boundaries
Active and Completed Watershed Projects - IDALS
2012-2013 Iowa Public School District Boundaries
2010-2011 Iowa Public School District Boundaries
2016-2017 Iowa Public School District Boundaries
2014 - 2015 Iowa Public School District Boundaries
Impaired Lakes 2008
2008-2009 Iowa Public School District Boundaries
2013-2014 Iowa Public School District Boundaries
Impaired Lakes 2010
Impaired Lakes 2012
Impaired Streams 2008

```


### Use CSW sources

```python
from harvesters.csw.harvester import CSWSource
c = CSWSource(url='http://data.nconemap.com/geoportal/csw?Request=GetCapabilities&Service=CSW&Version=2.0.2')

csw.fetch()
csw_info = csw.as_json()
print('CSW title: {}'.format(csw_info['identification']['title']))
 # CSW title: ArcGIS Server Geoportal Extension 10 - OGC CSW 2.0.2 ISO AP
```

## Development

To setup a develop environment, clone the repository and in a virtualenv install the dependencies

```
pip install -r requirements.txt
```

This will install the library in development mode, and other libraries for tests. 

## Test

Then to run the test suite with pytest:

```
pytest
```

We use [pytest-vcr](https://pytest-vcr.readthedocs.io/en/latest/) based on the wonderful [VCRpy](https://vcrpy.readthedocs.io/en/latest/), to mock http requests. In this way, we don't need to hit the real internet to run our test (which is very fragile and slow), because there is a mocked version of a each response needed by tests, in vcr's *cassettes* format. 

In order to update these *cassettes* just run as following: 

```
pytest --vcr-record=all
```

To actually hit the internet without use mocks, disable the plugin 

```
pytest --disable-vcr
```

Tests without a CKAN instance

```
python -m pytest tests

================ test session starts =============
platform linux -- Python 3.6.8, pytest-5.2.0, py-1.8.0, pluggy-0.13.0
rootdir: /home/hudson/dev/datopian/ckan-ng-harvester-core
plugins: vcr-1.0.2
collected 17 items                                                                                                                                                          

tests/test_csw_dataset_adapter.py ....      [ 23%]
tests/test_data_json.py .......             [ 64%]
tests/test_datajson_dataset_adapter.py .....[100%]

=============== 17 passed in 17.52s ==============
```

Tests with a CKAN instance.  
You will need to copy settings.py file to local_settings.py file and fill the required values.  
You can use a local or remote CKAN instance.  


```
python -m pytest tests_with_ckan/test_harvest.py
```