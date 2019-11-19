"""
process Data JSON files
    check the schema definition: https://project-open-data.cio.gov/v1.1/schema/catalog.json
    validate: maybe with this https://github.com/Julian/jsonschema
"""
import codecs
import csv
import json
import os
import urllib.request

import jsonschema as jss
import requests
import rfc3987 as rfc3987_url
from slugify import slugify
from validate_email import validate_email

from harvesters.logs import logger
from harvesters.harvester import HarvesterBaseSource

VALID_DATAJOSN_SCHEMAS = ['federal-v1.1', 'non-federal-v1.1']


class DataJSON(HarvesterBaseSource):
    """ a data.json file for read and validation """
    url = None  # URL of de data.json file
    schema_version = None
    raw_data_json = None  # raw downloaded text
    data_json = None  # JSON readed from data.json file
    headers = None

    def fetch(self, timeout=30):
        """ download de data.json file """
        logger.info(f'Fetching data from {self.url}')
        if self.url is None:
            error = "No URL defined"
            self.errors.append(error)
            logger.error(error)
            raise Exception(error)

        try:
            req = requests.get(self.url, timeout=timeout)
        except Exception as e:
            error = 'ERROR Donwloading data: {} [{}]'.format(self.url, e)
            self.errors.append(error)
            logger.error(error)
            raise

        logger.info(f'Data fetched status {req.status_code}')
        if req.status_code >= 400:
            error = '{} HTTP error: {}'.format(self.url, req.status_code)
            self.errors.append(error)
            logger.error(error)
            raise Exception(error)

        logger.info(f'Data fetched OK')
        self.raw_data_json = req.content

    def read_local_data_json(self, data_json_path):
        # initialize reading a JSON file
        if not os.path.isfile(data_json_path):
            return False, "File not exists"
        data_json_file = open(data_json_path, 'r')
        self.raw_data_json = data_json_file.read()
        return True, None

    def read_dict_data_json(self, data_json_dict):
        # to initialize using directly a dict
        self.data_json = data_json_dict
        return True, None

    def validate(self, validator_schema):
        """ Validate the data.json suorce 
            We need to know which validator to use 
            and two jsonschema definition file
            at ./validation/schemas/{validator_schema}
                /catalog.json: definition for full data.json 
                /dataset.json: definition for each dataset
            """
        if validator_schema not in VALID_DATAJOSN_SCHEMAS:
            raise Exception(f'Unknown validator_schema {validator_schema}')
        try:
            self.data_json = json.loads(self.raw_data_json)  # check for encoding errors
        except Exception as e:
            error = 'ERROR parsing JSON: {}. Data: {}'.format(e, self.raw_data_json)
            self.errors.append(error)
            logger.error(error)
            return False
            
        error = None

        if self.data_json is None:
            error = 'No data json available'
        elif type(self.data_json) == list:
            error = 'Data.json is a simple list. We expect a dict'  
            
        if error is not None:
            self.errors.append(error)
            logger.error(error)
            return False

        schemas_folder = os.path.join(os.path.dirname(__file__),
                                                      'validation',
                                                      'schemas',
                                                      validator_schema) 
        catalog_schema = os.path.join(schemas_folder, 'catalog.json')

        if os.path.isfile(catalog_schema):
            f = open(catalog_schema, 'r')
            schema = json.load(f)
            
            try:
                jss.validate(instance=self.data_json, schema=schema)
            except Exception as e:
                error = "Error validating catalog: {} with schema {}".format(e, schema)
                self.errors.append(error)
                return False

        return True

    def post_fetch(self):
        # save headers
        self.headers = self.data_json.copy()
        del self.headers['dataset']
        self.headers['schema_version'] = self.schema_version

        """ headers sample
        "@context": "https://openei.org/data.json",
        "@id": "https://openei.org/data.json",
        "@type": "dcat:Catalog",
        "conformsTo": "https://project-open-data.cio.gov/v1.1/schema",
        "describedBy": "https://project-open-data.cio.gov/v1.1/schema/catalog.json",
        """
        self.datasets = self.data_json['dataset']
        self.__detect_collections()

    def __detect_collections(self):
        # if a dataset has the property "isPartOf" assigned then
        #   this datasets must be marked as is_colleccion
        # when exists, isPartOf is a dataset identifier

        parent_identifiers = set()
        for dataset in self.datasets:
            parent = dataset.get('isPartOf', None)
            if parent is not None:
                # At the moment I don't know the CKAN ID but mark for later
                dataset['collection_pkg_id'] = ''
                parent_identifiers.add(parent)

        # mark all parents as collections
        for dataset in self.datasets:
            identifier = dataset.get('identifier')
            if identifier in parent_identifiers:
                dataset['is_collection'] = True

    def remove_duplicated_identifiers(self):
        unique_identifiers = []

        for dataset in self.datasets:
            idf = dataset['identifier']
            if idf not in unique_identifiers:
                unique_identifiers.append(idf)
            else:
                self.duplicates.append(idf)
                self.datasets.remove(dataset)

        return self.duplicates

    def count_resources(self):
        """ read all datasets and count resources """
        total = 0
        for dataset in self.datasets:
            distribution = dataset.get('distribution', [])
            total += len(distribution)
        return total

    def as_json(self):
        return self.data_json


class DataJSONDataset:

    def __init__(self, dataset):
        assert type(dataset) == dict
        self.data = dataset  # a dict
        self.bureau_code_url = "https://project-open-data.cio.gov/data/omb_bureau_codes.csv"
        
        self.errors = []
        self.omb_burueau_codes = set()
        # Constant URL is safe from protocol scheme abuse (bandit B310)
        self.ftpstream = urllib.request.urlopen(self.bureau_code_url) #nosec
        self.csvfile = csv.DictReader(codecs.iterdecode(self.ftpstream, 'utf-8'))
        for row in self.csvfile:
            self.omb_burueau_codes.add(row["Agency Code"] + ":" + row["Bureau Code"])

    def validate(self, validator_schema):

        schemas_folder = os.path.join(os.path.dirname(__file__),
                                                      'validation',
                                                      'schemas',
                                                      validator_schema) 
        dataset_schema = os.path.join(schemas_folder, 'dataset.json')
        if os.path.isfile(dataset_schema):
            f = open(dataset_schema, 'r')
            schema = json.load(f)
            
            try:
                jss.validate(self.data, schema=schema)
            except Exception as e:
                error = "Error validating dataset: {}".format(e)
                self.errors.append(error)
                logger.error(error)
                return False
        
        if validator_schema in ['federal-v1.1', 'federal']:
            if not self.validate_bureau_code():
                return False

        return True

    def validate_bureau_code(self):
        item = self.data
        # bureauCode # required
        if item.get('bureauCode', None) is not None:
            for bc in item["bureauCode"]:
                if bc not in self.omb_burueau_codes:
                    error = f'The bureau code {bc} was not found in our list at {self.bureau_code_url}'
                    self.errors.append(error)
                    return False
        
        return True
