"""
Base class for all harvesters
"""
import json
from abc import ABC, abstractmethod
from datapackage import Package, Resource


class HarvesterBaseSource(ABC):

    def __init__(self):
        self.errors = []
        self.datasets = []  # list of data units
        self.duplicates = []  # list of datasets with the same identifier

    @abstractmethod
    def fetch(self):
        """ Reach the source and check if it's alive 
            Download the data or indexes from source
            Raise error or return None"""
        pass
    
    @abstractmethod
    def validate(self):
        """ validate fetched data and save errors
            returns Boolean"""
        pass

    @abstractmethod
    def as_json(self):
        """ define a JSON version of the harvester data """
        pass
    
    def save_json(self, path):
        """ save the source data.json file """
        dmp = json.dumps(self.as_json(), indent=2)
        f = open(path, 'w')
        f.write(dmp)
        f.close()
    
    def save_duplicates(self, path):
        dmp = json.dumps(self.duplicates, indent=2)
        f = open(path, 'w')
        f.write(dmp)
        f.close()
    
    def save_errors(self, path):
        dmp = json.dumps(self.errors, indent=2)
        f = open(path, 'w')
        f.write(dmp)
        f.close()
    
    def save_datasets_as_data_packages(self, folder_path, identifier_field):
        """ save each dataset from a data.json source as _datapackage_ """
        for dataset in self.datasets:
            package = Package()

            #TODO check this, I'm learning datapackages
            resource = Resource({'data': dataset})
            resource.infer()  #adds "name": "inline"

            idf = slugify(dataset[identifier_field])

            resource_path = os.path.join(folder_path, f'resource_data_json_{idf}.json')
            if not resource.valid:
                raise Exception('Invalid resource')

            resource.save(resource_path)

            package.add_resource(descriptor=resource.descriptor)
            package_path = os.path.join(folder_path, f'pkg_data_json_{idf}.zip')
            package.save(target=package_path)


