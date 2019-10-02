''' transform datasets to CKAN datasets '''
from abc import ABC, abstractmethod


class CKANResourceAdapter(ABC):
    ''' transform other resource objects into CKAN resource '''

    def __init__(self, original_resource):
        self.original_resource = original_resource

    def get_base_ckan_resource(self):
        # Creates the Dict base for a CKAN resource
        # Check for required fields: https://docs.ckan.org/en/2.8/api/#ckan.logic.action.create.resource_create

        resource = {
            'package_id': None,  # (string) – id of package that the resource should be added to.
            'url': None,  # (string) – url of resource
            'revision_id': None,  # (string) – (optional)
            'description': None,  # (string) – (optional)
            'format': None,  # (string) – (optional)
            'hash': None,  # (string) – (optional)
            'name': None,  # (string) – (optional)
            'resource_type': None,  # (string) – (optional)
            'mimetype': None,  # (string) – (optional)
            'mimetype_inner': None,  # (string) – (optional)
            'cache_url': None,  # (string) – (optional)
            'size': None,  # (int) – (optional)
            'created': None,  # (iso date string) – (optional)
            'last_modified': None,  # (iso date string) – (optional)
            'cache_last_updated': None,  # (iso date string) – (optional)
            'upload': None,  # (FieldStorage (optional) needs multipart/form-data) – (optional)
        }

        return resource

    @abstractmethod
    def transform_to_ckan_resource(self):
        pass