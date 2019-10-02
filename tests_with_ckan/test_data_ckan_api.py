import unittest
from harvester.data_gov_api import CKANPortalAPI
import random
from slugify import slugify
import json
from harvester.logs import logger

# put you settings in the local_settings hidden-to-github file
from settings import (HARVEST_SOURCE_ID,
                       CKAN_API_KEY,
                       CKAN_BASE_URL,
                       CKAN_ORG_ID,
                       CKAN_VALID_USER_ID
                      )


class CKANPortalAPITestClass(unittest.TestCase):
    """ test a real CKAN API.
        #TODO test a local CKAN instance with real resource will be expensive but real test
        """

    def test_load_from_url(self):
        cpa = CKANPortalAPI(base_url=CKAN_BASE_URL)
        resources = 0

        page = 0
        for packages in cpa.search_harvest_packages(harvest_source_id=HARVEST_SOURCE_ID):
            page += 1
            print(f'API packages search page {page}')
            self.assertGreater(cpa.total_packages, 0)  # has resources in the first page
            break  # do not need more

    def test_create_package(self):

        cpa = CKANPortalAPI(base_url=CKAN_BASE_URL, api_key=CKAN_API_KEY)

        # error if duplicated
        dataset_title = 'Dataset number {}'.format(random.randint(1, 999999))
        dataset_name = slugify(dataset_title)
        package = {'name': dataset_name, 'title': dataset_title, 'owner_org': CKAN_ORG_ID}
        res = cpa.create_package(ckan_package=package)
        print(res)
        self.assertTrue(res['success'])

    def test_create_package_with_tags(self):

        cpa = CKANPortalAPI(base_url=CKAN_BASE_URL, api_key=CKAN_API_KEY)

        # error if duplicated
        dataset_title = 'Dataset number {}'.format(random.randint(1, 999999))
        dataset_name = slugify(dataset_title)
        tags = [{'name': 'tag001'}, {'name': 'tag002'}]

        package = {'name': dataset_name,
                   'title': dataset_title, 'owner_org': CKAN_ORG_ID,
                   'tags': tags}
        res = cpa.create_package(ckan_package=package)
        print(res)
        self.assertTrue(res['success'])

    def test_create_harvest_source(self):
        logger.info('Creating harvest source')
        cpa = CKANPortalAPI(base_url=CKAN_BASE_URL, api_key=CKAN_API_KEY)
        cpa.delete_all_harvest_sources(harvest_type='harvest', source_type='datajson')

        title = 'Energy JSON test {}'.format(random.randint(1, 999999))
        url = 'http://www.energy.gov/data-{}.json'.format(random.randint(1, 999999))
        res = cpa.create_harvest_source(title=title,
                                        url=url,
                                        owner_org_id=CKAN_ORG_ID,
                                        source_type='datajson',
                                        notes='Some tests about local harvesting sources creation',
                                        frequency='WEEKLY')

        self.assertTrue(res['success'])
        harvest_source = res['result']
        logger.info('Created: {}'.format(res['success']))

        # read it
        res = cpa.show_package(ckan_package_id_or_name=harvest_source['id'])
        self.assertTrue(res['success'])
        self.assertEqual(harvest_source['url'], url)
        self.assertEqual(harvest_source['title'], title)
        self.assertEqual(harvest_source['type'], 'harvest')
        self.assertEqual(harvest_source['source_type'], 'datajson')

        # search for it
        results = cpa.search_harvest_packages(rows=1000,
                                               harvest_type='harvest',
                                               source_type='datajson'
                                               )

        created_ok = False

        for datasets in results:
            for dataset in datasets:
                # print('FOUND: {}'.format(dataset['name']))
                if dataset['name'] == harvest_source['name']:
                    created_ok = True
                    logger.info('Found!')
                else:
                    logger.info('Other harvest source: {}'.format(dataset['name']))

        assert created_ok == True

        # create a dataset with this harvest_soure_id
        dataset_title = 'Dataset number {}'.format(random.randint(1, 999999))
        dataset_name = slugify(dataset_title)
        tags = [{'name': 'tag81'}, {'name': 'tag82'}]

        randval = random.randint(1, 999)
        extras = [
            {'key': 'harvest_source_id', 'value': harvest_source['id']},
            {'key': 'harvest_source_title', 'value': harvest_source['title']},
            # {'key': 'harvest_object_id', 'value': harvest_source['id']},  # ? not sure
            {'key': 'harvest_ng_source_id', 'value': harvest_source['id']},
            {'key': 'harvest_ng_source_title', 'value': harvest_source['title']},
            {'key': 'try_a_extra', 'value': randval}
            ]

        package = {'name': dataset_name,
                   'title': dataset_title, 'owner_org': CKAN_ORG_ID,
                   'tags': tags,
                   'extras': extras}
        res2 = cpa.create_package(ckan_package=package)
        self.assertTrue(res2['success'])
        logger.info('Package with harvest source: {}'.format(res2['success']))

        # read full dataset
        res3 = cpa.show_package(ckan_package_id_or_name=dataset_name)
        self.assertTrue(res3['success'])
        ckan_dataset = res3['result']
        logger.info('Package with harvest source readed: {}'.format(ckan_dataset))

        assert 'extras' in ckan_dataset
        assert [str(randval)] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'try_a_extra']
        # my custom ID (not connected to a real harvest ID)
        assert [harvest_source['id']] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'harvest_ng_source_id']

        # check if this package is related to harvest source
        total_datasets_in_source = 0
        datasets_from_source = cpa.search_harvest_packages(harvest_source_id=harvest_source['id'])
        connected_ok = False
        for datasets in datasets_from_source:
            for dataset in datasets:
                total_datasets_in_source += 1
                if dataset['name'] == dataset_name:
                    connected_ok = True
                    logger.info('Found!')
                else:
                    # we just expect one dataset
                    error = '{} != {} ------ {}'.format(dataset['name'], dataset_name, dataset)
                    logger.error(error)
                    assert error == False

        assert connected_ok == True
        assert total_datasets_in_source == 1
        logger.info(f' +++++++++++++ total_datasets_in_source={total_datasets_in_source}')

        # this fails, harvest process is more complex that just add an extra
        # assert [harvest_source['id']] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'harvest_source_id']

        # delete both
        logger.info('Delete CKAN package: {}'.format(ckan_dataset['id']))
        res4 = cpa.delete_package(ckan_package_id_or_name=ckan_dataset['id'])
        self.assertTrue(res4['success'])

        logger.info('Delete Harvest source: {}'.format(harvest_source['id']))
        res5 = cpa.delete_package(ckan_package_id_or_name=harvest_source['id'])
        self.assertTrue(res5['success'])

    def test_get_admins(self):

        cpa = CKANPortalAPI(base_url=CKAN_BASE_URL, api_key=CKAN_API_KEY)

        res = cpa.get_admin_users(organization_id=CKAN_ORG_ID)
        print(res)
        self.assertTrue(res['success'])

    def test_get_user_info(self):

        cpa = CKANPortalAPI(base_url=CKAN_BASE_URL, api_key=CKAN_API_KEY)

        res = cpa.get_user_info(user_id=CKAN_VALID_USER_ID)
        print(res)
        self.assertTrue(res['success'])

    def test_create_organization(self):

        cpa = CKANPortalAPI(base_url=CKAN_BASE_URL, api_key=CKAN_API_KEY)

        title = 'Organization number {}'.format(random.randint(1, 999999))
        name = slugify(title)

        organization = {
            'name': name,  # (string) – the name of the organization
            'id': '',  #  (string) – the id of the organization (optional)
            'title': title,  #  (string) – the title of the organization (optional)
            'description': 'Description {}'.format(title),  #  (string) – the description of the organization (optional)
            'image_url': 'http://sociologycanvas.pbworks.com/f/1357178020/1357178020/Structure.JPG',  #  (string) – the URL to an image to be displayed on the organization’s page (optional)
            'state': 'active',  #  (string) – the current state of the organization, e.g. 'active' or 'deleted'
            'approval_status': 'approved'  #  (string) – (optional)
        }

        res = cpa.create_organization(organization=organization)
        print(res)
        self.assertTrue(res['success'])

        # try to duplicate ir
        res = cpa.create_organization(organization=organization, check_if_exists=True)
        print(res)
        self.assertTrue(res['success'])

        res = cpa.show_organization(organization_id_or_name=name)
        print('**************\n{}\n****************\n'.format(res))
        self.assertTrue(res['success'])