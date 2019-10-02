import json
import requests
import os
from datapackage import Package, Resource
from slugify import slugify
import base64
from harvester.logs import logger

class CKANPortalAPI:
    """ API and data from data.gov
        API SPECS: https://docs.ckan.org/en/latest/api/index.html """

    version = '0.01-alpha'
    user_agent = 'ckan-portal-filter'
    api_key = None  # needed for some calls
    package_list_url = '/api/3/action/package_list'  # redirect to package_search (?)
    package_search_url = '/api/3/action/package_search'  # iterate with start and rows GET params
    package_create_url = '/api/3/action/package_create'
    package_update_url = '/api/3/action/package_update'
    package_delete_url = '/api/3/action/package_delete'
    package_show_url = '/api/3/action/package_show'
    organization_create_url = '/api/3/action/organization_create'
    organization_update_url = '/api/3/action/organization_update'
    organization_show_url = '/api/3/action/organization_show'

    # get members
    member_list_url = '/api/3/action/member_list'
    user_show_url = '/api/3/action/user_show'
    package_list = []
    total_packages = 0

    def __init__(self, base_url='https://catalog.data.gov', api_key=None):  # default data.gov
        self.base_url = base_url
        self.api_key = api_key

    def get_request_headers(self, include_api_key=False):
        headers = {'User-Agent': f'{self.user_agent} {self.version}'}
        if include_api_key:
            # headers['Autorization'] = self.api_key
            headers['X-CKAN-API-Key'] = self.api_key
        return headers

    def search_harvest_packages(self,
                                rows=1000,
                                method='POST',  # POST work in CKAN 2.8, fails in 2.3
                                harvest_source_id=None,  # just one harvest source
                                harvest_type=None,  # harvest for harvest sources
                                source_type=None):
        """ search harvested packages or harvest sources
            "rows" is the page size.
            You could search for an specific harvest_source_id """

        start = 0
        sort = "metadata_modified desc"

        url = '{}{}'.format(self.base_url, self.package_search_url)
        page = 0
        # TODO check for a real paginated version
        while url:
            page += 1

            params = {'start': start, 'rows': rows}  # , 'sort': sort}
            if harvest_source_id is not None:
                # you can't search by any extras
                # https://github.com/ckan/ckan/blob/30ca7aae2f2aca6a19a2e6ed29148f8428e25c86/ckan/logic/action/get.py#L1852
                # params['ext_harvest_source_id'] = harvest_source_id
                # params['ext_harvest_ng_source_id'] = harvest_source_id
                # params['extras'] = {'ext_harvest_ng_source_id': harvest_source_id}
                # params['q'] = f'harvest_source_id:{harvest_source_id}'

                # ---------------
                # this must work
                # ---------------
                # https://github.com/ckan/ckanext-harvest/blob/3a72337f1e619bf9ea3221037ca86615ec22ae2f/ckanext/harvest/helpers.py#L38
                # params['fq'] = f'+harvest_source_id:"{harvest_source_id}"'
                # but is not working. For some reason exta harvest_source_id doesn't exists

                # our new extra is working
                params['fq'] = f'+harvest_ng_source_id:"{harvest_source_id}"'

            elif harvest_type is not None:
                # at my local instance I need this.
                # I not sure why, in another public instances is not needed
                params['fq'] = f'+dataset_type:{harvest_type}'
                if source_type is not None:
                    params['q'] = f'(type:{harvest_type} source_type:{source_type})'
                else:
                    params['q'] = f'(type:{harvest_type})'

            logger.info(f'Searching {url} PAGE:{page} start:{start}, rows:{rows} with params: {params}')

            headers = self.get_request_headers()
            try:
                logger.info(f'Search harvest packages via {method}')
                if method == 'POST':  # depend on CKAN version
                    req = requests.post(url, data=params, headers=headers)
                else:
                    req = requests.get(url, params=params, headers=headers)

            except Exception as e:
                error = 'ERROR Donwloading package list: {} [{}]'.format(url, e)
                raise ValueError('Failed to get package list at {}'.format(url))

            content = req.content

            if req.status_code >= 400:
                error = ('ERROR searching CKAN package: {}'
                         '\n\t Status code: {}'
                         '\n\t Params: {}'
                         '\n\t content:{}'.format(url, req.status_code, params, content))
                logger.error(error)
                raise Exception(error)

            try:
                json_content = json.loads(content)  # check for encoding errors
            except Exception as e:
                error = 'ERROR parsing JSON data: {} [{}]'.format(content, e)
                raise ValueError(error)

            if not json_content['success']:
                error = 'API response failed: {}'.format(json_content.get('error', None))
                raise ValueError(error)

            result = json_content['result']
            count_results = result['count']
            sort_results = result['sort']
            facet_results = result['facets']
            results = result['results']
            real_results_count = len(results)
            self.total_packages += real_results_count
            logger.info(f'{real_results_count} results')

            if real_results_count == 0:
                url = None
            else:
                start += rows
                self.package_list += results
                logger.debug(f'datasets found: {results}')
                yield(results)

    def search_packages(self,
                        rows=1000,
                        method='POST',  # POST work in CKAN 2.8, fails in 2.3
                        search_params={}
                        ):  # datajson for
        """ search packages.
            "rows" is the page size.
            """

        start = 0

        url = '{}{}'.format(self.base_url, self.package_search_url)
        page = 0
        # TODO check for a real paginated version
        while url:
            page += 1

            params = {'start': start, 'rows': rows}
            params.update(search_params)
            logger.info(f'Searching packages {url} PAGE:{page} start:{start}, rows:{rows} with params: {params}')

            headers = self.get_request_headers()
            try:
                if method == 'POST':  # depend on CKAN version
                    req = requests.post(url, data=params, headers=headers)
                else:
                    req = requests.get(url, params=params, headers=headers)

            except Exception as e:
                error = 'ERROR Donwloading package list: {} [{}]'.format(url, e)
                raise ValueError('Failed to get package list at {}'.format(url))

            content = req.content

            if req.status_code >= 400:
                error = ('ERROR searching CKAN package: {}'
                         '\n\t Status code: {}'
                         '\n\t Params: {}'
                         '\n\t content:{}'.format(url, req.status_code, params, content))
                logger.error(error)
                raise Exception(error)

            try:
                json_content = json.loads(content)  # check for encoding errors
            except Exception as e:
                error = 'ERROR parsing JSON data: {} [{}]'.format(content, e)
                raise ValueError(error)

            if not json_content['success']:
                error = 'API response failed: {}'.format(json_content.get('error', None))
                raise ValueError(error)

            result = json_content['result']
            results = result['results']
            real_results_count = len(results)
            self.total_packages += real_results_count
            logger.info(f'{real_results_count} results')

            if real_results_count == 0:
                url = None
            else:
                start += rows
                self.package_list += results
                logger.debug(f'datasets found: {results}')
                yield(results)

    def get_all_packages(self, harvest_source_id=None,  # just one harvest source
                                harvest_type=None,  # 'harvest' for harvest sources
                                source_type=None):
        self.package_list = []
        self.total_pages = 0
        for packages in self.search_harvest_packages(harvest_source_id=harvest_source_id,
                                                    harvest_type=harvest_type,
                                                    source_type=source_type):

            self.total_pages += 1

    def read_local_packages(self, path):
        if not os.path.isfile(path):
            return False, "File not exists"
        packages_file = open(path, 'r')
        try:
            self.package_list = json.load(packages_file)
        except Exception as e:
            return False, "Error parsin json: {}".format(e)
        return True, None

    def count_resources(self):
        """ read all datasets and count resources """
        total = 0
        for dataset in self.package_list:
            resources = dataset.get('resources', [])
            total += len(resources)
        return total

    def remove_duplicated_identifiers(self):
        unique_identifiers = []
        self.duplicates = []

        for dataset in self.package_list:
            idf = dataset['id']
            if idf not in unique_identifiers:
                unique_identifiers.append(idf)
            else:
                self.duplicates.append(idf)
                self.package_list.remove(dataset)

        return self.duplicates

    def save_packages_list(self, path):
        dmp = json.dumps(self.package_list, indent=2)
        f = open(path, 'w')
        f.write(dmp)
        f.close()

    def create_package_from_data_json(self, dictt):
        """ transform a data.json dataset/package to a CKAN one
            ############
            # check how to map fields: https://github.com/GSA/ckanext-datajson/blob/07ca20e0b6dc1898f4ca034c1e073e0c27de2015/ckanext/datajson/harvester_base.py#L444
            # check the parser: https://github.com/GSA/ckanext-datajson/blob/datagov/ckanext/datajson/parse_datajson.py#L5
            ############
            Analyze gather vs import stages
            https://github.com/GSA/ckanext-datajson/blob/07ca20e0b6dc1898f4ca034c1e073e0c27de2015/ckanext/datajson/harvester_base.py#L112

            https://github.com/GSA/ckanext-datajson/blob/07ca20e0b6dc1898f4ca034c1e073e0c27de2015/ckanext/datajson/harvester_base.py#L394

        """
        pass

    def create_package(self, ckan_package,
                       on_duplicated='RAISE',  # if name already exists 'RAISE' 'SKIP' | 'DELETE'
                       ):
        """ POST to CKAN API to create a new package/dataset
            ckan_package is just a python dict
            https://docs.ckan.org/en/2.8/api/#ckan.logic.action.create.package_create
        """
        url = '{}{}'.format(self.base_url, self.package_create_url)
        headers = self.get_request_headers(include_api_key=True)

        headers['Content-Type'] = 'application/json'
        ckan_package_str = json.dumps(ckan_package)

        logger.info(f'POST {url} headers:{headers} data:{ckan_package}')

        try:
            req = requests.post(url, data=ckan_package_str, headers=headers)
        except Exception as e:
            error = 'ERROR creating [POST] CKAN package: {} [{}]'.format(url, e)
            raise

        content = req.content
        try:
            json_content = json.loads(content)
        except Exception as e:
            error = 'ERROR parsing JSON data: {} [{}]'.format(content, e)
            logger.error(error)
            raise

        if req.status_code == 409:
            logger.info(f'409 json_content: {json_content}')
            # another posible [error] = {'owner_org': ['Organization does not exist']}

            # Check for duplicates
            if json_content['error'].get('name', None) == ["That URL is already in use."]:
                logger.error(f'Package Already exists! ACTION: {on_duplicated}')
                if on_duplicated == 'SKIP':
                    return {'success': True}
                elif on_duplicated == 'DELETE':
                    delr = self.delete_package(ckan_package_id_or_name=ckan_package['name'])
                    if not delr['success']:
                        raise Exception('Failed to delete {}'.format(ckan_package['name']))
                    return self.create_package(ckan_package=ckan_package, on_duplicated='RAISE')
                elif on_duplicated == 'RAISE':
                    error = ('DUPLICATED CKAN package: {}'
                             '\n\t Status code: {}'
                             '\n\t content:{}'
                             '\n\t Dataset {}'.format(url, req.status_code, content, ckan_package))
                    logger.error(error)
                    raise Exception(error)

        if req.status_code >= 400:
            error = ('ERROR creating CKAN package: {}'
                     '\n\t Status code: {}'
                     '\n\t content:{}'
                     '\n\t Dataset {}'.format(url, req.status_code, content, ckan_package))
            logger.error(error)
            raise Exception(error)

        if not json_content['success']:
            error = 'API response failed: {}'.format(json_content.get('error', None))
            logger.error(error)

        return json_content

    def create_harvest_source(self, title, url, owner_org_id, name=None,
                              notes='',
                              source_type='datajson',
                              frequency='MANUAL',
                              on_duplicated='DELETE'):
        """ create a harvest source (is just a CKAN dataset/package),
            required to try locally harcesting process
            Previous: https://github.com/ckan/ckanext-harvest/blob/3a72337f1e619bf9ea3221037ca86615ec22ae2f/ckanext/harvest/logic/action/create.py#L27"""

        if name is None:
            name = self.generate_name(title=title)

        # ----------------------------------------------------
        # since the CKAN rejects the unregistered harvest types
        # https://github.com/ckan/ckanext-harvest/blob/3a72337f1e619bf9ea3221037ca86615ec22ae2f/ckanext/harvest/logic/validators.py#L125
        # we use 'datajson' for all until we fix
        # we define an extra for identifying the real type
        # real_source_type = source_type
        # if source_type != 'datajson':
        #    source_type = 'datajson'
        # ----------------------------------------------------

        ckan_package = {
                "frequency": frequency,
                "title": title,
                "name": name,
                "type": "harvest",
                "source_type": source_type,
                "url": url,
                "notes": notes,
                "owner_org": owner_org_id,
                "private": False,
                "state": "active",
                "active": True,
                "tags": [{'name': 'harvest source'}],
                "config": None,
                "extras": [
                    # {'key': 'harvest_source_type', 'value': real_source_type}
                    ]
                }

        if type(ckan_package['config']) == dict:
            ckan_package['config'] = json.dumps(ckan_package['config'])

        return self.create_package(ckan_package=ckan_package, on_duplicated=on_duplicated)

    def generate_name(self, title):
        # names are unique in CKAN
        # old harvester do like this: https://github.com/GSA/ckanext-datajson/blob/07ca20e0b6dc1898f4ca034c1e073e0c27de2015/ckanext/datajson/harvester_base.py#L747

        name = slugify(title)
        if len(name) > 95:  # max length is 100
            name = name[:95]

        return name

    def update_package(self, ckan_package):
        """ POST to CKAN API to update a package/dataset
            ckan_package is just a python dict
            https://docs.ckan.org/en/2.8/api/#ckan.logic.action.update.package_update
        """
        url = '{}{}'.format(self.base_url, self.package_update_url)
        headers = self.get_request_headers(include_api_key=True)

        headers['Content-Type'] = 'application/json'
        ckan_package = json.dumps(ckan_package)

        logger.error(f'POST {url} headers:{headers} data:{ckan_package}')
        try:
            req = requests.post(url, data=ckan_package, headers=headers)
        except Exception as e:
            error = 'ERROR creating CKAN package: {} [{}]'.format(url, e)
            raise

        content = req.content

        if req.status_code >= 400:
            error = 'ERROR updateing CKAN package: {} \n\t Status code: {} \n\t content:{}'.format(url, req.status_code, content)
            logger.error(error)
            raise Exception(error)

        try:
            json_content = json.loads(content)
        except Exception as e:
            error = 'ERROR parsing JSON data: {} [{}]'.format(content, e)
            raise

        if not json_content['success']:
            error = 'API response failed: {}'.format(json_content.get('error', None))
            logger.error(error)

        return json_content

    def delete_package(self, ckan_package_id_or_name):
        """ POST to CKAN API to delete a new package/dataset
            https://docs.ckan.org/en/2.8/api/#ckan.logic.action.delete.package_delete
        """
        url = '{}{}'.format(self.base_url, self.package_delete_url)
        headers = self.get_request_headers(include_api_key=True)
        data = {'id': ckan_package_id_or_name}
        logger.error(f'POST {url} headers:{headers} data:{data}')
        try:
            req = requests.post(url, data=data, headers=headers)
        except Exception as e:
            error = 'ERROR deleting CKAN package: {} [{}]'.format(url, e)
            raise

        content = req.content

        if req.status_code >= 400:
            error = 'ERROR deleting CKAN package: {} \n\t Status code: {} \n\t content:{}'.format(url, req.status_code, content)
            logger.error(error)
            raise Exception(error)

        try:
            json_content = json.loads(content)
        except Exception as e:
            error = 'ERROR parsing JSON data from delete_package: {} [{}]'.format(content, e)
            raise

        if not json_content['success']:
            error = 'API response failed: {}'.format(json_content.get('error', None))
            logger.error(error)

        return json_content

    def show_package(self, ckan_package_id_or_name):
        """ GET to CKAN API to show a package/dataset """

        url = '{}{}'.format(self.base_url, self.package_show_url)
        headers = self.get_request_headers()
        data = {'id': ckan_package_id_or_name}
        logger.info(f'POST {url} headers:{headers} data:{data}')
        try:
            req = requests.get(url, params=data, headers=headers)
        except Exception as e:
            error = 'ERROR showing CKAN package: {} [{}]'.format(url, e)
            raise

        content = req.content
        if req.status_code >= 400:
            error = 'ERROR showing CKAN package: {} \n\t Status code: {} \n\t content:{}'.format(url, req.status_code, content)
            logger.error(error)
            raise Exception(error)

        content = req.content

        try:
            json_content = json.loads(content)
        except Exception as e:
            error = 'ERROR parsing JSON data from show_package: {} [{}]'.format(content, e)
            raise

        if not json_content['success']:
            error = 'API response failed: {}'.format(json_content.get('error', None))
            logger.error(error)

        return json_content

    def save_datasets_as_data_packages(self, folder_path):
        """ save each dataset source as _datapackage_ """
        for dataset in self.package_list:
            package = Package()

            #TODO check this, I'm learning datapackages
            resource = Resource({'data': dataset})
            resource.infer()
            identifier = dataset['id']
            bytes_identifier = identifier.encode('utf-8')
            encoded = base64.b64encode(bytes_identifier)
            encoded_identifier = str(encoded, "utf-8")

            resource_path = os.path.join(folder_path, f'resource_ckan_api_{encoded_identifier}.json')
            if not resource.valid:
                raise Exception('Invalid resource')

            resource.save(resource_path)

            package.add_resource(descriptor=resource.descriptor)
            package_path = os.path.join(folder_path, f'pkg_ckan_api_{encoded_identifier}.zip')
            package.save(target=package_path)

    def get_admin_users(self, organization_id):
        """ GET to CKAN API to get list of admins
            https://docs.ckan.org/en/2.8/api/#ckan.logic.action.get.member_list
        """
        url = '{}{}?id={}&object_type=user&capacity=admin'.format(self.base_url, self.member_list_url, organization_id)
        headers = self.get_request_headers(include_api_key=True)
        logger.info(f'GET {url} headers:{headers}')
        try:
            req = requests.get(url, headers=headers)
        except Exception as e:
            error = 'ERROR getting organization members: {} [{}]'.format(url, e)
            raise

        content = req.content

        if req.status_code >= 400:
            error = 'ERROR getting organization members: {} \n\t Status code: {} \n\t content:{}'.format(url, req.status_code, content)
            logger.error(error)
            raise Exception(error)

        try:
            json_content = json.loads(content)
        except Exception as e:
            error = 'ERROR parsing JSON data from organization members {} [{}]'.format(content, e)
            raise

        if not json_content['success']:
            error = 'API response failed: {}'.format(json_content.get('error', None))
            logger.error(error)

        return json_content

    def get_user_info(self, user_id):
        """ GET to CKAN API to get list of admins
            https://docs.ckan.org/en/2.8/api/#ckan.logic.action.get.user_show
        """
        url = '{}{}?id={}'.format(self.base_url, self.user_show_url, user_id)
        headers = self.get_request_headers(include_api_key=True)
        logger.info(f'GET {url} headers:{headers}')
        try:
            req = requests.get(url, headers=headers)
        except Exception as e:
            error = 'ERROR getting users information: {} [{}]'.format(url, e)
            raise

        content = req.content

        if req.status_code >= 400:
            error = 'ERROR getting users information: {} \n\t Status code: {} \n\t content:{}'.format(url, req.status_code, content)
            logger.error(error)
            raise Exception(error)

        try:
            json_content = json.loads(content)
        except Exception as e:
            error = 'ERROR parsing JSON data from users information {} [{}]'.format(content, e)
            raise

        if not json_content['success']:
            error = 'API response failed: {}'.format(json_content.get('error', None))
            logger.error(error)

        return json_content

    def delete_all_harvest_sources(self, harvest_type='harvest', source_type='datajson'):
        logger.info(f'Deleting local harvest sources')
        deleted = 0
        for harvest_sources in self.search_harvest_packages(harvest_type=harvest_type, source_type=source_type):
            for harvest_source in harvest_sources:
                harvest_source_name = harvest_source['name']
                logger.info(f'Deleting local harvest {harvest_source_name}')
                res = self.delete_package(ckan_package_id_or_name=harvest_source_name)
                if not res['success']:
                    raise Exception(f'Failed to delete {harvest_source_name}')
                else:
                    logger.info(f'Deleted {harvest_source_name}')
                    deleted += 1

        logger.info(f'{deleted} harvest sources deleted')
        return deleted

    def import_harvest_sources(self, catalog_url,
                               method='GET',  # depend on CKAN version, GET for older versions
                               on_duplicated='DELETE',
                               harvest_type='harvest',
                               source_type='datajson',
                               delete_local_harvest_sources=True):
        """ import harvest sources from another CKAN open data portal """

        if delete_local_harvest_sources:
            deleted = self.delete_all_harvest_sources(source_type=source_type)

        logger.info(f'Getting external harvest sources for {catalog_url}')
        external_portal = CKANPortalAPI(base_url=catalog_url)

        total_sources = 0
        search_external = external_portal.search_harvest_packages(method=method,
                                                                  harvest_type=harvest_type,
                                                                  source_type=source_type)
        for external_harvest_sources in search_external:
            for external_harvest_source in external_harvest_sources:
                name = external_harvest_source['name']

                organization = external_harvest_source['organization']
                logger.info(f'**** Importing Organization {organization}')
                # copy organization locally
                del organization['id']  # drop original ID
                del organization['created']
                del organization['revision_id']
                res = self.create_organization(organization=organization)
                owner_org_id = organization['name']

                # res = self.delete_package(name)
                logger.info(external_harvest_source)
                res = self.create_harvest_source(title=external_harvest_source['title'],
                                                url=external_harvest_source['url'],
                                                owner_org_id=owner_org_id,
                                                name=name,
                                                notes=external_harvest_source['notes'],
                                                source_type=source_type,
                                                frequency=external_harvest_source['frequency'],
                                                on_duplicated=on_duplicated)

                if not res['success']:
                    raise Exception(f'Failed to import harvest source {name}')
                else:
                    logger.info(f'Created {name}')
                    total_sources += 1

        return total_sources

    def create_organization(self, organization, check_if_exists=True):
        """ POST to CKAN API to create a new organization
            organization is just a python dict
            https://docs.ckan.org/en/2.8/api/#ckan.logic.action.create.organization_create
        """
        logger.info(f'**** Creating Organization {organization}')
        if check_if_exists:
            logger.info(f'Exists Organization? {organization}')
            res = self.show_organization(organization_id_or_name=organization['name'])
            if res['success']:
                # do not create
                logger.info(f'Avoid create Organization {organization}')
                return res

        url = '{}{}'.format(self.base_url, self.organization_create_url)
        headers = self.get_request_headers(include_api_key=True)

        headers['Content-Type'] = 'application/json'
        organization = json.dumps(organization)

        logger.info(f'POST {url} headers:{headers} data:{organization}')

        try:
            req = requests.post(url, data=organization, headers=headers)
        except Exception as e:
            error = 'ERROR creating [POST] organization: {} [{}]'.format(url, e)
            raise

        content = req.content

        if req.status_code >= 400:

            error = ('ERROR creating [STATUS] organization: {}'
                     '\n\t Status code: {}'
                     '\n\t content:{}'
                     '\n\t Dataset {}'.format(url, req.status_code, content, organization))
            logger.error(error)
            raise Exception(error)

        try:
            json_content = json.loads(content)
        except Exception as e:
            error = 'ERROR parsing JSON data: {} [{}]'.format(content, e)
            logger.error(error)
            raise

        if not json_content['success']:
            error = 'API response failed: {}'.format(json_content.get('error', None))
            logger.error(error)

        return json_content

    def show_organization(self,
                          organization_id_or_name,
                          method='POST'):  # troubles using 2.3 and 2.8 CKAN versions):
        """ GET to CKAN API to show a organization """

        url = '{}{}'.format(self.base_url, self.organization_show_url)
        headers = self.get_request_headers()
        data = {'id': organization_id_or_name}
        logger.info(f'POST {url} headers:{headers} data:{data}')
        try:
            if method == 'POST':
                req = requests.post(url, data=data, headers=headers)
            else:
                req = requests.get(url, params=data, headers=headers)
        except Exception as e:
            error = 'ERROR showing organization: {} [{}]'.format(url, e)
            raise

        content = req.content

        if req.status_code >= 400 and req.status_code != 404:
            error = 'ERROR showing organization: {} \n\t Status code: {} \n\t content:{}'.format(url, req.status_code, content)
            logger.error(error)
            raise Exception(error)

        try:
            json_content = json.loads(content)
        except Exception as e:
            error = 'ERROR parsing JSON data from show_organization: {} [{}]'.format(content, e)
            raise

        if not json_content['success']:
            error = 'API response failed: {}'.format(json_content.get('error', None))
            logger.error(error)

        return json_content


