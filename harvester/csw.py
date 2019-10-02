"""
CSW stands for Catalog Service for the Web

Real Life cases (from catalog.data.gov):

Alaska LCC CSW Server: http://metadata.arcticlcc.org/csw
NC OneMap CSW: http://data.nconemap.com/geoportal/csw?Request=GetCapabilities&Service=CSW&Version=2.0.2
USACE Geospatial CSW: http://metadata.usace.army.mil/geoportal/csw?Request=GetCapabilities&Service=CSW&Version=2.0.2
2017_arealm: https://meta.geo.census.gov/data/existing/decennial/GEO/GPMB/TIGERline/TIGER2017/arealm/
GeoNode State CSW: http://geonode.state.gov/catalogue/csw?service=CSW&version=2.0.2&request=GetRecords&typenames=csw:Record&elementsetname=brief
OpenTopography CSW: https://portal.opentopography.org/geoportal/csw
"""
import requests
import json
from slugify import slugify
from urllib.parse import urlparse, urlencode, urlunparse
from owslib.csw import CatalogueServiceWeb, namespaces
from owslib.ows import ExceptionReport
from owslib import util
import xml.etree.ElementTree as xet
from harvester.iso_geo import ISODocument
from harvester.logs import logger


class CSWSource:
    """ A CSW Harvest Source """

    csw = None
    csw_info = {}

    errors = []
    datasets = []  # all datasets included
    duplicates = []  # list of datasets with the same identifier

    def __init__(self, url):
        self.url = url
        self.csw = None

    def get_cleaned_url(self):
        # remove all URL params
        parts = urlparse(self.url)
        return urlunparse((parts.scheme, parts.netloc, parts.path, None, None, None))

    def connect_csw(self, clean_url=True, timeout=120):
        # connect to csw source
        url = self.get_cleaned_url() if clean_url else self.url
        try:
            self.csw = CatalogueServiceWeb(url, timeout=timeout)
        except Exception as e:
            error = f'Error connection CSW: {e}'
            self.errors.append(error)
            return False

        self.read_csw_info()
        return True

    def as_json(self):
        self.read_csw_info()
        return self.csw_info

    def get_records(self, page=10, outputschema='gmd', esn='brief'):
        # iterate pages to get all records
        self.csw_info['records'] = {}
        self.csw_info['pages'] = 0

        # TODO get filters fom harvest source
        # https://github.com/GSA/ckanext-spatial/blob/datagov/ckanext/spatial/harvesters/csw.py#L90
        cql = None

        # output schema
        # outputschema: the outputSchema (default is 'http://www.opengis.net/cat/csw/2.0.2')
        # "csw" at GeoDataGovGeoportalHarvester
        # "gmd" at CSWHarvester
        # outputschema = 'gmd'  # https://github.com/geopython/OWSLib/blob/master/owslib/csw.py#L551

        startposition = 0
        kwa = {
            "constraints": [],
            "typenames": 'csw:Record',
            "esn": esn,
            # esn: the ElementSetName 'full', 'brief' or 'summary' (default is 'full')
            "startposition": startposition,
            "maxrecords": page,
            "outputschema": namespaces[outputschema],
            "cql": cql,
            }

        matches = 0
        self.csw_info['records'] = {}
        while True:
            try:
                self.csw.getrecords2(**kwa)
            except Exception as e:
                error = f'Error getting records(2): {e}'
                self.errors.append(error)
                break

            if self.csw.exceptionreport:
                exceptions = self.csw.exceptionreport.exceptions
                error = 'Error getting records: {}'.format(exceptions)
                self.errors.append(error)
                # raise Exception(error)
                break

            self.csw_info['pages'] += 1
            if matches == 0:
                matches = self.csw.results['matches']

            records = self.csw.records.items()

            for record in records:
                key, csw_record = record
                if outputschema == 'gmd':
                    # it's a MD_Metadata object
                    # https://github.com/geopython/OWSLib/blob/3338340e6a9c19dd3388240815d35d60a0d0cf4c/owslib/iso.py#L31
                    value = self.md_metadata_to_dict(csw_record)
                elif outputschema == 'csw':
                    # it's a CSWResource
                    error = 'Not using CSW schema, we require GMD'
                    value['error'] = error

                try:
                    value['iso_values'] = self.read_values_from_xml(xml_data=value['content'])
                except Exception as e:
                    error = f'Error reading ISO values {e}'
                    value['error'] = error
                    raise  # Exception(error)


                value['esn'] = esn
                self.csw_info['records'][key] = value
                yield value

            if len(records) == 0:
                break

            startposition += page
            if startposition > matches:
                break

            kwa["startposition"] = startposition

        self.csw_info['total_records'] = len(self.csw_info['records'].keys())

    def get_record(self, identifier, esn='full', outputschema='gmd'):
        #  Get Full record info
        try:
            records = self.csw.getrecordbyid([identifier], outputschema=namespaces[outputschema])
        except ExceptionReport as e:
            self.errors.append(f'Error getting record {e}')
            # 'Invalid parameter value: locator=outputSchema' is an XML error
            return None

        csw_record = self.csw.records[identifier]
        dict_csw_record = self.md_metadata_to_dict(csw_record)

        record = self.csw_info['records'].get(identifier, {})
        record.update(dict_csw_record)
        record['esn'] = esn
        record['outputschema'] = outputschema

        self.csw_info['records'][identifier] = record

        return record

    def read_values_from_xml(self, xml_data):
        # transform the XML in a dict as ISODocument class
        # (https://github.com/GSA/ckanext-spatial/blob/2a25f8d60c31add77e155c4136f2c0d4e3b86385/ckanext/spatial/model/harvested_metadata.py#L461) did with its read_values function.

        iso_parser = ISODocument(xml_str=xml_data)
        return iso_parser.read_values()

    def process_xml(self, raw_xml):
        # get the XML part we need
        # check samples at /samples folder

        try:
            str_xml = raw_xml.decode('utf-8')
        except Exception as e:
            error = f'Unable to decode bytes as UTF-8: {e}'
            raise Exception(error)
        str_xml = str_xml.replace('\\n', '\n').replace('\\t', '\t')

        try:
            mdtree = xet.fromstring(str_xml)
        except Exception as e:
            error = f'{e}\n\n - Unable to parse string. \n\n: \t{str_xml[:350]} \n\n'
            raise Exception(error)

        # check if root IS what we are looking for
        needed = ['{http://www.isotc211.org/2005/gmd}MD_Metadata', '{http://www.isotc211.org/2005/gmi}MI_Metadata']
        if mdtree.tag in needed:
            gm = mdtree
        else:
            # https://docs.python.org/3/library/xml.etree.elementtree.html#parsing-xml-with-namespaces
            ns = {'gmd': 'http://www.isotc211.org/2005/gmd',
                  'gmi': 'http://www.isotc211.org/2005/gmi'}

            gm1 = mdtree.find('gmd:MD_Metadata', ns)
            gm2 = mdtree.find('gmi:MI_Metadata', ns)
            gm = gm1 or gm2
            # if we have not a xmlns reference the search fails
            if gm is None:
                gm1 = mdtree.find('MD_Metadata')
                gm2 = mdtree.find('MI_Metadata')

            if gm is None:
                error = f'Unable to find MD_Metadata. \n\n: \t{str_xml[:150]} \n\n mdtree.root: {mdtree.tag} tg:"{tg}"'
                raise Exception(error)
        try:
            res = xet.tostring(gm)
        except Exception as e:
            error = f'{e}\n\n - gm1:{gm1} gm2:{gm2}\n\n Unable to string. \n\n: \t{str_xml[:150]} \n\n mdtree.root: {mdtree.tag}'
            raise Exception(error)

        if type(res) != str:
            res = res.decode('utf-8')

        return res

    def md_metadata_to_dict(self, mdm):
        # analyze an md_metadata object
        ret = {}

        ret['content'] = self.process_xml(raw_xml=mdm.xml)
        res = '<?xml version="1.0" encoding="UTF-8"?>\n{}'.format(ret['content'])
        ret['xml'] = res
        ret['identifier'] = mdm.identifier
        ret['parentidentifier'] = mdm.parentidentifier
        ret['language'] = mdm.language
        ret['dataseturi'] = mdm.dataseturi
        ret['languagecode'] = mdm.languagecode
        ret['datestamp'] = mdm.datestamp
        ret['charset'] = mdm.charset
        ret['hierarchy'] = mdm.hierarchy
        ret['contact'] = []
        for ctc in mdm.contact:
            contact = {'name': ctc.name,
                       'organization': ctc.organization,
                       'city': ctc.city,
                       'email': ctc.email,
                       'country': ctc.country}
            ret['contact'].append(contact)

        ret['datetimestamp'] = mdm.datetimestamp
        ret['stdname'] = mdm.stdname
        ret['stdver'] = mdm.stdver
        ret['locales'] = []
        for lo in mdm.locales:
            ret['locales'].append({'id': lo.id,
                                   'languagecode': lo.languagecode,
                                   'charset': lo.charset})

        # ret['referencesystem'] = mdm.referencesystem
        # this two will be reemplaced by "identificationinfo"
        #   ret['identification'] = mdm.identification
        #   ret['serviceidentification'] = mdm.serviceidentification
        ret['identificationinfo'] = []
        for ii in mdm.identificationinfo:
            iid = {'title': ii.title,
                   'abstract': ii.abstract}  # there are much more info
            ret['identificationinfo'].append(iid)

        ret['contentinfo'] = []
        for ci in mdm.contentinfo:
            cid = {'xml': ci.xml}  # there are much more info
            ret['contentinfo'].append(cid)

        ret['distribution'] = {}
        if mdm.distribution is not None:
            dd = {'format': mdm.distribution.format,
                  'version': mdm.distribution.version}  # there are much more info
            ret['distribution'] = dd

        # TODO ret['dataquality'] = mdm.dataquality
        return ret

    def read_csw_info(self):
        csw_info = {}
        service = self.csw
        # Check each service instance conforms to OWSLib interface
        service.alias = 'CSW'
        csw_info['version'] = service.version
        csw_info['identification'] = {}  # service.identification
        csw_info['identification']['type'] = service.identification.type
        csw_info['identification']['version'] = service.identification.version
        csw_info['identification']['title'] = service.identification.title
        csw_info['identification']['abstract'] = service.identification.abstract
        csw_info['identification']['keywords'] = service.identification.keywords
        csw_info['identification']['accessconstraints'] = service.identification.accessconstraints
        csw_info['identification']['fees'] = service.identification.fees

        csw_info['provider'] = {}
        csw_info['provider']['name'] = service.provider.name
        csw_info['provider']['url'] = service.provider.url
        ctc = service.provider.contact
        contact = {'name': ctc.name,
                   'organization': ctc.organization,
                   'site': ctc.site,
                   'instructions': ctc.instructions,
                   'email': ctc.email,
                   'country': ctc.country}
        csw_info['provider']['contact'] = contact

        csw_info['operations'] = []
        for op in service.operations:
            methods = op.methods
            for method in methods:
                if type(method) == dict:
                    constraints = []
                    for k, v in method.items():
                        if k == 'constraints':
                            for c in v:
                                if type(c) == dict:
                                    constraints.append(c)
                                else:
                                    mc = {'name': c.name, 'values': c.values}
                                    constraints.append(mc)
                            method['constraints'] = constraints

            operation = {'name': op.name,
                         'formatOptions': op.formatOptions,
                         'methods': methods}
            csw_info['operations'].append(operation)

        self.csw_info.update(csw_info)
        return self.csw_info

    def get_original_url(self, harvest_id=None):
        # take the URL and add required params
        parts = urlparse(self.url)
        # urlparse('http://www.cwi.nl:80/%7Eguido/Python.html?q=90&p=881')
        # ParseResult(scheme='http', netloc='www.cwi.nl:80', path='/%7Eguido/Python.html', params='', query='q=90&p=881', fragment='')

        params = {
            'SERVICE': 'CSW',
            'VERSION': '2.0.2',
            'REQUEST': 'GetRecordById',
            'OUTPUTSCHEMA': 'http://www.isotc211.org/2005/gmd',
            'OUTPUTFORMAT': 'application/xml',
        }
        if harvest_id is not None:
            params['ID'] = harvest_id

        url = urlunparse((
            parts.scheme,
            parts.netloc,
            parts.path,
            None,
            urlencode(params),
            None
        ))

        return url

    def validate(self):
        errors = []  # to return list of validation errors
        # return False, errors

        return True, None

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
            pass  # TODO
        return total

    def save_data_json(self, path):
        """ save the source data.json file """
        dmp = json.dumps(self.as_json(), indent=2)
        f = open(path, 'w')
        f.write(dmp)
        f.close()

    def save_errors(self, path):
        dmp = json.dumps(self.errors, indent=2)
        f = open(path, 'w')
        f.write(dmp)
        f.close()

    def save_duplicates(self, path):
        dmp = json.dumps(self.duplicates, indent=2)
        f = open(path, 'w')
        f.write(dmp)
        f.close()

    def save_datasets_as_data_packages(self, folder_path):
        """ save each dataset from a data.json source as _datapackage_ """
        for dataset in self.datasets:
            package = Package()

            #TODO check this, I'm learning datapackages
            resource = Resource({'data': dataset})
            resource.infer()  #adds "name": "inline"

            #FIXME identifier uses incompables characthers as paths (e.g. /).
            # could exist duplicates paths from different resources
            # use BASE64 or hashes
            idf = slugify(dataset['identifier'])

            resource_path = os.path.join(folder_path, f'resource_data_json_{idf}.json')
            if not resource.valid:
                raise Exception('Invalid resource')

            resource.save(resource_path)

            package.add_resource(descriptor=resource.descriptor)
            package_path = os.path.join(folder_path, f'pkg_data_json_{idf}.zip')
            package.save(target=package_path)


