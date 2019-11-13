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

from harvesters.datajson.validator_constants import (ACCRUAL_PERIODICITY_VALUES, BUREAU_CODE_URL,
                                      IANA_MIME_REGEX, ISO8601_REGEX, ISSUED_REGEX,
                                      LANGUAGE_REGEX, MODIFIED_REGEX_1,
                                      MODIFIED_REGEX_2, MODIFIED_REGEX_3,
                                      PRIMARY_IT_INVESTMENT_UII_REGEX,
                                      PROGRAM_CODE_REGEX, REDACTED_REGEX,
                                      TEMPORAL_REGEX_1, TEMPORAL_REGEX_2,
                                      TEMPORAL_REGEX_3)
from harvesters.logs import logger
from harvesters.harvester import HarvesterBaseSource


class JSONSchema:
    """ a JSON Schema definition for validating data.json files """
    json_content = None  # schema content
    valid_schemas = {  # schemas we know
                "https://project-open-data.cio.gov/v1.1/schema": '1.1',
                }

    def __init__(self, url):
        self.url = url  # URL of de schema definition. e.g. https://project-open-data.cio.gov/v1.1/schema/catalog.json
        try:
            req = requests.get(self.url)
        except Exception as e:
            error = 'ERROR Donwloading schema: {} [{}]'.format(self.url, e)
            raise ValueError('Failed to get schema definition at {}'.format(url))

        content = req.content
        try:
            self.json_content = json.loads(content)  # check for encoding errors
        except Exception as e:
            error = 'ERROR parsing JSON data: {} [{}]'.format(content, e)
            raise ValueError(error)


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

    def validate(self):

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
        elif not self.data_json.get('describedBy', False):
            error = 'Missing describedBy KEY'  
        elif not self.data_json.get('dataset', False):
            error = 'Missing "dataset" KEY'
            
        if error is not None:
            self.errors.append(error)
            logger.error(error)
            return False

        schema_definition_url = self.data_json['describedBy']
        self.schema = JSONSchema(url=schema_definition_url)
        
        validated_schema = self.validate_schema()
        if not validated_schema:
            return False
        
        # validate with jsonschema lib
        # many data.json are not extrictly valid, we use as if they are

        # TODO check and re-use a ckanext-datajson validator: https://github.com/GSA/ckanext-datajson/blob/datagov/ckanext/datajson/datajsonvalidator.py

        try:
            schema = self.schema.json_content
            jss.validate(instance=self.data_json, schema=schema)
        except Exception as e:
            error = "Error validating JsonSchema: {} with schema {}".format(e, schema)
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

    def validate_schema(self):
        """ validate using jsonschema lib """

        #TODO check how ckanext-datajson uses jsonschema. One example (there are more) https://github.com/GSA/ckanext-datajson/blob/datagov/ckanext/datajson/harvester_base.py#L368

        # https://github.com/GSA/ckanext-datajson/blob/datagov/ckanext/datajson/harvester_base.py#L120

        # https://github.com/GSA/ckanext-datajson/blob/datagov/ckanext/datajson/harvester_base.py#L137
        schema_value = self.data_json.get('conformsTo', '')
        errors_count = 0
        if schema_value not in self.schema.valid_schemas.keys():
            errors_count += 1
            self.errors.append(f'Error reading json schema value. "{schema_value}" is not known schema')
        self.schema_version = self.schema.valid_schemas.get(schema_value, '1.0')

        # list of needed catalog values  # https://github.com/GSA/ckanext-datajson/blob/datagov/ckanext/datajson/harvester_base.py#L152
        catalog_fields = ['@context', '@id', 'conformsTo', 'describedBy']
        self.catalog_extras = dict(('catalog_'+k, v) for (k, v) in self.data_json.items() if k in catalog_fields)

        return errors_count == 0

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

    def __init__(self):
        self.validation_errors = []
        self.omb_burueau_codes = set()
        # Constant URL is safe from protocol scheme abuse (bandit B310)
        self.ftpstream = urllib.request.urlopen(BUREAU_CODE_URL) #nosec
        self.csvfile = csv.DictReader(codecs.iterdecode(self.ftpstream, 'utf-8'))
        for row in self.csvfile:
            self.omb_burueau_codes.add(row["Agency Code"] + ":" + row["Bureau Code"])

    def validate_dataset(self, item):
        errors_array = []
        errs = {}
        # Required
        dataset_identifier = "%s" % item.get("identifier", "").strip()

        # title
        if self.check_required_string_field(item, "title", 2, dataset_identifier, errs):
                dataset_identifier = "%s" % item.get("title", "").strip()

        # accessLevel # required
        if self.check_required_string_field(item, "accessLevel", 3, dataset_identifier, errs):
            if item["accessLevel"] not in ("public", "restricted public", "non-public"):
                self.add_error(errs, 5, "Invalid Required Field Value",
                               "The field 'accessLevel' had an invalid value: \"%s\"" % item[
                                   "accessLevel"],
                               dataset_identifier)

        # bureauCode # required
        if not self.is_redacted(item.get('bureauCode')):
            if self.check_required_field(item, "bureauCode", list, dataset_identifier, errs):
                for bc in item["bureauCode"]:
                    if not isinstance(bc, str):
                        self.add_error(errs, 5, "Invalid Required Field Value", "Each bureauCode must be a string",
                                       dataset_identifier)
                    elif ":" not in bc:
                        self.add_error(errs, 5, "Invalid Required Field Value",
                                       "The bureau code \"%s\" is invalid. "
                                       "Start with the agency code, then a colon, then the bureau code." % bc,
                                       dataset_identifier)
                    elif bc not in self.omb_burueau_codes:
                        self.add_error(errs, 5, "Invalid Required Field Value",
                                       "The bureau code \"%s\" was not found in our list %s"
                                       % (bc, BUREAU_CODE_URL),
                                       dataset_identifier)

        # contactPoint # required
        if self.check_required_field(item, "contactPoint", dict, dataset_identifier, errs):
            cp = item["contactPoint"]
            # contactPoint - fn # required
            self.check_required_string_field(cp, "fn", 1, dataset_identifier, errs)

            # contactPoint - hasEmail # required
            if self.check_required_string_field(cp, "hasEmail", 9, dataset_identifier, errs):
                if not self.is_redacted(cp.get('hasEmail')):
                    email = cp["hasEmail"].replace('mailto:', '')
                    if not validate_email(email):
                        self.add_error(errs, 5, "Invalid Required Field Value",
                                       "The email address \"%s\" is not a valid email address." % email,
                                       dataset_identifier)

        # description # required
        self.check_required_string_field(
            item, "description", 1, dataset_identifier, errs)

        # identifier #required
        self.check_required_string_field(
            item, "identifier", 1, dataset_identifier, errs)

        # keyword # required
        if isinstance(item.get("keyword"), str):
            if not self.is_redacted(item.get("keyword")):
                self.add_error(errs, 5, "Update Your File!",
                               "The keyword field used to be a string but now it must be an array.", dataset_identifier)
        elif self.check_required_field(item, "keyword", list, dataset_identifier, errs):
            for kw in item["keyword"]:
                if not isinstance(kw, str):
                    self.add_error(errs, 5, "Invalid Required Field Value",
                                   "Each keyword in the keyword array must be a string", dataset_identifier)
                elif len(kw.strip()) == 0:
                    self.add_error(errs, 5, "Invalid Required Field Value",
                                   "A keyword in the keyword array was an empty string.", dataset_identifier)

        # modified # required
        if self.check_required_string_field(item, "modified", 1, dataset_identifier, errs):
            if not self.is_redacted(item['modified']) \
                    and not MODIFIED_REGEX_1.match(item['modified']) \
                    and not MODIFIED_REGEX_2.match(item['modified']) \
                    and not MODIFIED_REGEX_3.match(item['modified']):
                self.add_error(errs, 5, "Invalid Required Field Value",
                               "The field \"modified\" is not in valid format: \"%s\"" % item['modified'], dataset_identifier)

        # programCode # required
        if not self.is_redacted(item.get('programCode')):
            if self.check_required_field(item, "programCode", list, dataset_identifier, errs):
                for pc in item["programCode"]:
                    if not isinstance(pc, str):
                        self.add_error(errs, 5, "Invalid Required Field Value",
                                       "Each programCode in the programCode array must be a string", dataset_identifier)
                    elif not PROGRAM_CODE_REGEX.match(pc):
                        self.add_error(errs, 50, "Invalid Field Value (Optional Fields)",
                                       "One of programCodes is not in valid format (ex. 018:001): \"%s\"" % pc,
                                       dataset_identifier)

        # publisher # required
        if self.check_required_field(item, "publisher", dict, dataset_identifier, errs):
            # publisher - name # required
            self.check_required_string_field(
                item["publisher"], "name", 1, dataset_identifier, errs)

        # Required-If-Applicable

        # dataQuality # Required-If-Applicable
        if item.get("dataQuality") is None or self.is_redacted(item.get("dataQuality")):
            pass  # not required or REDACTED
        elif not isinstance(item["dataQuality"], bool):
            self.add_error(errs, 50, "Invalid Field Value (Optional Fields)",
                           "The field 'dataQuality' must be true or false, "
                           "as a JSON boolean literal (not the string \"true\" or \"false\").",
                           dataset_identifier)

        # distribution # Required-If-Applicable
        if item.get("distribution") is None:
            pass  # not required
        elif not isinstance(item["distribution"], list):
            if isinstance(item["distribution"], str) and self.is_redacted(item.get("distribution")):
                pass
            else:
                self.add_error(errs, 50, "Invalid Field Value (Optional Fields)",
                               "The field 'distribution' must be an array, if present.", dataset_identifier)
        else:
            for j, dt in enumerate(item["distribution"]):
                if isinstance(dt, str):
                    if self.is_redacted(dt):
                        continue
                distribution_name = dataset_identifier + \
                    (" distribution %d" % (j + 1))
                # distribution - downloadURL # Required-If-Applicable
                self.check_url_field(
                    False, dt, "downloadURL", distribution_name, errs, allow_redacted=True)

                # distribution - mediaType # Required-If-Applicable
                if 'downloadURL' in dt:
                    if self.check_required_string_field(dt, "mediaType", 1, distribution_name, errs):
                        if not IANA_MIME_REGEX.match(dt["mediaType"]) \
                                and not self.is_redacted(dt["mediaType"]):
                            self.add_error(errs, 5, "Invalid Field Value",
                                           "The distribution mediaType \"%s\" is invalid. "
                                           "It must be in IANA MIME format." % dt["mediaType"],
                                           distribution_name)

                # distribution - accessURL # optional
                self.check_url_field(
                    False, dt, "accessURL", distribution_name, errs, allow_redacted=True)

                # distribution - conformsTo # optional
                self.check_url_field(
                    False, dt, "conformsTo", distribution_name, errs, allow_redacted=True)

                # distribution - describedBy # optional
                self.check_url_field(
                    False, dt, "describedBy", distribution_name, errs, allow_redacted=True)

                # distribution - describedByType # optional
                if dt.get("describedByType") is None or self.is_redacted(dt.get("describedByType")):
                    pass  # not required or REDACTED
                elif not IANA_MIME_REGEX.match(dt["describedByType"]):
                    self.add_error(errs, 5, "Invalid Field Value",
                                   "The describedByType \"%s\" is invalid. "
                                   "It must be in IANA MIME format." % dt["describedByType"],
                                   distribution_name)

                # distribution - description # optional
                if dt.get("description") is not None:
                    self.check_required_string_field(
                        dt, "description", 1, distribution_name, errs)

                # distribution - format # optional
                if dt.get("format") is not None:
                    self.check_required_string_field(
                        dt, "format", 1, distribution_name, errs)

                # distribution - title # optional
                if dt.get("title") is not None:
                    self.check_required_string_field(
                        dt, "title", 1, distribution_name, errs)

        # license # Required-If-Applicable
        self.check_url_field(False, item, "license",
                             dataset_identifier, errs, allow_redacted=True)

        # rights # Required-If-Applicable
        # TODO move to warnings
        # if item.get("accessLevel") != "public":
        # check_string_field(item, "rights", 1, dataset_identifier, errs)

        # spatial # Required-If-Applicable
        # TODO: There are more requirements than it be a string.
        if item.get("spatial") is not None and not isinstance(item.get("spatial"), str):
            self.add_error(errs, 50, "Invalid Field Value (Optional Fields)",
                           "The field 'spatial' must be a string value if specified.", dataset_identifier)

        # temporal # Required-If-Applicable
        if item.get("temporal") is None or self.is_redacted(item.get("temporal")):
            pass  # not required or REDACTED
        elif not isinstance(item["temporal"], str):
            self.add_error(errs, 10, "Invalid Field Value (Optional Fields)",
                           "The field 'temporal' must be a string value if specified.", dataset_identifier)
        elif "/" not in item["temporal"]:
            self.add_error(errs, 10, "Invalid Field Value (Optional Fields)",
                           "The field 'temporal' must be two dates separated by a forward slash.", dataset_identifier)
        elif not TEMPORAL_REGEX_1.match(item['temporal']) \
                and not TEMPORAL_REGEX_2.match(item['temporal']) \
                and not TEMPORAL_REGEX_3.match(item['temporal']):
            self.add_error(errs, 50, "Invalid Field Value (Optional Fields)",
                           "The field 'temporal' has an invalid start or end date.", dataset_identifier)

        # Expanded Fields

        # accrualPeriodicity # optional
        if item.get("accrualPeriodicity") not in ACCRUAL_PERIODICITY_VALUES \
                and not self.is_redacted(item.get("accrualPeriodicity")):
            self.add_error(errs, 50, "Invalid Field Value (Optional Fields)",
                           "The field 'accrualPeriodicity' had an invalid value.", dataset_identifier)

        # conformsTo # optional
        self.check_url_field(False, item, "conformsTo",
                             dataset_identifier, errs, allow_redacted=True)

        # describedBy # optional
        self.check_url_field(False, item, "describedBy",
                             dataset_identifier, errs, allow_redacted=True)

        # describedByType # optional
        if item.get("describedByType") is None or self.is_redacted(item.get("describedByType")):
            pass  # not required or REDACTED
        elif not IANA_MIME_REGEX.match(item["describedByType"]):
            self.add_error(errs, 5, "Invalid Field Value",
                           "The describedByType \"%s\" is invalid. "
                           "It must be in IANA MIME format." % item["describedByType"],
                           dataset_identifier)

        # isPartOf # optional
        if item.get("isPartOf"):
            self.check_required_string_field(
                item, "isPartOf", 1, dataset_identifier, errs)

        # issued # optional
        if item.get("issued") is not None and not self.is_redacted(item.get("issued")):
            if not ISSUED_REGEX.match(item['issued']):
                self.add_error(errs, 50, "Invalid Field Value (Optional Fields)",
                               "The field 'issued' is not in a valid format.", dataset_identifier)

        # landingPage # optional
        self.check_url_field(False, item, "landingPage",
                             dataset_identifier, errs, allow_redacted=True)

        # language # optional
        if item.get("language") is None or self.is_redacted(item.get("language")):
            pass  # not required or REDACTED
        elif not isinstance(item["language"], list):
            self.add_error(errs, 50, "Invalid Field Value (Optional Fields)",
                           "The field 'language' must be an array, if present.", dataset_identifier)
        else:
            for s in item["language"]:
                if not LANGUAGE_REGEX.match(s) and not self.is_redacted(s):
                    self.add_error(errs, 50, "Invalid Field Value (Optional Fields)",
                                   "The field 'language' had an invalid language: \"%s\"" % s, dataset_identifier)

        # PrimaryITInvestmentUII # optional
        if item.get("PrimaryITInvestmentUII") is None or self.is_redacted(item.get("PrimaryITInvestmentUII")):
            pass  # not required or REDACTED
        elif not PRIMARY_IT_INVESTMENT_UII_REGEX.match(item["PrimaryITInvestmentUII"]):
            self.add_error(errs, 50, "Invalid Field Value (Optional Fields)",
                           "The field 'PrimaryITInvestmentUII' must be a string "
                           "in 023-000000001 format, if present.", dataset_identifier)

        # references # optional
        if item.get("references") is None:
            pass  # not required or REDACTED
        elif not isinstance(item["references"], list):
            if isinstance(item["references"], str) and self.is_redacted(item.get("references")):
                pass
            else:
                self.add_error(errs, 50, "Invalid Field Value (Optional Fields)",
                               "The field 'references' must be an array, if present.", dataset_identifier)
        else:
            for s in item["references"]:
                if not rfc3987_url.match(s) and not self.is_redacted(s):
                    self.add_error(errs, 50, "Invalid Field Value (Optional Fields)",
                                   "The field 'references' had an invalid rfc3987 URL: \"%s\"" % s, dataset_identifier)

        # systemOfRecords # optional
        self.check_url_field(False, item, "systemOfRecords",
                             dataset_identifier, errs, allow_redacted=True)

        # theme #optional
        if item.get("theme") is None or self.is_redacted(item.get("theme")):
            pass  # not required or REDACTED
        elif not isinstance(item["theme"], list):
            self.add_error(errs, 50, "Invalid Field Value (Optional Fields)", "The field 'theme' must be an array.",
                           dataset_identifier)
        else:
            for s in item["theme"]:
                if not isinstance(s, str):
                    self.add_error(errs, 50, "Invalid Field Value (Optional Fields)",
                                   "Each value in the theme array must be a string", dataset_identifier)
                elif len(s.strip()) == 0:
                    self.add_error(errs, 50, "Invalid Field Value (Optional Fields)",
                                   "A value in the theme array was an empty string.", dataset_identifier)

        # Form the output data.
        for err_type in sorted(errs):
            errors_array.append({err_type[1]: [err_item + (" (%d locations)" % len(errs[err_type][err_item]) if len(errs[err_type][err_item]) else "")
                  for err_item in sorted(errs[err_type], key=lambda x: (-len(errs[err_type][x]), x))]})
        return errors_array

    def check_required_field(self, obj, field_name, data_type, dataset_identifier, errs):
        # checks that a field exists and has the right type
        if field_name not in obj:
            self.add_error(errs, 10, "Missing Required Fields",
                           "The '%s' field is missing." % field_name, dataset_identifier)
            return False
        elif obj[field_name] is None:
            self.add_error(errs, 10, "Missing Required Fields",
                           "The '%s' field is empty." % field_name, dataset_identifier)
            return False
        elif not isinstance(obj[field_name], data_type):
            self.add_error(errs, 5, "Invalid Required Field Value",
                           "The '%s' field must be a %s but it has a different datatype (%s)." % (
                               field_name, self.nice_type_name(data_type), self.nice_type_name(type(obj[field_name]))), dataset_identifier)
            return False
        elif isinstance(obj[field_name], list) and len(obj[field_name]) == 0:
            self.add_error(errs, 10, "Missing Required Fields",
                           "The '%s' field is an empty array." % field_name, dataset_identifier)
            return False
        return True

    def check_required_string_field(self, obj, field_name, min_length, dataset_identifier, errs):
        # checks that a required field exists, is typed as a string, and has a minimum length
        if not self.check_required_field(obj, field_name, str, dataset_identifier, errs):
            return False
        elif len(obj[field_name].strip()) == 0:
            self.add_error(errs, 10, "Missing Required Fields", "The '%s' field is present but empty." % field_name,
                           dataset_identifier)
            return False
        elif len(obj[field_name].strip()) < min_length:
            self.add_error(errs, 100, "Invalid Field Value",
                           "The '%s' field is very short (min. %d): \"%s\"" % (
                               field_name, min_length, obj[field_name]),
                           dataset_identifier)
            return False
        return True

    def check_url_field(self, required, obj, field_name, dataset_identifier, errs, allow_redacted=False):
        # checks that a required or optional field, if specified, looks like a URL
        if not required and (field_name not in obj or obj[field_name] is None):
            return True  # not required, so OK
        if not self.check_required_field(obj, field_name, str, dataset_identifier,
                                         errs):
            return False  # just checking data type
        if allow_redacted and self.is_redacted(obj[field_name]):
            return True
        if not rfc3987_url.match(obj[field_name]):
            self.add_error(errs, 5, "Invalid Required Field Value",
                           "The '%s' field has an invalid rfc3987 URL: \"%s\"." % (field_name, obj[field_name]), dataset_identifier)
            return False
        return True

    @staticmethod
    def add_error(errs, severity, heading, description, context=None):
        heading = "%s: %s" % (context, heading)
        s = errs.setdefault((severity, heading), {}
                            ).setdefault(description, set())
        if context:
            s.add(context)

    @staticmethod
    def is_redacted(field):
        if isinstance(field, str) and REDACTED_REGEX.match(field):
            return True
        return False

    @staticmethod
    def nice_type_name(data_type):
        if data_type == str:
            return "string"
        elif data_type == list:
            return "array"
        else:
            return str(data_type)
