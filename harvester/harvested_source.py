from harvester import config
from jinja2 import Template
import os
from harvester.logs import logger
import pkg_resources


class HarvestedSource:
    """
    analyze all data from a particular previously harvested source
    """
    name = None  # source name (and folder name)
    data = None  # data dict
    results = None  # harvest results
    errors = None
    final_results = {}  # all files processed

    def __init__(self, name):
        config.SOURCE_NAME = name
        self.name = name
        data = config.get_report_files()
        self.data = data['data']
        self.results = data['results']
        self.errors = data['errors']

    def process_results(self):

        # analyze results

        actions = {}  # create | delete | update
        validation_errors = []
        action_errors = []
        action_warnings = []
        # print(f'Result: {self.results}')
        if type(self.results) != list:
            logger.error(f'Unexpected results: {self.results}')
            return False

        for result in self.results:

            # print(f'Result: {result}')
            comparison_results = result.get('comparison_results', None)
            if comparison_results is None:
                # this is bad. This source is broken
                return False
            action = comparison_results['action']
            if action not in actions.keys():
                actions[action] = {'total': 0, 'success': 0, 'fails': 0}
            actions[action]['total'] += 1

            if action in ['create', 'update']:  # delete has no new_data
                if len(comparison_results['new_data'].get('validation_errors', [])) > 0:
                    validation_errors += comparison_results['new_data']['validation_errors']

            action_results = comparison_results.get('action_results', {})
            success = action_results.get('success', False)
            if success:
                actions[action]['success'] += 1
            else:
                actions[action]['fails'] += 1

            action_warnings += action_results.get('warnings', [])
            action_errors += action_results.get('errors', [])

        self.final_results['actions'] = actions
        self.final_results['validation_errors'] = validation_errors
        self.final_results['action_warnings'] = action_warnings
        self.final_results['action_errors'] = action_errors

        return True

    def get_json_data(self):
        data = {
            'name': self.name,
            'data': self.data,
            'results': self.results,
            'errors': self.errors,
            'actions': self.final_results.get('actions', {}),
            'validation_errors': self.final_results.get('validation_errors', {}),
            'action_warnings': self.final_results.get('action_warnings', {}),
            'action_errors': self.final_results.get('action_errors', {})
        }
        return data

    def render_template(self, save=True):
        # redenr through harvest-report.html
        context = self.get_json_data()
        # fails (?) template_txt = pkg_resources.resource_string('harvester', 'templates/harvest-report.html')
        # https://stackoverflow.com/questions/6028000/how-to-read-a-static-file-from-inside-a-python-package
        template_path = pkg_resources.resource_filename('harvester', 'templates/harvest-report.html')
        f = open(template_path, 'r')
        template = Template(f.read())
        f.close()
        html = template.render(**context)
        if save:
            report_path = config.get_html_report_path()
            self.save_report(html=html, report_path=report_path)
            logger.info(f'Saved report to {report_path}')

        return html

    def save_report(self, html, report_path):
        f = open(report_path, 'w')
        f.write(html)
        f.close()


class HarvestedSources:
    """
    analyze ALL harvested sources. Iterate and process all
    """
    base_folder = None
    all_data = []  # one row per harvest source
    summary_data = {'harvest_sources_readed': 0,
                    'harvest_sources_failed': 0,
                    'total_datasets': 0,

                    }

    def __init__(self, base_folder=None):
        self.base_folder = config.DATA_FOLDER_PATH if base_folder is None else base_folder

    def process_all(self):

        logger.info(f'Inspecting {self.base_folder} folder')
        for subdir, dirs, files in os.walk(self.base_folder):
            for name in dirs:
                if name == 'harvest_sources':
                    continue
                logger.info(f'Processing {name} folder')
                self.summary_data['harvest_sources_readed'] += 1

                hs = HarvestedSource(name=name)
                ret = hs.process_results()
                if not ret:
                    self.summary_data['harvest_sources_failed'] += 1
                    continue
                hs.render_template(save=True)

                data = hs.get_json_data()
                self.all_data.append(data)

                if type(data['data']) == list:
                    datasets = []
                    logger.error(f'{name}: Data JSON Source is a list. Must be a dict')
                if type(data['data']) == dict:
                    datasets = data['data'].get('dataset', [])
                if len(datasets) == 0:
                    logger.error(f'Source with 0 datasets {name}')
                self.summary_data['total_datasets'] += len(datasets)
                logger.info(' - Total datasets: {}'.format(self.summary_data['total_datasets']))

        harvest_sources_readed = self.summary_data['harvest_sources_readed']
        harvest_sources_failed = self.summary_data['harvest_sources_failed']
        total_datasets = self.summary_data['total_datasets']
        logger.info('''**************
                        Harvest sources readed: {}
                        Harvest sources failed: {}
                        Total datasets: {}'''.format(harvest_sources_readed,
                                                     harvest_sources_failed,
                                                     total_datasets))