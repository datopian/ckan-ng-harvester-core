import re
from harvester_adapters.ckan import settings
from harvesters.logs import logger


def clean_tags(tags):
    ret = []
    pattern = re.compile(r'[^A-Za-z0-9\s_\-!?]+')
    
    for tag in tags:
        tag = pattern.sub('', tag).strip()
        if len(tag) > settings.MAX_TAG_NAME_LENGTH:
            logger.error('tag is long, cutting: {}'.format(tag))
            tag = tag[:settings.MAX_TAG_NAME_LENGTH]
        elif len(tag) < settings.MIN_TAG_NAME_LENGTH:
            logger.error('tag is short: {}'.format(tag))
            tag += '_' * (settings.MIN_TAG_NAME_LENGTH - len(tag))
        if tag != '':
            ret.append(tag.lower().replace(' ', '-'))  # copyin CKAN behaviour
    return ret