import re


def clean_tags(tags):
    ret = []
    pattern = re.compile('[^A-Za-z0-9\s_\-!?]+')
    for tag in tags:
        cleaned = pattern.sub('', tag).strip()
        if cleaned != '':
            ret.append(cleaned)
    return ret