from .command import DbtCloudRunStatus

import logging
import os
import sys
from ruamel import yaml
import json
from datetime import datetime

def create_logger(name) -> logging.Logger:
    log_level = logging.WARNING
    if os.environ.get('PIPERIDER_LOG_LEVEL') == 'DEBUG':
        log_level = logging.DEBUG
    if os.environ.get('PIPERIDER_LOG_LEVEL') == 'INFO':
        log_level = logging.INFO

    log = logging.getLogger(name)
    log.setLevel(log_level)
    handler = logging.StreamHandler(sys.stderr)
    handler.setLevel(log_level)
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    log.addHandler(handler)
    return log

def get_version():
    version_file = os.path.normpath(os.path.join(os.path.dirname(__file__), 'VERSION'))
    with open(version_file) as fh:
        version = fh.read().strip()
        return version

__version__ = get_version()


def safe_load_yaml(file_path):
    try:
        with open(file_path, 'r') as f:
            payload = yaml.safe_load(f)
    except yaml.YAMLError as e:
        print(e)
        return None
    except FileNotFoundError:
        return None
    return payload


def write_to_file(data: dict) -> None:
    filepath = os.path.join(os.getcwd(), 'metric.json')

    with open(filepath, 'w') as f:
        f.write(json.dumps(data, separators=(',', ':')))
    
