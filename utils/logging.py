import logging
import logging.config
import yaml

with open('logging.yaml', 'r') as f:
    config = yaml.safe_load(f.read())
    logging.config.dictConfig(config)


def get_logger(logger_name:str="compare"):
    LOGGER = logging.getLogger(logger_name)
    return LOGGER

