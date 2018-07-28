import argparse
import os
import logging.config

from alblogs import config, logsdb, statsdb

CONFIGURATION = None
LOGGER = None


def _find_config_path(options):
    if options.config:
        config_path = options.config
    else:
        config_path = os.environ.get('ALBLOGS_CONFIG_PATH')
        if config_path is None:
            config_path = os.getcwd()

    if config_path is None:
        raise RuntimeError("Unable to determine configuration path")

    if not os.path.exists(config_path):
        raise RuntimeError("Config path '{0}' does not exist".format(config_path))

    config_file = os.path.join(config_path, "application.config")
    if not os.path.exists(config_file):
        raise RuntimeError("Config file '{0}' does not exist".format(config_file))

    return config_path


def _init_logging(config_path):
    log_config = os.path.join(config_path, "logging.config")
    if os.path.exists(log_config):
        logging.config.fileConfig(log_config)

        global LOGGER
        LOGGER = logging.getLogger(__name__)
        LOGGER.info("Logging initialized from {0}".format(log_config))

    else:
        print("NO LOGGING CONFIGURATION FOUND AT {0}, LOGGING IS NOT INITIALIZED".format(log_config))


def _init_configuration(config_path):
    global CONFIGURATION
    CONFIGURATION = config.Configuration(config_path)


def get_default_argparser():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", action='store', help="Full path to the configuration directory")
    return parser


def initialize(options):
    config_path = _find_config_path(options)
    _init_logging(config_path)
    _init_configuration(config_path)


def get_configuration():
    global CONFIGURATION
    return CONFIGURATION


def open_logsdb(filename, create=False):
    db = logsdb.Database(filename, create)
    db.open()
    return db


def open_statsdb(filename, create=False):
    db = statsdb.Database(filename, create)
    db.open()
    return db






