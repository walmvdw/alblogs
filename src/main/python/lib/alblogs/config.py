import yaml
import os.path
import re
import logging

EXPAND_RE = re.compile("(.*?)(\$\{.*?\})(.*)")
LOGGER = None


def get_log():
    global LOGGER
    if LOGGER is None:
        LOGGER = logging.getLogger(__name__)
    return LOGGER


class ConfigurationSection(object):
    def __init__(self, parent, section):
        self._parent = parent
        self._section = section

    def get_value(self, key):
        return self._parent.get_value("{0}.{1}".format(self._section, key))


class Configuration(object):
    def __init__(self, path):
        self._path = path
        self._data = None
        self._init()

    def _init(self):
        get_log().info("Initializing configuration for path '{0}'".format(self._path))
        config_file = os.path.join(self._path, "application.config")
        with open(config_file, 'r') as fhandle:
            self._data = yaml.load(fhandle)

    def _expand_value(self, value):
        result = ""
        match = EXPAND_RE.match(value)
        while match:
            get_log().debug("Expand config value; group(1): '{0}'; group(2): '{1}'; group(3): '{2}'".format(match.group(1), match.group(2), match.group(3)))
            result += match.group(1)
            key = match.group(2)
            key_val = self.get_value((key[2:-1]))
            if key_val is not None:
                result += key_val
            value = match.group(3)
            match = EXPAND_RE.match(value)

        result += value

        return result

    def get_value(self, key):
        keys = key.split('.')
        value = self._data
        for enum_key in keys:
            value = value.get(enum_key)
            if value is None:
                get_log().warn("Key {0} not found in configuration data (subkey {1})".format(key, enum_key))
                return None
        value = self._expand_value(value)

        return value

    def get_data_dir(self):
        return self.get_value("directories.data")

    def get_temp_dir(self):
        return self.get_value("directories.temp")

    def get_reports_dir(self):
        return self.get_value("directories.reports")

    def get_srclogs_dir(self):
        return self.get_value("directories.srclogs")

    def get_archive_dir(self):
        return self.get_value("directories.archive")

