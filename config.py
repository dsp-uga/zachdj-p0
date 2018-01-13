import json
import os

"""
    The config module exposes an interface for reading configuration variables from the config.json file
"""

configuration = {}
_config_file_exists = os.path.exists("config.json")
if not _config_file_exists:
    print("WARNING: Config file is missing.")
else:
    configuration = json.load(open('config.json', 'r'))


def get(key, default_value=""):
    """
    Returns the configuration variable associated with the given key,
    or default_value if the given key doesn't exist in config.json

    default_value will also be returned if the config file does not exist.

    :param key: the variable to retrieve (case-sensitive)
    :param default_value: the value to return if the key is not found in the configuration file
    :return: the configuration value associated with the given key, or default_value if the key is not found
    """

    value = configuration.get(key)  # returns None if the key DNE

    if value is None:
        return default_value

    return value
