import configparser


def get_config():
    """
    Reads the configuration data from the 'config.ini' file and returns a dictionary.
    """
    config = configparser.ConfigParser()
    config.read("config.ini")
    return dict(config["DEFAULT"])
