import configparser

def read_config(config_file='config.ini'):
    config = configparser.ConfigParser()
    config.read(config_file)
    return config.get('settings', 'run_analysis', fallback='no')
