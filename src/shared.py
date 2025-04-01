import logging

def config_logger(log_level):
    log_format = '%(asctime)s - %(levelname)s - %(message)s'
    if log_level == 'debug':
        logging.basicConfig(
            level=logging.DEBUG,
            format=log_format)
    elif log_level == 'info':
        logging.basicConfig(
            level=logging.INFO,
            format=log_format)
    else:
        raise Exception ('log_level must be "debug" or "info"')