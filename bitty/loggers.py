import os
import logging
import logging.handlers


LOG_LEVEL_MAP = {
    'info': logging.INFO,
    'debug': logging.DEBUG,
    'warning': logging.WARNING,
    'warn': logging.WARNING,
    'error': logging.ERROR
}


global_formatter = logging.Formatter(
    '%(asctime)s %(filename)s:%(lineno)d %(name)-12s '
    '%(levelname)-8s %(message)s',
    datefmt='%m-%d %H:%M:%S'
)


def get_levels(verbosity):
    log_level = logging.ERROR - (verbosity * 10)
    if log_level < logging.DEBUG:
        log_level = logging.DEBUG

    import_log_level = logging.CRITICAL - (verbosity * 10)
    if import_log_level < logging.DEBUG:
        import_log_level = logging.DEBUG

    return log_level, import_log_level


def get_env_levels():
    log_level = os.getenv('LOG_LEVEL', 'warning')
    log_level = LOG_LEVEL_MAP.get(log_level.lower(), logging.WARNING)
    log_level = int((logging.ERROR - log_level) / 10)
    return get_levels(log_level)


def get_stream_handler(level=None, formatter=None):
    if not level:
        level = logging.DEBUG
    if not formatter:
        formatter = global_formatter

    handler = logging.StreamHandler()

    handler.setFormatter(formatter)
    handler.setLevel(level)
    return handler


def configure(logger, level, formatter=None, address=None):
    s_handler = get_stream_handler(level=level, formatter=formatter)
    logger.addHandler(s_handler)
    logger.setLevel(level)


def setup():
    bitty_logger = logging.getLogger('bitty')
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.WARNING)
    bitty_level, _ = get_env_levels()
    configure(bitty_logger, bitty_level)
