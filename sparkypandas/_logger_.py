# copied and slightly modified the code from:
# https://stackoverflow.com/questions/40806225/pyspark-logging-from-the-executor
import logging
import logging.config
import os
import tempfile


class _Unique(logging.Filter):
    """Messages are allowed through just once.
    The 'message' includes substitutions, but is not formatted by the
    handler. If it were, then practically all messages would be unique!
    """

    def __init__(self, name=""):
        logging.Filter.__init__(self, name)
        self.reset()

    def reset(self):
        """Act as if nothing has happened."""
        self.__logged = {}

    def filter(self, rec):
        """logging.Filter.filter performs an extra filter on the name."""
        return logging.Filter.filter(self, rec) and self.__is_first_time(rec)

    def __is_first_time(self, rec):
        """Emit a message only once."""
        msg = rec.msg % (rec.args)
        if msg in self.__logged:
            self.__logged[msg] += 1
            return False
        else:
            self.__logged[msg] = 1
            return True


def init_logger(logfile='pyspark.log'):
    """Replaces getLogger from logging to ensure each worker configures
            logging locally."""
    try:
        logfile = os.path.join(os.environ['LOG_DIRS'].split(',')[0], logfile)
    except (KeyError, IndexError):
        tmpdir = tempfile.gettempdir()
        logfile = os.path.join(tmpdir, logfile)
        root_logger = logging.getLogger("")
        root_logger.addFilter(_Unique())
        root_logger.warning(
            "LOG_DIRS not in environment variables or is empty. Will log to {}."
                .format(logfile))

    # Alternatively, load log settings from YAML or use JSON.
    log_settings = {
        'version': 1,
        'disable_existing_loggers': False,
        'handlers': {
            'file': {
                'class': 'logging.FileHandler',
                'level': 'DEBUG',
                'formatter': 'detailed',
                'filename': logfile
            },
            'default': {
                'level': 'INFO',
                'class': 'logging.StreamHandler',
            },
        },
        'formatters': {
            'detailed': {
                'format': ("%(asctime)s.%(msecs)03d %(levelname)s %(module)s - "
                           "%(funcName)s: %(message)s"),
            },
        },
        'loggers': {
            'driver': {
                'level': 'INFO',
                'handlers': ['file', 'default']
            },
            'executor': {
                'level': 'DEBUG',
                'handlers': ['file', 'default']
            },
        }
    }

    logging.config.dictConfig(log_settings)


init_logger()


def get_driver_logger():
    return logging.getLogger('driver')


def get_executor_logger():
    return logging.getLogger('executor')
