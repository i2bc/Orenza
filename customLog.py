import logging


class ContextLoggerAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        return "[%s] %s" % (self.extra["database"], msg), kwargs


def set_context(logger, database):
    logger = ContextLoggerAdapter(logger, {"database": database})
    return logger


FORMAT = "%(levelname)s - %(message)s - %(asctime)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
logging.basicConfig(format=FORMAT, datefmt=DATE_FORMAT)

# Create the logger
logger = logging.getLogger("orenza")
logger.setLevel(logging.INFO)

def get_logger():
    return logger


# Create file which log our messages
fh_detailed = logging.FileHandler("orenza_detailed.log")
fh_detailed.setLevel(logging.INFO)
fh_detailed.setFormatter(logging.Formatter(fmt=FORMAT, datefmt=DATE_FORMAT))

fh_error = logging.FileHandler("orenza_error.log")
fh_error.setLevel(logging.ERROR)
fh_error.setFormatter(logging.Formatter(fmt=FORMAT, datefmt=DATE_FORMAT))

logger.addHandler(fh_detailed)
logger.addHandler(fh_error)
