import logging.config

logging_config = {
    "version": 1,
    "formatters": {
        "default": {
            "format": "%(asctime)s %(levelname)s: %(message)s",
            "datefmt": "%Y/%m/%d %H:%M:%S",
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "default",
            "level": "INFO",
            "stream": "ext://sys.stdout",
        }
    },
    "loggers": {
        "": {"handlers": ["console"], "level": "INFO", "propagate": True},
        "glue": {"handlers": ["console"], "level": "INFO", "propagate": False},
    },
}

logging.config.dictConfig(logging_config)
