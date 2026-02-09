# backend/logging_config.py

import logging.config

# This is a standard Python logging dictionary.
# In a real job, this is often loaded from a YAML or JSON file.
LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "standard": {
            "format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S"
        },
        "minimal": {
            "format": "%(message)s"
        }
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "standard",
            "level": "INFO",
            "stream": "ext://sys.stdout"
        }
    },
    "loggers": {
        # 1. Your Application Logs (Keep these visible!)
        "backend": {
            "handlers": ["console"],
            "level": "INFO",
            "propagate": False
        },
        "uvicorn": {
            "handlers": ["console"],
            "level": "INFO",
            "propagate": False
        },
        
        # 2. 🤫 THE SILENCER (Third-Party Libraries)
        # We force these to WARNING so they don't spam the console.
        "azure": {"level": "WARNING"},
        "azure.core.pipeline.policies.http_logging_policy": {"level": "WARNING"},
        "azure.identity": {"level": "WARNING"},
        "urllib3": {"level": "WARNING"},
        "httpcore": {"level": "WARNING"},
    },
    # Root logger acts as a catch-all
    "root": {
        "handlers": ["console"],
        "level": "INFO"
    }
}

def setup_logging():
    """Applies the configuration defined above."""
    logging.config.dictConfig(LOGGING_CONFIG)