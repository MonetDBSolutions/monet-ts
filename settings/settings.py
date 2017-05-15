import logging.handlers
import os

from settings import logconfig

# Make filepaths relative to settings.
path = lambda root,*a: os.path.join(root, *a)
ROOT = os.path.dirname(os.path.abspath(__file__))

# Deployment Configuration

PRODUCTION = 1
DEVELOPMENT = 2

STAGE_ENV = 'GUARDIAN_DEPLOYMENT'

STAGES = {'PRODUCTION': PRODUCTION, 'DEVELOPMENT': DEVELOPMENT}

if STAGE_ENV in os.environ:
    DEPLOYMENT_STAGE = STAGES.get(os.environ[STAGE_ENV].upper(), 'DEVELOPMENT')
else:
    DEPLOYMENT_STAGE = DEVELOPMENT

# See PEP 391 and logconfig for formatting help.  Each section of LOGGERS
# will get merged into the corresponding section of log_settings.py.
# Handlers and log levels are set up automatically based on LOG_LEVEL and DEBUG
# unless you set them here.  Messages will not propagate through a logger
# unless propagate: True is set.
LOGGERS = {
    'loggers': {
        'boilerplate': {},
    },
}

if DEPLOYMENT_STAGE != PRODUCTION:
    LOG_LEVEL = logging.DEBUG
else:
    LOG_LEVEL = logging.INFO

USE_SYSLOG = DEPLOYMENT_STAGE != DEVELOPMENT

logconfig.initialize_logging("boilerplate", logging.handlers.SysLogHandler.LOG_LOCAL2, LOGGERS, LOG_LEVEL, USE_SYSLOG)
