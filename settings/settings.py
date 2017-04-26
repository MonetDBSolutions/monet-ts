import logging.handlers
import os

from settings import logconfig

# Make filepaths relative to settings.
path = lambda root,*a: os.path.join(root, *a)
ROOT = os.path.dirname(os.path.abspath(__file__))

# Deployment Configuration

SOLO = 1
PRODUCTION = 2
DEV = 3
STAGING = 4

STAGES = {'SOLO': SOLO, 'PRODUCTION': PRODUCTION, 'DEV': DEV, 'STAGING': STAGING}

if 'DEPLOYMENT_TYPE' in os.environ:
    DEPLOYMENT = STAGES.get(os.environ['DEPLOYMENT_TYPE'].upper(), 'SOLO')
else:
    DEPLOYMENT = SOLO

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

if DEPLOYMENT != PRODUCTION:
    LOG_LEVEL = logging.DEBUG
else:
    LOG_LEVEL = logging.INFO

USE_SYSLOG = DEPLOYMENT != SOLO
SYSLOG_TAG = "boilerplate"
SYSLOG_FACILITY = logging.handlers.SysLogHandler.LOG_LOCAL2

logconfig.initialize_logging(SYSLOG_TAG, SYSLOG_FACILITY, LOGGERS, LOG_LEVEL, USE_SYSLOG)
