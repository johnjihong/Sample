"""Logging configuration."""

import logging

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)-15s %(threadName)-12s %(name)-5s %(levelname)-8s %(message)s',
    datefmt='%b-%d %H:%M:%S'
)

# Name the logger after the packages.
logger = logging.getLogger(__package__)
