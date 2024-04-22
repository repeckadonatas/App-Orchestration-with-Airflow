"""
A customized logger that is used throughout the project
to log the responses of the app. A file is created daily
to store all log messages for that day.
"""
import sys
import logging.handlers
from logging.handlers import RotatingFileHandler

from src.constants import *


def get_console_handler() -> logging.StreamHandler:
    """
    A function to create a console handler object.
    Outputs the information starting at INFO level to a console.
    Formats the output by using a pre-set format.
    :return: returns a console handler object
    """
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)  # level=20
    console_handler.setFormatter(FORMATTER)
    return console_handler


def get_file_handler() -> logging.handlers.RotatingFileHandler:
    """
    Creates a file handler object that stores log outputs.
    :return: returns .logs file
    """
    file_handler = RotatingFileHandler(LOG_FILE)
    file_handler.setFormatter(FORMATTER)
    return file_handler


def app_logger(logger_name: str) -> logging.getLogger():
    """
    A function to create a logger object with set parameters.
    The parameters are inherited from get_console_handler() function.
    Needs to be called in a file from where the exceptions will be logged.
    :param logger_name: Name for a logger.
           If "__name__" is passed, the logger will return the path to the file where the exception has occurred.
    :return: returns logger object
    """
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    logger.addHandler(get_console_handler())
    logger.addHandler(get_file_handler())
    return logger
