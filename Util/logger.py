# coding=utf-8
import os
import logging
from colorlog import ColoredFormatter
from logging.handlers import RotatingFileHandler


def mix_logger(logger_name, console_level=logging.DEBUG, file_level=None):
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    date_format = '%H:%M:%S'

    # StreamHandler logs to file
    if file_level is not None:
        file_logger = RotatingFileHandler(
            filename=get_log_path(logger_name),
            encoding="utf-8",
            backupCount=8)
        file_logger.setLevel(file_level)
        file_logger.namer = lambda x: get_log_path(logger_name, x.split('.')[-1])
        file_format = logging.Formatter(
            fmt='[%(asctime)s]%(name)5s:%(process)-5d <%(levelname)s> | %(message)s',
            datefmt=date_format)
        file_logger.setFormatter(file_format)
        file_logger.doRollover()
        logger.addHandler(file_logger)

    # StreamHandler logs to console
    console = logging.StreamHandler()
    console.setLevel(console_level)
    console_format = ColoredFormatter(
        fmt='%(log_color)s[%(asctime)s]%(name)5s:%(process)-5d <%(levelname)s> | %(message)s',
        datefmt=date_format)
    console.setFormatter(console_format)
    logger.addHandler(console)

    return logger


def get_log_path(logger_name, index=None):
    log_path = '{}/../log'.format(os.path.split(os.path.abspath(__file__))[0])
    if not os.path.exists(log_path):
        os.makedirs(log_path)
    if index is None:
        log_path += '/{}.log'.format(logger_name)
    else:
        log_path += '/{}.{}.log'.format(logger_name, index)
    return log_path
