# coding=utf-8
import os
import logging
from logging.handlers import RotatingFileHandler


class MyLogFormatter(logging.Formatter):
    def format(self, record):
        record.relativeCreated = record.relativeCreated // 1000
        return super().format(record)


def mix_logger(logger_name, console_level=logging.DEBUG, file_level=None):
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    date_format = '%H:%M:%S'

    # StreamHandler logs to file
    if file_level is not None:
        file_logger = RotatingFileHandler(
            filename=get_log_path(logger_name),
            encoding="utf-8",
            backupCount=64)
        file_logger.setLevel(file_level)
        file_logger.namer = lambda x: get_log_path(logger_name, x.split('.')[-1])
        file_format = MyLogFormatter(
            fmt='%(filename)16s:%(lineno)d @%(asctime)s [%(levelname)8s] > %(message)s',
            datefmt=date_format)
        file_logger.setFormatter(file_format)
        file_logger.doRollover()
        logger.addHandler(file_logger)

    # StreamHandler logs to console
    console = logging.StreamHandler()
    console.setLevel(console_level)
    console_format = MyLogFormatter(
        fmt='%(module)s:%(lineno)d(%(relativeCreated)ds)@%(asctime)s [%(levelname)s] > %(message)s',
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
