# coding=utf-8
import pymysql
from dbutils.pooled_db import PooledDB
from redis import ConnectionPool, StrictRedis


def mysql_conn(config, db_key):
    config[db_key]['port'] = int(config[db_key]['port'])
    conn = pymysql.connect(**config[db_key])
    return conn


def mysql_pool(config, db_key):
    for key in config["MYSQL_POOL"]:
        config["MYSQL_POOL"][key] = int(config["MYSQL_POOL"][key])
    config[db_key]['port'] = int(config[db_key]['port'])
    pool = PooledDB(creator=pymysql, **config[db_key], **config["MYSQL_POOL"])
    return pool


def redis_conn(config, db_key):
    return StrictRedis(**config[db_key])


def redis_pool(config, db_key):
    return ConnectionPool(**config[db_key])
