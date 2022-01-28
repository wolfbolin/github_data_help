# coding=utf-8
import gc
import gzip
import json
import time
import Util
import Config
import pymysql
import logging
import platform
import requests
from io import BytesIO
from requests.adapters import HTTPAdapter
from redis import ConnectionPool, StrictRedis
from datetime import datetime, timedelta, timezone


def main(config: dict):
    # 初始化logger
    logger = Util.mix_logger("main", logging.DEBUG, logging.DEBUG)

    # 连接数据库
    mysql_pool = Util.mysql_pool(config, "MYSQL")
    redis_pool = ConnectionPool(**config["REDIS"])
    redis = StrictRedis(connection_pool=redis_pool)

    # 读取已下载的时间片
    if not redis.exists("GHA_exist_time_tick"):
        conn = mysql_pool.connection()
        exist_time_tick = get_app_pair(conn, "GHA", "exist_time_tick")
        if exist_time_tick is None:
            exist_time_tick = []
        else:
            exist_time_tick = json.loads(exist_time_tick)
        if len(exist_time_tick) != 0:
            redis.sadd("GHA_exist_time_tick", *exist_time_tick)
        conn.close()

    time_tick = datetime.strptime(config["PLAN"]["start_time"], "%Y-%m-%d-%H")
    stop_time = datetime.strptime(config["PLAN"]["stop_time"], "%Y-%m-%d-%H")
    while time_tick < stop_time:
        time_tick_str = time_tick2str(time_tick)
        logger.info("Time tick: {}".format(time_tick_str))
        if not redis.sismember("GHA_exist_time_tick", time_tick_str):
            try:
                # 针对某小时的事件数据，下载，解压，分割
                logger.info("Downloading data @{}".format(time_tick_str))
                event_data = download_archive_data(time_tick_str)
                Util.turbo_multiprocess(config, fetch_tick_data, time_tick_str, event_data)
                gc.collect()
            except Exception as e:
                logger.error("[RE] Data time: {}".format(time_tick_str))
                raise e
            # 记录下载的时间片
            redis.sadd("GHA_exist_time_tick", time_tick_str)
        else:
            logger.info("Time tick {} already done".format(time_tick_str))
        # 切换到下一个时间片
        time_tick += timedelta(hours=1)

    # 任务完成
    # 持久化已下载时间片
    conn = mysql_pool.connection()
    exist_time_tick = list(map(lambda x: x.decode(), list(redis.smembers("GHA_exist_time_tick"))))
    set_app_pair(conn, "GHA", "exist_time_tick", json.dumps(exist_time_tick))
    conn.close()


def fetch_tick_data(config: dict, logger: logging.Logger, db_pool: dict, common_data, function_data):
    time_tick = common_data
    json_data = function_data

    if len(json_data.strip()) == 0:
        return
    event = json.loads(json_data)
    # 写入单行数据
    write_event_data(config, db_pool["mysql"].connection(), time_tick, event)


"""
数据处理相关函数封装
"""


def download_archive_data(time_tick) -> list:
    url = "https://data.gharchive.org/{}.json.gz".format(time_tick)
    session = requests.Session()
    session.mount('https://', HTTPAdapter(max_retries=3))
    http_data = session.get(url, timeout=60)
    gzip_data = BytesIO(http_data.content)
    gzip_data = gzip.GzipFile(mode="rb", fileobj=gzip_data).read()
    text_data = gzip_data.decode().split("\n")
    return text_data


def write_event_data(config: dict, conn: pymysql.Connection, time_tick: str, data: dict):
    cursor = conn.cursor()
    sql_data = {
        "id": data["id"],
        "type": data["type"],
        "user_id": data["actor"]["id"],
        "user_name": data["actor"]["login"],
        "user_data": json.dumps(data["actor"], ensure_ascii=False),
        "repo_id": data["repo"]["id"],
        "repo_name": data["repo"]["name"],
        "repo_data": json.dumps(data["repo"], ensure_ascii=False),
        "payload": json.dumps(data["payload"], ensure_ascii=False),
        "time_tick": time_tick,
        "created_at": utc_time2local_time(data["created_at"], config["FMT"]["date"]),
    }
    # print(cursor.mogrify(mysql_replace_sql("GA_Archive", sql_data), args=list(sql_data.values())))
    cursor.execute(mysql_replace_sql("GA_Archive", sql_data), args=list(sql_data.values()))
    conn.commit()


"""
时间处理相关函数封装
"""


def time_tick2str(tick_now):
    system = platform.system().lower()
    if system == "linux":
        tick_now = tick_now.strftime("%Y-%m-%d-%-H")  # Linux
    elif system == "windows":
        tick_now = tick_now.strftime("%Y-%m-%d-%#H")  # Windows
    else:
        tick_now = "{dt.year}-{dt.month:02d}-{dt.day:02d}-{dt.hour}".format(dt=tick_now)
    return tick_now


def utc_time2local_time(text: str, fmt: str) -> datetime:
    time_data = datetime.strptime(text, fmt)
    time_data = time_data.replace(tzinfo=timezone.utc)
    time_data = time_data.astimezone()
    return time_data


"""
数据库操作相关函数封装
"""


def set_app_pair(conn, app, key, val):
    with conn.cursor() as cursor:
        sql = "INSERT INTO `kvdb` (`app`,`key`,`val`) VALUES (%s,%s,%s)" \
              "ON DUPLICATE KEY UPDATE `val`=VALUES(`val`)"
        cursor.execute(query=sql, args=[app, key, val])
        conn.commit()
        return cursor.rowcount


def get_app_pair(conn, app, key):
    with conn.cursor() as cursor:
        sql = "SELECT `key`, `val` FROM `kvdb` WHERE `app`=%s AND `key`=%s"
        cursor.execute(query=sql, args=[app, key])
        item = cursor.fetchone()
        if item and key == item[0]:
            return item[1]
        else:
            return None


def mysql_replace_sql(table, data):
    sql = "REPLACE INTO `{}`".format(table)
    sql += "(`{}`)".format("`,`".join(data.keys()))
    sql += "VALUES({})".format(("%s," * len(data.items()))[:-1])
    return sql


if __name__ == '__main__':
    _config = Config.get_config()
    main(_config)
