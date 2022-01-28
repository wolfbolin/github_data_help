# coding=utf-8
import gc
import gzip
import json
import time
import Common
import Config
import pymysql
import logging
import platform
import requests
from io import BytesIO
from redis import ConnectionPool, StrictRedis
from datetime import datetime, timedelta, timezone

mixLogger = None


def main(config):
    mysql_pool = Common.mysql_pool(config, "MYSQL_BOT")
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
        mixLogger.info("Time tick: {}".format(time_tick_str))
        if not redis.sismember("GHA_exist_time_tick", time_tick_str):
            try:
                conn = mysql_pool.connection()
                do_next_tick(config, conn, time_tick_str)
                conn.close()
                gc.collect()
            except Exception as e:
                mixLogger.error("[RE] Data time: {}".format(time_tick_str))
                raise e
            # 记录下载的时间片
            redis.sadd("GHA_exist_time_tick", time_tick_str)
        # 切换到下一个时间片
        time_tick += timedelta(hours=1)

    # 任务完成
    # 持久化已下载时间片
    conn = mysql_pool.connection()
    exist_time_tick = list(map(lambda x: x.decode(), list(redis.smembers("GHA_exist_time_tick"))))
    set_app_pair(conn, "GHA", "exist_time_tick", json.dumps(exist_time_tick))
    conn.close()


def do_next_tick(config, conn, time_tick):
    # 针对某小时的事件数据，下载，解压，分割
    mixLogger.info("Downloading data @{}".format(time_tick))
    event_data = download_archive_data(time_tick)
    # 逐行读取数据并理解数据
    for line, json_data in enumerate(event_data):
        try:
            # 避免多余的空行
            json_data = json_data.strip()
            if len(json_data) == 0:
                continue
            # 解析每行的Json数据
            event = json.loads(json_data)
            # 输出日志信息
            msg = "<ID:{: ^16};Type:{: ^32}>".format(event["id"], event["type"])
            msg = "Data line {}: {} @ {} UTC".format(line + 1, msg, event["created_at"])
            mixLogger.info(msg)
            # 写入单行数据
            write_event_data(config, conn, time_tick, event)
        except ValueError as e:
            mixLogger.error("[RE] File line {}: {}".format(line + 1, json_data))
            raise e


def download_archive_data(time_tick) -> list:
    url = "https://data.gharchive.org/{}.json.gz".format(time_tick)
    http_data = requests.get(url)
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


def mysql_replace_sql(table, data):
    sql = "REPLACE INTO `{}`".format(table)
    sql += "(`{}`)".format("`,`".join(data.keys()))
    sql += "VALUES({})".format(("%s," * len(data.items()))[:-1])
    return sql


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


def utc_time2local_time(text: str, fmt: str) -> datetime:
    time_data = datetime.strptime(text, fmt)
    time_data = time_data.replace(tzinfo=timezone.utc)
    time_data = time_data.astimezone()
    return time_data


def time_tick2str(tick_now):
    system = platform.system().lower()
    if system == "linux":
        tick_now = tick_now.strftime("%Y-%m-%d-%-H")  # Linux
    elif system == "windows":
        tick_now = tick_now.strftime("%Y-%m-%d-%#H")  # Windows
    else:
        tick_now = "{dt.year}-{dt.month:02d}-{dt.day:02d}-{dt.hour}".format(dt=tick_now)
    return tick_now


if __name__ == '__main__':
    mixLogger = Common.mix_logger("main", logging.DEBUG, logging.DEBUG)
    _config = Config.get_config()
    main(_config)
