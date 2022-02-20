# coding=utf-8
import json
import time
import Util
import Config
import logging
from sql_helper import *
from time_helper import *
from datetime import timedelta
from aria2_helper import Aria2


def main(config):
    # 初始化logger
    logger = Util.mix_logger("main", logging.DEBUG, logging.DEBUG)

    # 连接数据库
    mysql_conn = Util.mysql_conn(config, "GHA_MYSQL")
    redis_conn = Util.redis_conn(config, "GHA_REDIS")

    # 更新Redis记录
    redis_conn.flushdb()
    exist_time_tick = get_app_pair(mysql_conn, "GHA", "aria2_downloaded")
    if exist_time_tick is not None:
        exist_time_tick = set(json.loads(exist_time_tick))
        if len(exist_time_tick) != 0:
            redis_conn.sadd("GHA_aria2_done", *exist_time_tick)

    # 计算所有需要下载的时间片
    time_tick_count = 0
    time_tick = str2datetime(config["PLAN"]["start_time"])
    stop_time = str2datetime(config["PLAN"]["stop_time"])
    while time_tick < stop_time:
        time_tick_count += 1
        time_tick_str = fmt_time_tick(time_tick)
        if not redis_conn.sismember("GHA_aria2_done", time_tick_str):
            redis_conn.sadd("GHA_aria2_queue", time_tick_str)
        time_tick += timedelta(hours=1)
    logger.info("共计{}/{}个时间片需要下载".format(redis_conn.scard("GHA_aria2_queue"), time_tick_count))

    # 连接到Aria2
    aria2 = Aria2(**config["ARIA2"]["aria2"])
    aria2.set_default_attr("gid", "status", "files", "errorCode")

    # 逐步消化下载任务
    while any([redis_conn.scard("GHA_aria2_queue") != 0, redis_conn.scard("GHA_aria2_task") != 0]):
        # 回收已完成的下载任务
        finish_list = aria2.tellStopped()
        for task in finish_list:
            time_tick = task["files"][0]["uris"][0]["uri"].split("/")[-1].split(".")[0]
            if task["status"] != "complete":
                logger.warning("时间片<{}>下载失败({})".format(time_tick, task["errorCode"]))
            else:
                logger.info("时间片<{}>下载完成".format(time_tick))
                redis_conn.sadd("GHA_aria2_done", time_tick)  # 加入解压集合
            redis_conn.srem("GHA_aria2_task", time_tick)
            aria2.removeDownloadResult(task["gid"])

        # 记录已下载数据
        exist_time_tick = list(map(lambda x: x.decode(), list(redis_conn.smembers("GHA_aria2_done"))))
        set_app_pair(mysql_conn, "GHA", "aria2_downloaded", json.dumps(exist_time_tick))

        # 控制下游流程队列深度
        if redis_conn.scard("GHA_aria2_queue") == 0:
            continue

        if redis_conn.scard("GHA_aria2_task") < config["ARIA2"]["aria2_depth"]:
            time_tick = redis_conn.spop("GHA_aria2_queue").decode()
            redis_conn.sadd("GHA_aria2_task", time_tick)
            aria2.addUri(["https://data.gharchive.org/{}.json.gz".format(time_tick)])
            logger.info("新增下载任务<{}>".format(time_tick))

        time.sleep(1)

    logger.warning("程序退出")


if __name__ == '__main__':
    _app_name = __file__.split("/")[-2]
    _config = Config.get_config(_app_name)
    main(_config)
