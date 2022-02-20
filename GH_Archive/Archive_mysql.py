# coding=utf-8
import os
import gzip
import json
import time
import Util
import logging
from sql_helper import *
from time_helper import *


def unzip_handler(config):
    # 设置日志记录器
    logger = Util.mix_logger("gzip", logging.DEBUG)

    # 连接到Redis与MySQL
    mysql_pool = Util.mysql_pool(config, "GHA_MYSQL")
    redis_conn = Util.redis_conn(config, "GHA_REDIS")

    # 逐步消化解压任务
    logger.info("Gzip进程初始化完成")
    live_count = 0
    while redis_conn.scard("GHA_wait_for_unzip") != 0:
        time_tick = redis_conn.spop("GHA_wait_for_unzip")  # 尝试取出（并发场景设计）
        if time_tick is None:
            time.sleep(1)
            continue
        time_tick = time_tick.decode()
        redis_conn.sadd("GHA_gzip_task_list", time_tick)
        logger.info("正在解压时间片: {}".format(time_tick))
        # 解压文件
        aria2_path = config["GZIP"]["path"].format(config["PLAN"]["task_year"], time_tick)
        if not os.path.exists(aria2_path):
            redis_conn.sadd("GHA_failed_time_tick", time_tick)
            redis_conn.srem("GHA_gzip_task_list", time_tick)
            logger.warning("时间片<{}>数据不存在，已跳过".format(time_tick))
            continue
        data_file = open(aria2_path, "rb")
        gzip_data = gzip.GzipFile(mode="rb", fileobj=data_file).read()

        # 处理数据格式
        logger.info("正在处理时间片: {}".format(time_tick))
        gzip_data = gzip_data.decode().split("\n")
        sql_data = []
        event_data = None
        for index, event in enumerate(gzip_data):
            if len(event.strip()) == 0:
                continue
            event = json.loads(event)
            event_data = {
                "id": event["id"],
                "type": event["type"],
                "user_id": event["actor"]["id"],
                "user_url": event["actor"]["url"],
                "user_name": event["actor"]["display_login"],
                "user_login": event["actor"]["login"],
                "repo_id": event["repo"]["id"],
                "repo_url": event["repo"]["url"],
                "repo_name": event["repo"]["name"],
                "payload": json.dumps(event["payload"], ensure_ascii=False),
                "time_tick": time_tick,
                "created_at": utc_time2local_time(event["created_at"], config["PLAN"]["date_fmt"]),
            }
            sql_data.append(tuple(event_data.values()))

        # 发送至MySQL
        logger.info("正在写入时间片: {}".format(time_tick))
        mysql_conn = mysql_pool.connection()
        cursor = mysql_conn.cursor()
        cursor.executemany(mysql_replace_sql("GA_Archive", event_data), args=sql_data)
        mysql_conn.commit()

        # 记录状态
        redis_conn.sadd("GHA_exist_time_tick", time_tick)
        redis_conn.srem("GHA_gzip_task_list", time_tick)
        logger.info("时间片<{}>解压写入完成".format(time_tick))
        # 自我销毁
        live_count += 1
        if live_count > 5:
            break

    logger.warning("Gzip进程计划退出")
