# coding=utf-8
import gc
import os
import gzip
import json
import time
import Util
import Config
import logging
import traceback
from sql_helper import *
from time_helper import *
from datetime import timedelta
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool


def main(config):
    # 初始化logger
    logger = Util.mix_logger("main", logging.DEBUG, logging.DEBUG)

    # 连接数据库
    mysql_conn = Util.mysql_conn(config, "GHA_MYSQL")
    redis_conn = Util.redis_conn(config, "GHA_REDIS")

    # 更新Redis记录
    redis_conn.flushdb()
    exist_time_tick = get_app_pair(mysql_conn, "GHA", "exist_time_tick")
    if exist_time_tick is not None:
        exist_time_tick = set(json.loads(exist_time_tick))
        if len(exist_time_tick) != 0:
            redis_conn.sadd("GHA_exist_time_tick", *exist_time_tick)
    failed_time_tick = get_app_pair(mysql_conn, "GHA", "failed_time_tick")
    if failed_time_tick is not None:
        failed_time_tick = set(json.loads(failed_time_tick))
        if len(failed_time_tick) != 0:
            redis_conn.sadd("GHA_failed_time_tick", *failed_time_tick)

    # 计算所有需要解压的时间片
    time_tick = str2datetime(config["PLAN"]["start_time"])
    stop_time = str2datetime(config["PLAN"]["stop_time"])
    while time_tick < stop_time:
        time_tick_str = fmt_time_tick(time_tick)
        if not redis_conn.sismember("GHA_exist_time_tick", time_tick_str):
            redis_conn.sadd("GHA_wait_for_unzip", time_tick_str)
        time_tick += timedelta(hours=1)
    logger.info("共计{}个时间片需要解压".format(redis_conn.scard("GHA_wait_for_unzip")))

    # 进程管理
    process_bin = []
    process_pool = ProcessPoolExecutor(max_workers=config["TURBO"]["max_process"])

    # 启动多进程解压
    for i in range(config["TURBO"]["max_process"]):
        logger.info("启动Gzip解压进程")
        process_bin.append(process_pool.submit(unzip_process, config))

    # 等待任务结束并监控
    while redis_conn.scard("GHA_wait_for_unzip") != 0 or redis_conn.scard("GHA_gzip_task_list") != 0:
        msg = "任务状态: "
        msg += "Wait unzip[{}] ".format(redis_conn.scard("GHA_wait_for_unzip"))
        msg += "Gzip[{:>2}/{:<2}] ".format(redis_conn.scard("GHA_gzip_task_list"), config["TURBO"]["max_process"])
        logger.info(msg)

        # 在主进程更新记录
        update_exist_time_tick(redis_conn, mysql_conn)

        # 管理进程生命周期
        for index in range(len(process_bin)):
            if process_bin[index].done() and redis_conn.scard("GHA_wait_for_unzip") > 0:
                gc.collect()
                time.sleep(1)
                try:
                    process_bin[index] = process_pool.submit(unzip_process, config)
                except BrokenProcessPool:
                    logger.error("进程池被迫中断，正在重启")
                    redis_conn.sadd("GHA_wait_for_unzip", *list(redis_conn.smembers("GHA_gzip_task_list")))
                    redis_conn.delete("GHA_gzip_task_list")
                    process_pool = ProcessPoolExecutor(max_workers=config["TURBO"]["max_process"])

        time.sleep(1)
    else:
        update_exist_time_tick(redis_conn, mysql_conn)

    msg = ["进程状态: "]
    for i, x in enumerate(process_bin):
        e = x.exception()
        if e is not None:
            msg.append("进程<{}>异常[{}]".format(i, e))
    if len(msg) != 1:
        logger.error("".join(msg))
    logger.warning("程序终止")


def update_exist_time_tick(redis_conn, mysql_conn):
    exist_time_tick = list(map(lambda x: x.decode(), list(redis_conn.smembers("GHA_exist_time_tick"))))
    set_app_pair(mysql_conn, "GHA", "exist_time_tick", json.dumps(exist_time_tick))

    exist_time_tick = list(map(lambda x: x.decode(), list(redis_conn.smembers("GHA_failed_time_tick"))))
    set_app_pair(mysql_conn, "GHA", "failed_time_tick", json.dumps(exist_time_tick))


def unzip_process(*args):
    try:
        unzip_handler(*args)
    except BaseException as e:
        print(e)
        traceback.print_exc()
        raise e


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

    logger.warning("Gzip进程退出")


if __name__ == '__main__':
    _app_name = __file__.split("/")[-2]
    _config = Config.get_config(_app_name)
    main(_config)
