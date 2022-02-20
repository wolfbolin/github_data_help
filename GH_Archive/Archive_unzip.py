# coding=utf-8
import gc
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


def unzip_process(config):
    try:
        if config["PLAN"]["unziper"] == "mongo":
            from Archive_mongo import unzip_handler
            unzip_handler(config)
        if config["PLAN"]["unziper"] == "mysql":
            from Archive_mysql import unzip_handler
            unzip_handler(config)
    except BaseException as e:
        print(e)
        traceback.print_exc()
        raise e


if __name__ == '__main__':
    _app_name = __file__.split("/")[-2]
    _config = Config.get_config(_app_name)
    main(_config)
