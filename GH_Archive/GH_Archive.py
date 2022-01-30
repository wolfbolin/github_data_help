# coding=utf-8
import gzip
import json
import os
import time
import Util
import Config
import logging
import traceback
from sql_helper import *
from time_helper import *
from datetime import timedelta
from aria2_helper import Aria2
from concurrent.futures import wait
from concurrent.futures import ALL_COMPLETED
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import ProcessPoolExecutor


def main(config):
    # 初始化logger
    logger = Util.mix_logger("main", logging.DEBUG, logging.DEBUG)

    # 连接数据库
    mysql = Util.mysql_conn(config, "GHA_MYSQL")
    redis = Util.redis_conn(config, "GHA_REDIS")

    # 更新Redis记录
    redis.delete("GHA_wait_for_unzip")
    redis.delete("GHA_exist_time_tick")
    redis.delete("GHA_downloading_num")
    redis.delete("GHA_wait_for_download")
    exist_time_tick = get_app_pair(mysql, "GHA", "exist_time_tick")
    if exist_time_tick is not None:
        exist_time_tick = set(json.loads(exist_time_tick))
        if len(exist_time_tick) != 0:
            redis.sadd("GHA_exist_time_tick", *exist_time_tick)

    # 计算所有需要下载的时间片
    redis.delete("GHA_wait_for_download")
    time_tick = str2datetime(config["PLAN"]["start_time"])
    stop_time = str2datetime(config["PLAN"]["stop_time"])
    while time_tick < stop_time:
        redis.sadd("GHA_wait_for_download", fmt_time_tick(time_tick))
        time_tick += timedelta(hours=1)
    logger.info("共计{}个时间片需要下载".format(redis.scard("GHA_wait_for_download")))

    # 进程管理
    process_bin = []
    process_pool = ProcessPoolExecutor(max_workers=config["TURBO"]["max_process"])

    # 选择合适的文件下载方式
    if config["PLAN"]["downloader"] == "aria2":
        logger.info("启动Aria2下载进程")
        process_bin.append(process_pool.submit(aria_process, config))

    # 启动多进程解压
    for i in range(config["TURBO"]["max_process"] - 1):
        logger.info("启动Gzip解压进程")
        process_bin.append(process_pool.submit(unzip_process, config))

    # 等待任务结束并监控
    redis.set("GHA_downloading_num", 0)
    while any([x.running() for x in process_bin]):
        wait_task_num = redis.scard("GHA_wait_for_download")
        aria2_task_num = int(redis.get("GHA_downloading_num"))
        unzip_task_num = redis.scard("GHA_wait_for_unzip")
        logger.info("任务状态: Wait [{:^8}] Aria2 [{:>2}/{:<2}] | Unzip [{:>2}/{:<2}]"
                    .format(wait_task_num, aria2_task_num, config["ARIA2"]["aria2_depth"],
                            unzip_task_num, config["ARIA2"]["unzip_depth"]))

        # 在主进程更新记录
        exist_time_tick = list(map(lambda x: x.decode(), list(redis.smembers("GHA_exist_time_tick"))))
        set_app_pair(mysql, "GHA", "exist_time_tick", json.dumps(exist_time_tick))

        time.sleep(1)


def unzip_process(*args):
    try:
        unzip_handler(*args)
    except BaseException as e:
        traceback.print_exc()
        raise e


def unzip_handler(config):
    # 设置日志记录器
    logger = logging.getLogger("gzip")
    if config["GZIP"]["logger"]:
        logger = Util.mix_logger("gzip", logging.DEBUG)
    logger.info("Gzip输出测试")

    # 连接到Redis与MySQL
    mysql_pool = Util.mysql_pool(config, "GHA_MYSQL")
    redis_conn = Util.redis_conn(config, "GHA_REDIS")

    # 初始化进程池
    executor = ThreadPoolExecutor(max_workers=config["TURBO"]["max_thread"])

    # 逐步消化解压任务
    logger.info("Gzip进程初始化完成")
    while redis_conn.scard("GHA_wait_for_download") != 0 or redis_conn.scard("GHA_wait_for_unzip") != 0:
        time_tick = redis_conn.spop("GHA_wait_for_unzip")  # 尝试取出（并发场景设计）
        if time_tick is None:
            time.sleep(1)
            continue
        time_tick = time_tick.decode()
        logger.info("正在解压时间片: {}".format(time_tick))
        # 解压文件
        aria2_path = config["GZIP"]["path"].format(time_tick)
        data_file = open(aria2_path, "rb")
        gzip_data = gzip.GzipFile(mode="rb", fileobj=data_file).read()
        # 分片写入
        event_data = gzip_data.decode().split("\n")
        unzip_task = [executor.submit(write_process, config, mysql_pool, event, time_tick) for event in event_data]
        wait(unzip_task, return_when=ALL_COMPLETED)
        # 删除文件
        os.remove(aria2_path)
        # 记录状态
        redis_conn.sadd("GHA_exist_time_tick", time_tick)
        logger.info("时间片<{}>解压写入完成".format(time_tick))


def write_process(*args):
    try:
        write_handler(*args)
    except BaseException as e:
        traceback.print_exc()
        raise e


def write_handler(config, mysql_pool, event: str, time_tick):
    if len(event.strip()) == 0:
        return
    event = json.loads(event)

    # Write to mysql
    mysql = mysql_pool.connection()
    cursor = mysql.cursor()
    sql_data = {
        "id": event["id"],
        "type": event["type"],
        "user_id": event["actor"]["id"],
        "user_name": event["actor"]["login"],
        "user_data": json.dumps(event["actor"], ensure_ascii=False),
        "repo_id": event["repo"]["id"],
        "repo_name": event["repo"]["name"],
        "repo_data": json.dumps(event["repo"], ensure_ascii=False),
        "payload": json.dumps(event["payload"], ensure_ascii=False),
        "time_tick": time_tick,
        "created_at": utc_time2local_time(event["created_at"], config["PLAN"]["date_fmt"]),
    }
    # print(cursor.mogrify(mysql_replace_sql("GA_Archive", sql_data), args=list(sql_data.values())))
    cursor.execute(mysql_replace_sql("GA_Archive", sql_data), args=list(sql_data.values()))
    mysql.commit()


def aria_process(*args):
    try:
        aria_handler(*args)
    except BaseException as e:
        traceback.print_exc()
        raise e


def aria_handler(config):
    # 设置日志记录器
    logger = logging.getLogger("aria2")
    if config["ARIA2"]["logger"]:
        logger = Util.mix_logger("aria2", logging.DEBUG)
    logger.info("Aria2输出测试")

    # 连接到Redis
    redis = Util.redis_conn(config, "GHA_REDIS")

    # 连接到Aria2
    aria2 = Aria2(**config["ARIA2"]["aria2"])
    aria2.set_default_attr("gid", "status", "files", "errorCode")

    # 逐步消化下载任务
    logger.info("Aria2进程初始化完成")
    while redis.scard("GHA_wait_for_download") != 0:
        # 回收已完成的下载任务
        finish_list = aria2.tellStopped()
        for task in finish_list:
            time_tick = task["files"][0]["uris"][0]["uri"].split("/")[-1].split(".")[0]
            if task["status"] != "complete":
                logger.warning("时间片<{}>下载失败({})".format(time_tick, task["errorCode"]))
            else:
                logger.info("时间片<{}>下载完成".format(time_tick))
                redis.sadd("GHA_wait_for_unzip", time_tick)  # 加入解压集合
            aria2.removeDownloadResult(task["gid"])

        # 考虑下游流程队列深度
        if redis.scard("GHA_wait_for_unzip") < config["ARIA2"]["unzip_depth"]:
            # 获取下载器中任务量
            active_list = aria2.tellActive()
            active_list = set([x["files"][0]["uris"][0]["uri"].split("/")[-1].split(".")[0] for x in active_list])
            waiting_list = aria2.tellWaiting()
            waiting_list = set([x["files"][0]["uris"][0]["uri"].split("/")[-1].split(".")[0] for x in waiting_list])
            download_list = set.union(active_list, waiting_list)
            redis.set("GHA_downloading_num", str(len(download_list)))

            # 控制下载器中的任务量
            if len(download_list) < config["ARIA2"]["aria2_depth"]:
                time_tick = redis.spop("GHA_wait_for_download").decode()
                logger.info("新增下载任务<{}>".format(time_tick))
                download_url = ["https://data.gharchive.org/{}.json.gz".format(time_tick)]
                aria2.addUri(download_url)

        time.sleep(1)


if __name__ == '__main__':
    _app_name = __file__.split("/")[-2]
    _config = Config.get_config(_app_name)
    main(_config)
