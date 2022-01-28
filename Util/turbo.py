# coding=utf-8
import Util
import queue
import pymysql
import logging
import multiprocessing
from redis import ConnectionPool
from dbutils.pooled_db import PooledDB
from concurrent.futures import ThreadPoolExecutor


def turbo_multiprocess(config: dict, turbo_function: callable, common_data, args_list: list):
    # 日志输出准备
    logger = logging.getLogger(config["TURBO"]["main_logger"])

    # 池化参数
    process_bin = []
    result_data = []
    max_thread = config["TURBO"]["max_thread"]
    max_process = config["TURBO"]["max_process"]
    core_num = min(Util.cpu_core(), max_process)
    result_list = multiprocessing.Manager().list()
    task_queue = multiprocessing.Queue(maxsize=max_process * max_thread)

    # 创建进程
    for i in range(core_num):
        process_arg = (i, config, turbo_function, task_queue, result_list)
        process = multiprocessing.Process(target=turbo_multithread, args=process_arg)
        process.start()
        process_bin.append(process)

    # 下发任务数据
    np = 0
    for num, data in enumerate(args_list):
        task_queue.put((common_data, data))
        nnp = int((num + 1) * 100 / len(args_list))
        if nnp > np + 1:
            np = nnp
            logger.info("Task sending rate: {}%".format(np))

    # 下发终止指令
    for i in range(core_num):
        task_queue.put(Util.ExitSignal("Done"))

    # 等待进程结束
    for process in process_bin:
        process.join()

    # 收集计算产物
    for message in result_list:
        result_data.append(message)

    return result_data


def turbo_multithread(thread_id: int, config: dict, turbo_function: callable,
                      data_input: multiprocessing.Queue, data_output):
    # 日志输出准备
    logger = Util.mix_logger(config["TURBO"]["fork_logger"].format(thread_id))

    # 建立连接池
    mysql_pool = PooledDB(creator=pymysql, **config["MYSQL"], **config["MYSQL_POOL"])
    redis_pool = ConnectionPool(**config["REDIS"])
    db_pool = {"mysql": mysql_pool, "redis": redis_pool}

    # 建立线程池
    max_thread = config["TURBO"]["max_thread"]
    data_buffer = queue.Queue(maxsize=max_thread)
    executor = ThreadPoolExecutor(max_workers=max_thread)
    data_iter = DataIterator(config, logger, db_pool, turbo_function, data_input, data_buffer)

    # 动态分配任务并收集结果
    for res in executor.map(turbo_factory, data_iter):
        if res is not None:
            data_output.append(res)


class DataIterator:
    def __init__(self, config: dict, logger: logging.Logger, db_pool: dict, turbo_function: callable,
                 data_input: multiprocessing.Queue, data_buffer: queue.Queue):
        self.config = config
        self.logger = logger
        self.db_pool = db_pool
        self.data_input = data_input
        self.data_buffer = data_buffer
        self.turbo_function = turbo_function

    def __iter__(self):
        return self

    def __next__(self):
        function_args = self.data_input.get()
        if isinstance(function_args, Util.ExitSignal):
            raise StopIteration
        self.data_buffer.put(function_args)
        return self.config, self.logger, self.db_pool, self.turbo_function, self.data_buffer


def turbo_factory(data_pack):
    config, logger, db_pool, turbo_function, data_buffer = data_pack
    function_arg = data_buffer.get()
    return turbo_function(config, logger, db_pool, *function_arg)


def turbo_function_demo(config: dict, logger: logging.Logger, db_pool: dict, common_data, function_data):
    pass
