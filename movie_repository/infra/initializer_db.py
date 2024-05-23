import time

from dataclasses import asdict
from pymongo import MongoClient

from . import fetch
from movie_repository.entity import MovieEntity
from movie_repository.util.logger import logger

CLIENT = MongoClient("mongodb://localhost:27017/")  # 端
DB = CLIENT["movie_repository"]  # 库
COLLECTIONS = DB["movie_collection"]  # 表


def init_database(total: int, page_size: int):
    """
    从bilibili, tencent, douban平台获取电影信息
    并存入mongodb数据库的movie_collection数据表中
    """
    batch = total // page_size
    '''
    从B站、豆瓣、腾讯分别爬取1000条数据，该过程理应通过异步来执行
    数据量较大，在程序中通过TOTAL和PAGE_SIZE来切分成不同的子任务
    在这里将设计为协程函数调用的形式来防止阻塞产生的严重耗时，参考：
    1. TypeScript await/async 异步流
    2. Java VirtualThread 虚拟线程池
    3. Java ForkJoinPool 工作窃取线程池(基于平台线程)
    '''
    for page in range(1, batch):
        # 获取Bilibili电影列表
        batch_result_bilibili: [MovieEntity] = fetch.Bilibili.batch(page, page_size)
        documents: list[dict[str, any]] = [asdict(movie) for movie in batch_result_bilibili]
        logger.info(f'[Batch {page}] Inserting bilibili data into mongodb')
        COLLECTIONS.insert_many(documents)
        time.sleep(0.5)  # QOS缓冲

        batch_result_tencent: [MovieEntity] = fetch.Tencent.batch(page)
        documents: list[dict[str, any]] = [asdict(movie) for movie in batch_result_tencent]
        logger.info(f'[Batch {page}] Inserting tencent data into mongodb')
        COLLECTIONS.insert_many(documents)
        time.sleep(0.5)  # QOS缓冲
