import asyncio
import os
import uuid
from dataclasses import asdict
from datetime import datetime

import yaml
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection

from . import fetcher
from . import init_config
from movie_repository.util.logger import logger
from movie_repository.entity.entity_warmup import WarmupData
from movie_repository.entity.entity_warmup import mask

CLIENT = AsyncIOMotorClient("mongodb://localhost:27017/")  # 端
DB = CLIENT["movie_repository"]  # 库
COLLECTIONS: AsyncIOMotorCollection = DB["movie_collection"]  # 表
WARMUP_PATH: str = os.path.join(init_config.saves_path, 'warmup.yaml')


async def init_database(total: int, page_size: int):
    """
    从bilibili, tencent, douban平台获取电影信息
    并存入mongodb数据库的movie_collection数据表中
    """
    batch = total // page_size
    warmup_system(total, page_size)  # 尝试断点重载
    '''
    从B站、豆瓣、腾讯分别爬取1000条数据，该过程理应通过异步来执行
    数据量较大，在程序中通过TOTAL和PAGE_SIZE来切分成不同的子任务
    在这里将设计为协程函数调用的形式来防止阻塞产生的严重耗时，参考：
    1. TypeScript await/async 异步流
    2. Java VirtualThread 虚拟线程池
    3. Java ForkJoinPool 工作窃取线程池(基于平台线程)
    '''
    tasks = []
    for page in range(1, batch + 1):
        tasks.append(asyncio.sleep(3))  # DEBUG 模拟阻塞
        # tasks.append(fetcher.Bilibili.run_async(COLLECTIONS, page, page_size))
        # tasks.append(fetcher.Tencent.run_async(COLLECTIONS, page))
    await asyncio.gather(*tasks)


def warmup_system(total: int, page_size: int):
    """
    从已有的Saves中定位状态和时间版本来决定这次是否要重新爬取数据
    计划使用warmup.yaml配置来实时更新配置状态，并决策爬取新数据
    的必要性
    """
    if not os.path.exists(WARMUP_PATH):
        logger.info(f"Warmup configuration doesn't exist, now creating it.")
        unique_id = str(uuid.uuid4())
        timestamp = int(datetime.now().timestamp())
        version = f"{unique_id}_{timestamp}"
        # 将字典写入 YAML 文件
        with open(WARMUP_PATH, 'w', encoding='utf-8') as file:
            yaml.dump(asdict(WarmupData(version)), file)
            file.close()
        return
    logger.info(f"Reading warmup configuration to restore current system.")
    with open(WARMUP_PATH, 'r', encoding='utf-8') as file:
        warmup_data: dict = yaml.safe_load(file)
        read_data: WarmupData = mask(warmup_data, WarmupData)
        file.close()
        figure_out_warmup(read_data)


def figure_out_warmup(warmup: WarmupData):
    """

    """
