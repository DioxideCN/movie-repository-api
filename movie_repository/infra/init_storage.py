import asyncio
import os
import uuid
from dataclasses import asdict
from datetime import datetime

import yaml
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection

from . import fetcher
from . import init_config
from .warmup import WarmupHandler
from movie_repository.util.logger import logger
from movie_repository.entity.entity_warmup import WarmupData
from movie_repository.entity.entity_warmup import mask

CLIENT = AsyncIOMotorClient("mongodb://localhost:27017/")  # 端
DB = CLIENT["movie_repository"]  # 库
COLLECTIONS: AsyncIOMotorCollection = DB["movie_collection"]  # 表
WARMUP_PATH: str = os.path.join(init_config.saves_path, 'warmup.yaml')


async def init_database(total: int, page_size: int) -> WarmupHandler:
    """
    从bilibili, tencent, douban平台获取电影信息
    并存入mongodb数据库的movie_collection数据表中
    """
    batch: int = total // page_size  # 总子任务数
    warmup: WarmupHandler = warmup_system()  # 预热系统数据
    '''爬取数据应通过异步执行，通过TOTAL和PAGE_SIZE来切分子任务'''
    tasks = []
    for page in range(1, batch + 1):
        '''
        为什么如此伟大的Python不支持从平台线程栈的角度去构造纤程呢
        非要把一个并发的高级概念卑微地分配在一个loop循环里面，逆天
        '''
        tasks.append(fetcher.Bilibili.run_async(warmup, COLLECTIONS, page, page_size))
        tasks.append(fetcher.Tencent.run_async(warmup, COLLECTIONS, page))
    await asyncio.gather(*tasks)
    return warmup


def warmup_system() -> WarmupHandler:
    """
    从已有的Saves中定位状态和时间版本来决定这次是否要重新爬取数据
    计划使用warmup.yaml配置来实时更新配置状态，并决策爬取新数据
    的必要性
    """
    if not os.path.exists(WARMUP_PATH):
        logger.info(f"Warmup configuration doesn't exist, now creating it.")
        unique_id: str = str(uuid.uuid4())
        timestamp: int = int(datetime.now().timestamp())
        # 将字典写入 YAML 文件
        with open(WARMUP_PATH, 'w', encoding='utf-8') as file:
            default_warmup = WarmupData(f"{unique_id}_{timestamp}")
            yaml.dump(asdict(default_warmup), file)
            file.close()
        return WarmupHandler(path=WARMUP_PATH, warmup_data=default_warmup)
    logger.info(f"Reading warmup configuration to restore current system.")
    with open(WARMUP_PATH, 'r', encoding='utf-8') as file:
        warmup_data: dict = yaml.safe_load(file)
        read_data: WarmupData = mask(warmup_data, WarmupData)
        file.close()
    return figure_out_warmup(read_data)


def figure_out_warmup(warmup: WarmupData) -> WarmupHandler:
    instance: WarmupHandler = WarmupHandler(path=WARMUP_PATH, warmup_data=warmup)
    instance.update_version()  # 调用版本更新
    return instance
