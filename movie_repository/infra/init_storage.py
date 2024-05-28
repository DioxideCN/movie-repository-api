import os
import uuid
from dataclasses import asdict
from datetime import datetime

import yaml
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection


from . import init_config
from .fetch import run_all
from .init_config import kafka_producer
from .warmup import WarmupHandler
from movie_repository.util.logger import logger
from movie_repository.entity.entity_warmup import WarmupData
from movie_repository.entity.entity_warmup import mask
from movie_repository.entity.entity_movie import MovieEntityV2

CLIENT = AsyncIOMotorClient("mongodb://localhost:27017/")  # 端
DB = CLIENT["movie_repository"]  # 库
collections: AsyncIOMotorCollection = DB[MovieEntityV2.collection]  # 表
WARMUP_PATH: str = os.path.join(init_config.saves_path, 'warmup.yaml')


async def init_database(total: int) -> WarmupHandler:
    warmup: WarmupHandler = await warmup_system(total)  # 预热系统数据
    return warmup


async def warmup_system(total: int) -> WarmupHandler:
    """
    从已有的Saves中定位状态和时间版本来决定这次是否要重新爬取数据
    计划使用warmup.yaml配置来实时更新配置状态，并决策爬取新数据
    的必要性
    """
    if not os.path.exists(WARMUP_PATH):
        logger.info(f"Configuration file 'warmup.yaml' doesn't exist, now creating it.")
        unique_id: str = str(uuid.uuid4())
        timestamp: int = int(datetime.now().timestamp())
        # 将字典写入 YAML 文件
        with open(WARMUP_PATH, 'w', encoding='utf-8') as file:
            default_warmup: WarmupData = WarmupData(f"{unique_id}_{timestamp}")
            yaml.dump(asdict(default_warmup), file)
            file.close()
            default_handler: WarmupHandler = WarmupHandler(warmup_data=default_warmup, path=WARMUP_PATH)
            await run_all(default_handler, collections, total)  # 初始化项目
            logger.info('(Kafka Lifecycle) Closing Kafka producer.')
            kafka_producer.close()
            return default_handler
    with open(WARMUP_PATH, 'r', encoding='utf-8') as file:
        warmup_data: dict = yaml.safe_load(file)
        logger.info(f"Read file 'warmup.yaml' into system.")
        read_data: WarmupData = mask(warmup_data, WarmupData)
        file.close()
        return await figure_out_warmup(read_data, total)


async def figure_out_warmup(warmup: WarmupData, total: int) -> WarmupHandler:
    instance: WarmupHandler = WarmupHandler(warmup_data=warmup, path=WARMUP_PATH)
    if instance.update_version():
        # 尝试版本更新
        logger.info('Warmup system version has been updated.')
        '''
        为什么如此伟大的Python不支持从平台线程栈的角度去构造纤程呢
        非要把一个并发的高级概念卑微地分配在一个loop循环里面，逆天
        '''
        await run_all(instance, collections, total)  # 发生版本更新要启动重新拉取的任务
    else:
        # 检查PENDING或ERROR任务
        await instance.try_rollback(collections)
    logger.info('(Kafka Lifecycle) Closing Kafka producer.')
    kafka_producer.close()
    return instance
