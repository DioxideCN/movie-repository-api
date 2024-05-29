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

client = AsyncIOMotorClient("mongodb://localhost:27017/")  # 端
db = client["movie_repository"]  # 库
collections: AsyncIOMotorCollection = db[MovieEntityV2.collection]  # 表
warmup_path: str = os.path.join(init_config.saves_path, 'warmup.yaml')


async def init_database(total: int) -> WarmupHandler:
    warmup: WarmupHandler = await warmup_system(total)  # 预热系统数据
    return warmup


async def warmup_system(total: int) -> WarmupHandler:
    """
    从已有的Saves中定位状态和时间版本来决定这次是否要重新爬取数据
    计划使用warmup.yaml配置来实时更新配置状态，并决策爬取新数据
    的必要性
    """
    if not os.path.exists(warmup_path):
        logger.info(f"Configuration file 'warmup.yaml' doesn't exist, now creating it.")
        unique_id: str = str(uuid.uuid4())
        timestamp: int = int(datetime.now().timestamp())
        # 将字典写入 YAML 文件
        with open(warmup_path, 'w', encoding='utf-8') as file:
            default_warmup: WarmupData = WarmupData(f"{unique_id}_{timestamp}")
            yaml.dump(asdict(default_warmup), file)
            file.close()
            default_handler: WarmupHandler = WarmupHandler(warmup_data=default_warmup, path=warmup_path)
            await run_all(default_handler, total)  # 初始化项目
            logger.info('(Kafka Lifecycle) Closing Kafka producer.')
            kafka_producer.close()
            return default_handler
    with open(warmup_path, 'r', encoding='utf-8') as file:
        warmup_data: dict = yaml.safe_load(file)
        logger.info(f"Read file 'warmup.yaml' into system.")
        read_data: WarmupData = mask(warmup_data, WarmupData)
        file.close()
        return await figure_out_warmup(read_data, total)


async def figure_out_warmup(warmup: WarmupData, total: int) -> WarmupHandler:
    instance: WarmupHandler = WarmupHandler(warmup_data=warmup, path=warmup_path)
    if instance.update_version():
        # 尝试版本更新
        logger.info('Warmup system version has been updated.')
        await run_all(instance, total)  # 发生版本更新要启动重新拉取的任务
    else:
        # 检查PENDING或ERROR任务
        await instance.try_rollback()
    logger.info('(Kafka Lifecycle) Closing Kafka producer.')
    kafka_producer.close()
    return instance
