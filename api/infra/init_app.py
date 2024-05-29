import os

from api.util.logger import logger
from .init_config import init_configuration
from .init_storage import init_database, collections

TOTAL = int(os.getenv("total", "60"))


async def init_app():
    logger.info('Initializing Movie Repository API 0.0.1-SNAPSHOT application.')
    if not TOTAL == 60:
        logger.info(f'Found specific environment variable total of value int({TOTAL}).')
    # Step1.初始化并读取配置文件
    logger.info('Initializing configuration embedded Kafka client on port(s): 9092.')
    await init_configuration()
    # Step2.初始化并载入mongodb
    logger.info('Initializing storage module embedded MongoDB client on port(s): 27017.')
    warmup = await init_database(TOTAL)
    await warmup.finalizer(collections)
