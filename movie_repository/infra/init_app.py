import os

from movie_repository.util.logger import logger
from .init_config import init_configuration
from .init_storage import init_database

TOTAL = int(os.getenv("total", "60"))


async def init_app():
    logger.info('Starting post initializer...')
    logger.info(f'Found environment variables: total {TOTAL}.')
    # Step1.初始化并读取配置文件
    logger.info('Initializing configuration files...')
    init_configuration()
    # Step2.初始化并载入mongodb
    logger.info('Initializing mongo db...')
    warmup = await init_database(TOTAL)
    logger.info('Saving warmup information...')
    warmup.save_to_file()
