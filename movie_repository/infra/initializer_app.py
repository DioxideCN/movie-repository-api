import os

from movie_repository.util.logger import logger
from .initializer_config import init_configuration
from .initializer_db import init_database

TOTAL = int(os.getenv("total", "60"))
PAGE_SIZE = int(os.getenv("page-size", "30"))


async def init_app():
    logger.info('Starting post initializer...')
    logger.info(f'Found environment variables: total {TOTAL} page-size {PAGE_SIZE}.')
    # Step1.初始化并读取配置文件
    logger.info('Initializing configuration files...')
    init_configuration()
    # Step2.初始化并载入mongodb
    logger.info('Initializing mongo db...')
    await init_database(TOTAL, PAGE_SIZE)
    # Step3.启动fast api服务
    logger.info('Starting fast api server...')
