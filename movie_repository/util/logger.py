import logging
import os
from datetime import datetime

date_str = datetime.now().strftime("%Y-%m-%d")

# 日志文件的路径
log_directory = "logs"
log_path = os.path.join(os.path.dirname(__file__), '..', '..', 'logs', f'{date_str}.log')

# 确保日志目录存在
if not os.path.exists(os.path.dirname(log_path)):
    os.makedirs(os.path.dirname(log_path))

# 配置日志器基本规则
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)-5s --- [ %(module)-18s ] %(funcName)-20s : %(message)s',
    handlers=[
        logging.FileHandler(log_path, encoding='utf-8'),  # 文件处理器，写入到指定的日志文件
        logging.StreamHandler()  # 控制台处理器，同时在控制台输出
    ]
)

# 创建日志记录器
logger = logging.getLogger()


def format_msg(msg: str) -> str:
    return f'{msg}'


def info(msg: str):
    logger.info(f'{msg}')


def debug(msg: str):
    logger.debug(f'{msg}')


def warning(msg: str):
    logger.warning(f'{msg}')


def error(msg: str):
    logger.error(f'{msg}')
