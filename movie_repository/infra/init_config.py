import json
import os

import aiofiles

from movie_repository.util.logger import logger
from .warmup import WarmupHandler, Status, generate_key
from movie_repository.util.default_util import TimeUtil

time_formatter: str = '%Y-%m-%d %H%M%S.%f'
saves_directory = "saves"
saves_path = os.path.join(os.path.dirname(__file__), '..', '..', saves_directory)
json_file: [str] = [
    'tencent',  # 腾讯
    'mgtv',  # 芒果
    'bilibili',  # B站
    'iqiyi',  # 爱奇艺
    'youku',  # 优酷
]


def init_configuration():
    # 确保saves文件夹存在
    if not os.path.exists(saves_path):
        logger.info("Directory 'saves' doesn't exists, now creating it.")
        os.makedirs(saves_path)
    # 确保JSON文件夹存在
    for directory_name in json_file:
        directory_path: str = os.path.join(saves_path, directory_name)
        if not os.path.exists(directory_path):
            logger.info(f"Directory '{directory_name}' doesn't exist, now creating it.")
            os.makedirs(directory_path)


async def write_in(warmup: WarmupHandler,
                   file_name: str,
                   batch: int,
                   pagesize: int,
                   data_set: any,
                   retry_on_task_id: str = ''):
    if file_name not in json_file:  # 不存在的目标文件
        err_msg = f"Target file '{file_name}' doesn't exist in 'json_file' list."
        logger.error(err_msg)
        raise FileNotFoundError(err_msg)

    if batch < 0:  # 不合法的batch批次
        err_msg = f"Batch file '{file_name}_{batch}.json' is illegal."
        logger.error(err_msg)
        raise ValueError(err_msg)

    file_task_id = retry_on_task_id if retry_on_task_id.startswith('FILE.TASK') else generate_key('FILE.TASK')
    file_begin_time = TimeUtil.now()

    warmup.put_file_trace(task_id=file_task_id, directory=file_name,
                          batch=batch, pagesize=pagesize, status=Status.PENDING,
                          begin=file_begin_time)

    file_path = os.path.join(saves_path, file_name, f'{file_name}_{batch}.json')
    logger.info(f"Writing data into 'saves/{file_name}/{file_name}_{batch}.json'.")

    async with aiofiles.open(file_path, 'w', encoding='utf-8') as file:
        await file.write(json.dumps(data_set, indent=4))

    warmup.put_file_trace(task_id=file_task_id, directory=file_name,
                          batch=batch, pagesize=pagesize, status=Status.FINISHED,
                          begin=file_begin_time, end=TimeUtil.now())
