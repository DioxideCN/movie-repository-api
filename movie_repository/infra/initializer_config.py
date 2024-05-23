import json
import os
from dataclasses import asdict

from movie_repository.util.logger import logger


saves_directory = "saves"
saves_path = os.path.join(os.path.dirname(__file__), '..', '..', saves_directory)
json_file: [str] = [
    'tencent',  # 腾讯
    'douban',   # 豆瓣
    'bilibili',  # Bilibili
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


def write_in(file_name: str, batch: int, data_set: any):
    if file_name not in json_file:  # 不存在的目标文件
        err_msg: str = f"Target file '{file_name}' doesn't exits in 'json_file' list."
        logger.error(err_msg)
        raise FileNotFoundError(err_msg)
    if batch < 0:  # 不合法的batch批次
        err_msg: str = f"Batch file '{file_name}_{batch}.json' is illegal."
        logger.error(err_msg)
        raise ValueError(err_msg)
    file_path: str = os.path.join(saves_path, file_name, f'{file_name}_{batch}.json')
    logger.info(f"Writing data into 'saves/{file_name}/{file_name}_{batch}.json'.")
    with open(file_path, 'w') as file:
        json.dump(data_set, file, indent=4)
