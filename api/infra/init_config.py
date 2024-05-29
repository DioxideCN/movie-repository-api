import os

import json
from dataclasses import asdict, fields
from typing import List

from kafka import KafkaProducer, KafkaClient, KafkaConsumer

from api.entity.entity_movie import MovieEntityV2
from api.util.logger import logger

kafka_host = 'localhost:9092'
kafka_file_topic = 'file_updater_topic'
kafka_db_topic = 'db_updater_topic'
kafka_file_group = 'file_updater_group'
kafka_db_group = 'db_updater_group'
kafka_client: KafkaClient = KafkaClient(bootstrap_servers=kafka_host)
kafka_producer: KafkaProducer = KafkaProducer(bootstrap_servers=kafka_host)
kafka_file_consumer: KafkaConsumer = KafkaConsumer(kafka_file_topic,
                                                   bootstrap_servers=kafka_host,
                                                   group_id=kafka_file_group,
                                                   auto_offset_reset='earliest',
                                                   enable_auto_commit=True,
                                                   consumer_timeout_ms=1000,
                                                   session_timeout_ms=30000,
                                                   request_timeout_ms=305000,
                                                   max_poll_interval_ms=300000)
kafka_db_consumer: KafkaConsumer = KafkaConsumer(kafka_db_topic,
                                                 bootstrap_servers=kafka_host,
                                                 group_id=kafka_db_group,
                                                 auto_offset_reset='earliest',
                                                 enable_auto_commit=True,
                                                 consumer_timeout_ms=1000,
                                                 session_timeout_ms=30000,
                                                 request_timeout_ms=305000,
                                                 max_poll_interval_ms=300000)


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


async def init_configuration():
    # 确保saves文件夹存在
    if not os.path.exists(saves_path):
        logger.info("Resource root directory '/saves' doesn't exists, now creating it.")
        os.makedirs(saves_path)
    # 确保JSON文件夹存在
    for directory_name in json_file:
        directory_path: str = os.path.join(saves_path, directory_name)
        if not os.path.exists(directory_path):
            logger.info(f" -- Created '{directory_name}' sub-directory in root directory '/saves'.")
            os.makedirs(directory_path)
    # 初始化Kafka并尝试添加topic
    kafka_client.add_topic(kafka_file_topic)
    kafka_client.add_topic(kafka_db_topic)
    kafka_client.close()
    # 尝试消费掉上次没有消费的消息
    consume_messages(kafka_file_consumer, kafka_file_topic)
    consume_messages(kafka_db_consumer, kafka_db_topic)


def push_file_dump_msg(file_name: str,
                       batch: int,
                       pagesize: int,
                       data_set: any,
                       retry_on_task_id: str = ''):
    if file_name not in json_file:  # 不存在的目标文件
        err_msg = f"Cannot save data, platform '{file_name}' hasn't been included in system."
        logger.error(err_msg)
        raise FileNotFoundError(err_msg)

    if batch <= 0:  # 不合法的batch批次
        err_msg = f"Batch '{file_name}_{batch}.json' is illegal."
        logger.error(err_msg)
        raise ValueError(err_msg)

    message = json.dumps({
        "file_name": file_name,
        "batch": batch,
        "pagesize": pagesize,
        "data_set": data_set,
        "retry_on_task_id": retry_on_task_id
    })
    kafka_producer.send(kafka_file_topic, message.encode('utf-8'))


def push_db_insert_msg(movies: List[MovieEntityV2]):
    """
    将新增的数据推送到kafka顺序消费
    """
    for movie in movies:
        # 转换dataclass到dict，并移除不需要的字段
        movie_dict = asdict(movie)
        exclude_fields = {field.name for field in fields(movie) if field.metadata.get('exclude')}
        for field in exclude_fields:
            movie_dict.pop(field, None)
        kafka_producer.send(kafka_db_topic, json.dumps(movie_dict).encode('utf-8'))


def consume_messages(consumer, topic_name):
    counter = 0
    try:
        for msg in consumer:
            counter += 1
        logger.info(f'(Kafka Lifecycle) Consumed {counter} message(s) in last {topic_name}.')
    except Exception as e:
        logger.error(f'Error while consuming messages from {topic_name}: {e}')
    finally:
        consumer.close()
