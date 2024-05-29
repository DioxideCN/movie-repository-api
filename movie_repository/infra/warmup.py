import asyncio
import json
import os
import uuid
from collections import defaultdict
from dataclasses import asdict
from datetime import datetime
from enum import Enum

import aiofiles
import yaml
from kafka import KafkaConsumer
from motor.motor_asyncio import AsyncIOMotorCollection

from movie_repository.entity import WarmupData
from movie_repository.infra import fetch
from movie_repository.infra.init_config import saves_path, kafka_db_topic, \
    kafka_host, kafka_db_group, kafka_file_topic, kafka_file_group
from movie_repository.util import logger
from movie_repository.util.default_util import TimeUtil


class Status(str, Enum):
    PENDING = "PENDING"
    FINISHED = "FINISHED"
    ERROR = "ERROR"


def generate_key(prefix: str = 'TASK') -> str:
    """生成任务用的随机统一UUID"""
    return f'{prefix}#{str(uuid.uuid4())}'


class WarmupHandler:
    def __init__(self, warmup_data: WarmupData, path: str = ''):
        self.path = path
        self.warmup_data = warmup_data

    def update_version(self) -> bool:
        """
        尝试更新版本，如果更新成功返回True，不需要更新返回False
        """
        old_timestamp = int(self.warmup_data.version.split('_')[1])
        new_timestamp = int(datetime.now().timestamp())
        if new_timestamp - old_timestamp > 113_568:  # 一周一周期
            unique_id = str(uuid.uuid4())
            timestamp = int(datetime.now().timestamp())
            version = f"{unique_id}_{timestamp}"
            self.warmup_data = WarmupData(version)  # 变更为新版本时需要清空trace
            return True
        return False

    def put_file_trace(self, task_id: str, directory: str, batch: int, pagesize: int,
                       status: Status, begin: str = "", end: str = ""):
        trace_item: dict[str, any] = {
            'task_id': task_id,
            'directory': directory,
            'file_name': batch,
            'pagesize': pagesize,
            'status': status.value,
            'begin': begin,
            'end': end,
        }
        self.put_trace('file', trace_item)

    def put_batch_trace(self, task_id: str, source: str, batch: int, pagesize: int,
                        status: Status, begin: str = "", end: str = ""):
        trace_item: dict[str, any] = {
            'task_id': task_id,
            'source': source,
            'batch': batch,
            'pagesize': pagesize,
            'status': status.value,
            'begin': begin,
            'end': end,
        }
        self.put_trace('batch', trace_item)

    def put_trace(self, node: str, val: dict[str, any]):
        read_data = self.warmup_data
        if node == 'file':
            trace_list = read_data.snapshot.file.trace
        else:
            trace_list = read_data.snapshot.batch.trace
        # 查找并更新或插入新的记录
        updated = False
        for trace_item in trace_list:
            if trace_item.get('task_id') == val.get('task_id'):
                trace_item.update(val)
                updated = True
                break
        if not updated:
            trace_list.append(val)
        # 更新内存中的 warmup_data
        self.warmup_data = read_data

    async def finalizer(self, collection: AsyncIOMotorCollection):
        """
        warmup最后处理器，用来消费kafka、写入数据库和存储文件
        """
        await asyncio.gather(
            self.consume_kafka_db_dump(collection),
            self.consume_kafka_file_dump()
        )
        # 将内存中的 warmup_data 写入文件
        logger.info('Starting saving warmup resources into file.')
        with open(self.path, 'w', encoding='utf-8') as file:
            yaml.dump(asdict(self.warmup_data), file)

    @staticmethod
    async def consume_kafka_db_dump(collection: AsyncIOMotorCollection):
        """
        消费kafka的数据库新增消息
        """
        logger.info('(Warmup Finalizer) Starting saving record(s) into mongodb.')
        kafka_db_consumer: KafkaConsumer = KafkaConsumer(kafka_db_topic,
                                                         bootstrap_servers=kafka_host,
                                                         group_id=kafka_db_group,
                                                         auto_offset_reset='earliest',
                                                         enable_auto_commit=True,
                                                         consumer_timeout_ms=1000)
        counter = 0
        for msg in kafka_db_consumer:
            data = json.loads(msg.value.decode('utf-8'))
            platform_data_item = data.pop('platform_detail')[0]
            await collection.update_one(
                {"_id": data['_id']},
                [{
                    "$set": {
                        "fixed_title": data['fixed_title'],
                        "create_time": {
                            "$cond": {
                                "if": {"$or": [{"$eq": ["$create_time", None]}, {"$not": ["$create_time"]}]},
                                "then": data['create_time'],
                                "else": "$create_time"
                            }
                        },
                        "update_time": data['update_time'],
                        "platform_data": {
                            "$cond": {
                                "if": {"$isArray": "$platform_data"},
                                "then": {
                                    "$function": {
                                        "body": """function(platform_data, new_data) { let index = 
                                                platform_data.findIndex(item => item.source === new_data.source); 
                                                if (index !== -1) { platform_data[index] = new_data; } else { 
                                                platform_data.push(new_data); } return platform_data; }""",
                                        "args": ["$platform_data", platform_data_item],
                                        "lang": "js"
                                    }
                                },
                                "else": [platform_data_item]  # 如果platform_data不存在，创建新数组
                            }
                        }
                    }
                }],
                upsert=True
            )
            counter += 1
        logger.info(f'Successfully saving {counter} record(s) into mongodb.')
        logger.info('(Kafka Lifecycle) Closing Kafka DB Consumer.')
        kafka_db_consumer.close()

    async def consume_kafka_file_dump(self):
        """
        消费掉所有kafka中的file消息，写入json文件
        """
        logger.info("(Warmup Finalizer) Starting saving json file(s) into 'saves'.")
        kafka_file_consumer: KafkaConsumer = KafkaConsumer(kafka_file_topic,
                                                           bootstrap_servers=kafka_host,
                                                           group_id=kafka_file_group,
                                                           auto_offset_reset='earliest',
                                                           enable_auto_commit=True,
                                                           consumer_timeout_ms=1000)
        counter = 0
        for msg in kafka_file_consumer:
            data = json.loads(msg.value.decode('utf-8'))
            # 写入逻辑
            retry_on_task_id: str = data['retry_on_task_id']
            file_name: str = data['file_name']
            batch: int = data['batch']
            pagesize: int = data['pagesize']
            data_set: any = data['data_set']
            # 从kafka消费消息来顺序写入文件
            file_task_id = retry_on_task_id if retry_on_task_id.startswith('FILE.TASK') else generate_key(
                'FILE.TASK')
            file_begin_time = TimeUtil.now()
            self.put_file_trace(task_id=file_task_id, directory=file_name,
                                batch=batch, pagesize=pagesize, status=Status.PENDING,
                                begin=file_begin_time)
            file_path = os.path.join(saves_path, file_name, f'{file_name}_{batch}.json')
            async with aiofiles.open(file_path, 'w', encoding='utf-8') as file:
                await file.write(json.dumps(data_set, indent=4))
            self.put_file_trace(task_id=file_task_id, directory=file_name,
                                batch=batch, pagesize=pagesize, status=Status.FINISHED,
                                begin=file_begin_time, end=TimeUtil.now())
            counter += 1
        logger.info(f"Successfully saving {counter} json file(s) into 'saves'.")
        logger.info('(Kafka Lifecycle) Closing Kafka File Consumer.')
        kafka_file_consumer.close()

    async def try_rollback(self):
        """
        尝试从PENDING和ERROR的检查点回滚数据
        """
        results = defaultdict(lambda: defaultdict(int))
        for component_name in ['file', 'db', 'batch']:
            component = getattr(self.warmup_data.snapshot, component_name)
            for trace in component.trace:
                if trace.get('status') != Status.FINISHED.value:
                    platform = trace['source']
                    task_id = trace['task_id']
                    results[platform][task_id] = int(trace['batch'])
                    logger.info(f'(Rollback) Task {task_id} with {trace["status"]} '
                                f'at {platform} platform will be rollback.')
        await fetch.rollback_all(self, results)
