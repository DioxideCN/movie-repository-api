import json
import os
import uuid
from collections import defaultdict
from dataclasses import asdict
from datetime import datetime
from enum import Enum

import aiofiles
import yaml
from motor.motor_asyncio import AsyncIOMotorCollection

from movie_repository.entity import WarmupData
from movie_repository.infra import fetch
from movie_repository.infra.init_config import kafka_consumer, saves_path
from movie_repository.util import logger
from movie_repository.util.default_util import TimeUtil


class Status(str, Enum):
    PENDING = "PENDING"
    FINISHED = "FINISHED"
    ERROR = "ERROR"


def generate_key(prefix: str = 'TASK') -> str:
    return f'{prefix}#{str(uuid.uuid4())}'


class WarmupHandler:
    def __init__(self, warmup_data: WarmupData, path: str = ''):
        self.path = path
        self.warmup_data = warmup_data

    def update_version(self) -> bool:
        old_timestamp = int(self.warmup_data.version.split('_')[1])
        new_timestamp = int(datetime.now().timestamp())
        if new_timestamp - old_timestamp > 113_568:  # 一周一周期
            unique_id = str(uuid.uuid4())
            timestamp = int(datetime.now().timestamp())
            version = f"{unique_id}_{timestamp}"
            self.warmup_data = WarmupData(version)  # 变更为新版本时需要清空trace
            return True
        return False

    def put_file_trace(self,
                       task_id: str,
                       directory: str,
                       batch: int,
                       pagesize: int,
                       status: Status,
                       begin: str = "",
                       end: str = ""):
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

    def put_db_trace(self,
                     task_id: str,
                     platform: str,
                     batch: int,
                     pagesize: int,
                     status: Status,
                     begin: str = "",
                     end: str = ""):
        trace_item: dict[str, any] = {
            'task_id': task_id,
            'platform': platform,
            'batch': batch,
            'pagesize': pagesize,
            'status': status.value,
            'begin': begin,
            'end': end,
        }
        self.put_trace('db', trace_item)

    def put_batch_trace(self,
                        task_id: str,
                        source: str,
                        batch: int,
                        pagesize: int,
                        status: Status,
                        begin: str = "",
                        end: str = ""):
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
        elif node == 'db':
            trace_list = read_data.snapshot.db.trace
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

    async def save_to_file(self):
        # 消费掉所有kafka中的file消息
        for msg in kafka_consumer:
            data = json.loads(msg.value.decode('utf-8'))
            # 假设这是你的写入逻辑
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
            logger.info(f"(Kafka Lifecycle) Saving data into file 'saves/{file_name}/{file_name}_{batch}.json'.")
            async with aiofiles.open(file_path, 'w', encoding='utf-8') as file:
                await file.write(json.dumps(data_set, indent=4))
            self.put_file_trace(task_id=file_task_id, directory=file_name,
                                batch=batch, pagesize=pagesize, status=Status.FINISHED,
                                begin=file_begin_time, end=TimeUtil.now())
        logger.info('(Kafka Lifecycle) Closing Kafka consumer.')
        kafka_consumer.close()
        logger.info('Starting saving warmup resources into file.')
        # 将内存中的 warmup_data 写入文件
        with open(self.path, 'w', encoding='utf-8') as file:
            yaml.dump(asdict(self.warmup_data), file)

    async def try_rollback(self, collection: AsyncIOMotorCollection):
        # 尝试从PENDING和ERROR的检查点回滚数据
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
        await fetch.rollback_all(self, collection, results)
