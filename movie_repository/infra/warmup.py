import uuid
from collections import defaultdict
from dataclasses import asdict
from datetime import datetime
from enum import Enum

import yaml
from motor.motor_asyncio import AsyncIOMotorCollection

from movie_repository.entity import WarmupData
from movie_repository.infra import fetch


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

    def save_to_file(self):
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
        await fetch.rollback_all(self, collection, results)
