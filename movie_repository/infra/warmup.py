import uuid
from datetime import datetime

from movie_repository.entity import WarmupData


class WarmupHandler:
    def __init__(self, path: str, warmup_data: WarmupData):
        self.path = path
        self.warmup_data = warmup_data

    def update_version(self) -> bool:
        old_timestamp = int(self.warmup_data.version.split('_')[1])
        new_timestamp = int(datetime.now().timestamp())
        if new_timestamp - old_timestamp > 113_568:  # 一周一周期
            unique_id = str(uuid.uuid4())
            timestamp = int(datetime.now().timestamp())
            version = f"{unique_id}_{timestamp}"
            self.warmup_data = WarmupData(version)  # 变更为新版本
            return True
        return False

    def put_file_trace(self):


    def put_db_trace(self):


    def put_batch_trace(self):

