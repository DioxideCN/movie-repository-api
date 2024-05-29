import re
from dataclasses import asdict, fields
from datetime import timezone, timedelta, datetime
from hashlib import sha256
from typing import List

from motor.motor_asyncio import AsyncIOMotorCollection
from pymongo import UpdateOne
from pymongo.errors import BulkWriteError

from movie_repository.entity.entity_movie import MovieEntityV2
from movie_repository.util import logger

_time_formatter: str = '%Y-%m-%d %H:%M:%S.%f'
_tz_utc_8 = timezone(timedelta(hours=8))
_chinese_punctuations = r'[\u3000-\u303F\uff00-\uffef]'  # 中文标点符号


class StringUtil:
    @staticmethod
    def hash(val: str) -> str:
        # 使用re.sub将中文标点替换为空字符串
        return sha256(re.sub(_chinese_punctuations, '', val).encode()).hexdigest()


class TimeUtil:
    @staticmethod
    def now() -> str:
        """
        获取UTC+8时区的调用该函数的时间
        格式为：2024-05-35 21:26:17,253
        """
        return datetime.now(_tz_utc_8).strftime(_time_formatter)[:-3]
