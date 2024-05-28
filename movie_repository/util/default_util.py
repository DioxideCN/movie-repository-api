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


class MongoUtil:
    @staticmethod
    async def batch_put(collection: AsyncIOMotorCollection,
                        movies: List[MovieEntityV2]):
        for movie in movies:
            # 转换dataclass到dict，并移除不需要的字段
            movie_dict = asdict(movie)
            exclude_fields = {field.name for field in fields(movie) if field.metadata.get('exclude')}
            for field in exclude_fields:
                movie_dict.pop(field, None)
            # 准备批量操作
            await collection.update_one(
                {"_id": StringUtil.hash(movie.fixed_title)},
                {
                    "$setOnInsert": movie_dict,
                    "$addToSet": {"platform_data": {"$each": movie_dict.pop('platform_detail')}}
                },
                upsert=True
            )


# 该版本使用堆栈形式的生产者消费者模型来串行化
class MongoUtilV2:
    operations_stack = []
    batch_size: int = 60

    @staticmethod
    async def batch_put(collection: AsyncIOMotorCollection,
                        movies: List[MovieEntityV2]):
        for movie in movies:
            # 转换dataclass到dict，并移除不需要的字段
            movie_dict = asdict(movie)
            exclude_fields = {field.name for field in fields(movie) if field.metadata.get('exclude')}
            for field in exclude_fields:
                movie_dict.pop(field, None)
            # 准备批量操作
            platform_data_item = movie_dict.pop('platform_detail')
            op = UpdateOne(
                {"_id": movie_dict['_id']},
                [{
                    "$set": {
                        "data": movie_dict['data'],  # 更新顶级data字段
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
            MongoUtilV2.operations_stack.append(op)
            if len(MongoUtilV2.operations_stack) >= MongoUtilV2.batch_size:
                await MongoUtilV2.execute_batch(collection)

    @staticmethod
    async def execute_batch(collection: AsyncIOMotorCollection):
        if MongoUtilV2.operations_stack:
            try:
                result = await collection.bulk_write(MongoUtilV2.operations_stack)
                logger.info(f"Batch executed: {result.bulk_api_result}")
            except BulkWriteError as e:
                logger.error(f"Error executing batch: {e.details}")
            MongoUtilV2.operations_stack.clear()  # 清空栈

    @staticmethod
    async def finalize(collection: AsyncIOMotorCollection):
        # 最后一次执行
        await MongoUtilV2.execute_batch(collection)


class TimeUtil:
    @staticmethod
    def now() -> str:
        """
        获取UTC+8时区的调用该函数的时间
        格式为：2024-05-35 21:26:17,253
        """
        return datetime.now(_tz_utc_8).strftime(_time_formatter)[:-3]
