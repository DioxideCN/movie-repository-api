import asyncio
import threading

from kafka import KafkaConsumer, KafkaProducer
from motor.motor_asyncio import AsyncIOMotorCollection, AsyncIOMotorClient

from api.entity.entity_movie import MovieEntityV2

kafka_host = '127.0.0.1:9092'
kafka_file_topic = 'file_updater_topic'
kafka_file_group = 'file_updater_group'
kafka_db_topic = 'db_updater_topic'
kafka_db_group = 'db_updater_group'
kafka_producer: KafkaProducer = KafkaProducer(bootstrap_servers=kafka_host)

client = AsyncIOMotorClient("mongodb://localhost:27017/")  # 端
db = client["movie_repository"]  # 库
collections: AsyncIOMotorCollection = db[MovieEntityV2.collection]  # 表

# kafka_producer.send(kafka_file_topic, json.dumps({"demo file": "demo"}).encode('utf-8'))
# kafka_producer.send(kafka_file_topic, json.dumps({"demo file 1": "demo"}).encode('utf-8'))
# kafka_producer.send(kafka_db_topic, json.dumps({"demo db": "demo1"}).encode('utf-8'))
# kafka_producer.send(kafka_db_topic, json.dumps({"demo db 1": "demo1"}).encode('utf-8'))
kafka_producer.flush()
kafka_producer.close()


# 独立的消费逻辑
async def consume_file_messages():
    kafka_file_consumer = KafkaConsumer(
        kafka_file_topic,
        bootstrap_servers=kafka_host,
        group_id=kafka_file_group,
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        consumer_timeout_ms=5000  # 增加超时时间
    )
    for msg in kafka_file_consumer:
        print(msg)
        kafka_file_consumer.commit()
        print(msg.offset)
    kafka_file_consumer.close()


async def consume_db_messages():
    kafka_db_consumer = KafkaConsumer(
        kafka_db_topic,
        bootstrap_servers=kafka_host,
        group_id=kafka_db_group,
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        consumer_timeout_ms=5000  # 增加超时时间
    )
    for msg in kafka_db_consumer:
        print(msg)
        kafka_db_consumer.commit()
        print(msg.offset)
    kafka_db_consumer.close()


def run_asyncio_loop(asyncio_function):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(asyncio_function)
    loop.close()


# 启动线程来消费消息
file_thread = threading.Thread(target=run_asyncio_loop, args=(consume_file_messages(),))
db_thread = threading.Thread(target=run_asyncio_loop, args=(consume_db_messages(),))

file_thread.start()
db_thread.start()

file_thread.join()
db_thread.join()
