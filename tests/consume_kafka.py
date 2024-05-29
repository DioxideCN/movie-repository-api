import json

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

kafka_host = 'localhost:9092'
kafka_file_topic = 'file_updater_topic'
kafka_file_group = 'file_updater_group'
kafka_db_topic = 'db_updater_topic'
kafka_db_group = 'db_updater_group'
kafka_producer: KafkaProducer = KafkaProducer(bootstrap_servers=kafka_host)
kafka_file_consumer: KafkaConsumer = KafkaConsumer(kafka_file_topic,
                                                   bootstrap_servers=kafka_host,
                                                   group_id=kafka_file_group,
                                                   auto_offset_reset='earliest',
                                                   enable_auto_commit=True,
                                                   consumer_timeout_ms=1000)
kafka_db_consumer: KafkaConsumer = KafkaConsumer(kafka_db_topic,
                                                 bootstrap_servers=kafka_host,
                                                 group_id=kafka_db_group,
                                                 auto_offset_reset='earliest',
                                                 enable_auto_commit=True,
                                                 consumer_timeout_ms=1000)


# movie_dict = {'_id': 'cc7d7b82b3d5ed32261256c449539a3d1b318b37e3fae291ef0c45ca1361e185', 'create_time': '2024-05-29 15:46:00.793', 'update_time': '2024-05-29 15:46:00.793', 'fixed_title': '只是一次偶然的旅行', 'platform_detail': [{'source': 'mgtv', 'title': '只是一次偶然的旅行', 'cover_url': 'https://2img.hitv.com/preview/sp_images/2020/12/23/20201223104222045.jpg', 'create_time': '2024-05-29 15:46:00.793', 'directors': [], 'actors': ['窦靖童', '贺开朗'], 'release_date': '2021', 'description': '女孩（窦靖童饰）独自来到拉萨旅行，而一只被供奉的“五彩神虾”意外改变了她的所有计划。女孩决定独自驾车穿越半个中国将神虾送归大海。一路上，她遇到了形色各异的人和事，回忆、梦境与幻觉也断续涌现在她眼前。最终，放虾之旅以意想不到的方式落幕，女孩也抵达了她内心最隐秘的世界。', 'score': 0.0, 'movie_type': ['剧情', '文艺', '公路'], 'metadata': {'clip_id': '356373', 'views': '602581', 'update_time': '2023-08-18 19:29:42'}}]}
# future = kafka_producer.send(kafka_db_topic, json.dumps(movie_dict).encode('utf-8'))
# try:
#     record_metadata = future.get(timeout=10)
#     print(f"Message sent to topic {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}")
# except KafkaError as e:
#     print(f"Failed to send message: {e}")

# counter = 0
# for msg in kafka_file_consumer:
#     counter += 1
# print(f'consume {counter} file messages')
# kafka_file_consumer.close()
#
counter = 0
for msg in kafka_db_consumer:
    counter += 1
print(f'consume {counter} db messages')
kafka_db_consumer.close()
