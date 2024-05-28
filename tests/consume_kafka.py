from kafka import KafkaConsumer

kafka_host = 'localhost:9092'
kafka_file_topic = 'file_updater_topic'
kafka_file_group = 'file_updater_group'
kafka_consumer: KafkaConsumer = KafkaConsumer(kafka_file_topic,
                                              bootstrap_servers=kafka_host,
                                              group_id=kafka_file_group,
                                              consumer_timeout_ms=1000)

counter = 0
for msg in kafka_consumer:
    counter += 1

print(f'consume {counter} messages')
