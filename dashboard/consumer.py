from kafka import KafkaConsumer
from json import loads
import random

consumer = KafkaConsumer(
    'source',
    bootstrap_servers=['kafka_kafkanet:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group' + str(random.randint(1, 100001)),
    value_deserializer=lambda x: loads(x.decode('utf-8')))

for message in consumer:
    print( message.value)