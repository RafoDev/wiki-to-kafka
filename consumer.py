from kafka import KafkaConsumer
from config import *

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers= server_addr + ':9092',
    auto_offset_reset='earliest'
)

for message in consumer:
    msg = message.value.decode('utf-8')
    print(msg)

consumer.close()
