from kafka import KafkaProducer
from config import *
import json
from sseclient import SSEClient as EventSource

producer = KafkaProducer(bootstrap_servers=server_addr+':9092')
topic = topic_name

def send_message(message):
    producer.send(topic, message.encode('utf-8'))

def page_create():
    url = 'https://stream.wikimedia.org/v2/stream/page-create'
    for event in EventSource(url):
        if event.event == 'message':
            try:
                change = json.loads(event.data)
            except ValueError:
                pass
            else:
                if change['meta']['domain'] == 'canary':
                    continue
                date_hour = change['rev_timestamp'].replace('T', ' ').replace('Z', '')
                user = change['performer']['user_text']
                is_a_bot = change['performer']['user_is_bot']
                send_message('{} is {} at {}'.format(user,is_a_bot,date_hour))

page_create()