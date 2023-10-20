from websockets.sync.client import connect
import json
import time

from kafka import KafkaConsumer


def consumer_start(topic_name="btcusd"):
    consumer = KafkaConsumer(topic_name, auto_offset_reset='earliest', bootstrap_servers=['localhost:9092'], api_version=(0, 10), consumer_timeout_ms=1000)
    maxs = []
    n = 0
    for msg in consumer:
        time.sleep(0.1)
        record = json.loads(msg.value)
        price = 0 if float(record["data"]['price']) == 999999999.0 else float(record["data"]['price'])
        maxs.append(price)
        maxs.sort(reverse=True)
        maxs = maxs[:10]
        print(maxs)
        n += 1
    if consumer is not None:
        consumer.close()
    print(maxs)
    print(n)
