from websockets.sync.client import connect
import json
import time

from kafka import KafkaProducer

def publish_message(producer_instance, topic_name, key, value):
    try:
        key_bytes = bytes(key, encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
    except Exception as ex:
        print('message publishing error!')
        print(str(ex))


def connect_kafka_producer():
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10))
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer


server_ip = "wss://ws.bitstamp.net"

channel_name = "live_orders_btcusd"

subscription_msg = {
    "event": "bts:subscribe",
    "data": {
        "channel": channel_name
    }
}

unsubscription_msg = {
    "event": "bts:unsubscribe",
    "data": {
        "channel": channel_name
    }
}

def producer_start(transactions_number=30, topic_name="btcusd", key="raw"):
    with connect(server_ip) as websocket:
        websocket.send(json.dumps(subscription_msg))
        message = websocket.recv()
        print(f"Received: {message}")

        print("_" * 30)
        kafka_producer = connect_kafka_producer()

        for num in range(transactions_number):
            time.sleep(0.1)
            message = websocket.recv()
            print(f"{num}: {message}")
            publish_message(kafka_producer, topic_name, key, message)
        print("_" * 30)
        if kafka_producer is not None:
            kafka_producer.close()

        websocket.send(json.dumps(unsubscription_msg))
