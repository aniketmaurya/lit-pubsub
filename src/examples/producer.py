import json
import socket

from confluent_kafka import Producer
from loguru import logger

TOPIC = "kafka-test"
bootstrap_servers = "localhost:9092"
conf = {"bootstrap.servers": bootstrap_servers, "client.id": socket.gethostname()}

producer = Producer(conf)


def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Message delivery failed: {err}")


def send_msg(msg: dict, topic=None):
    topic = topic or TOPIC
    msg = json.dumps(msg).encode("utf-8")

    producer.produce(topic, msg, callback=delivery_report)
    producer.poll(1)


if __name__ == '__main__':
    msg = {"msg": "hi"}
    send_msg(msg, topic=TOPIC)
