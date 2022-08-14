import json
import random
import socket
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Optional

import lightning as L
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from loguru import logger

from lit_pubsub.base import BaseMessaging


class Kafka(BaseMessaging):
    def __init__(
        self,
        sub_topic: str,
        bootstrap_servers: str,
        pub_topic: Optional[str] = None,
        project: Optional[str] = None,
        group_id: str = "lit_kafka",
        auto_offset: str = "earliest",
    ):
        super().__init__(sub_topic, pub_topic, project)
        self._consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": group_id,
                "auto.offset.reset": auto_offset,
            }
        )
        self._consumer.subscribe([self.sub_topic])
        self._publisher = Producer(
            {"bootstrap.servers": bootstrap_servers, "client.id": socket.gethostname()}
        )
        self._executor = None

    def _delivery_report(self, err, msg):
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.info(f"Message delivered successfully")

    def send_msg(self, data: dict, **_):
        """send msg to the pub topic"""
        msg = self._to_json(data).encode("utf-8")
        self._publisher.produce(self.pub_topic, msg, callback=self._delivery_report)
        self._publisher.flush()

    def receive_msg(self, timeout: float = 0.2):
        """receive msg from the sub topic"""
        msg = self._consumer.poll(timeout=timeout)
        decoded_msg = None
        if msg is None:
            return decoded_msg
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event
                logger.error(
                    "%% %s [%d] reached end at offset %d\n"
                    % (msg.topic(), msg.partition(), msg.offset())
                )
            elif msg.error():
                raise KafkaException(msg.error())
        else:
            decoded_msg = json.loads(msg.value().decode("utf-8"))
        return decoded_msg

    def consumer_loop(self, process_msg: Callable, max_workers=None):
        """"""
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        try:
            self._consumer.subscribe([self.sub_topic])
            while True:
                msg = self.receive_msg()
                if msg:
                    self.async_process(msg, process_msg)
                time.sleep(0.1)
        finally:
            # Close down consumer to commit final offsets.
            self._consumer.close()
            self._executor.shutdown(wait=False)


class KafkaWork(L.LightningWork):
    def __init__(
        self,
        sub_topic: str,
        bootstrap_servers: str,
        project: Optional[str] = None,
        group_id: str = "lit_kafka",
        auto_offset: str = "earliest",
    ):
        super().__init__()
        self.sub_topic = sub_topic
        self.bootstrap_servers = bootstrap_servers
        self.project = project
        self.group_id = group_id
        self.auto_offset = auto_offset
        self.randname = f"{random.randint(1, 100)}"

    def process_msg(self, msg):
        print(f"{self.randname} implement this method to process the kafka {msg}")

    def run(self, *args, **kwargs):
        kafka = Kafka(
            self.sub_topic,
            bootstrap_servers=self.bootstrap_servers,
            project=self.project,
            group_id=self.group_id,
            auto_offset=self.auto_offset,
        )
        kafka.consumer_loop(self.process_msg)
