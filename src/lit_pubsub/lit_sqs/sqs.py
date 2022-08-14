import json
import random
import socket
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Optional

import boto3
import lightning as L
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from loguru import logger

from pubsub.base import BaseMessaging


class SQS(BaseMessaging):
    def __init__(
        self,
        sub_topic: str,
        pub_topic: Optional[str] = None,
        project: Optional[str] = None,
    ):
        super().__init__(sub_topic, pub_topic, project)
        sqs = boto3.resource("sqs")
        self.queue = sqs.get_queue_by_name(QueueName="test")

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

    def consumer_loop(
        self,
        process_msg: Callable,
    ):
        """"""
        for msg in self.queue.receive_messages():
            self.async_process(msg, process_msg)


class SQSWork(L.LightningWork):
    def __init__(
        self,
        sub_topic: str,
        project: Optional[str] = None,
    ):
        super().__init__()
        self.sub_topic = sub_topic
        self.project = project
        self.randname = f"{random.randint(1, 100)}"

    def process_msg(self, msg):
        print(f"{self.randname} implement this method to process the kafka {msg}")

    def run(self, *args, **kwargs):
        sqs = SQS(
            self.sub_topic,
        )
        sqs.consumer_loop(self.process_msg)
