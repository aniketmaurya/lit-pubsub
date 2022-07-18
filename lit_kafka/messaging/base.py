import json
from typing import Callable, Optional


class BaseMessaging:
    def __init__(self, pub_topic: str, sub_topic: str, project: Optional[str] = None):
        """
        Create a consumer and publisher with topic and Project.
        Args:
            pub_topic: Topic where msg is published
            sub_topic: Topic from where msg is consumed
            project: Project id is required for GCP
        """
        self.pub_topic = pub_topic
        self.sub_topic = sub_topic
        self.project = project
        self._publisher = None
        self._consumer = None

    def _create_pubsub(self):
        """Create either kafka or GCP PubSub consumer & publisher"""

    def _to_json(self, data: dict) -> str:
        return json.dumps(data)

    def send_msg(self, data: dict, **kwargs):
        """send msg to the pub topic"""

    def receive_msg(self, **kwargs):
        """receive msg from the sub topic"""

    def consumer_loop(self, process_msg: Callable):
        """Define async consumer loop"""
