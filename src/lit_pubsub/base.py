import json
from typing import Any, Callable, Optional


class BaseMessaging:
    def __init__(
        self,
        sub_topic: str,
        pub_topic: Optional[str] = None,
        project: Optional[str] = None,
    ):
        """
        Create a consumer and publisher with topic and Project.
        Args:
            sub_topic: Topic from where msg is consumed
            project: Project id is required for GCP
        """
        self.pub_topic = pub_topic
        self.sub_topic = sub_topic
        self.project = project
        self._publisher: Any = None
        self._consumer: Any = None
        self._executor: Any = None

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

    def done_callback(self, future):
        print(f"{future.result()} completed")

    def async_process(self, msg, func):
        future = self._executor.submit(func, msg)
        future.add_done_callback(self.done_callback)
        return future
