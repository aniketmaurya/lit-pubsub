from typing import Optional, List

import lightning as L
from lightning.app.structures import List as LightningList

from lit_kafka.messaging import KafkaWork


class KafkaRootFlow(L.LightningFlow):
    def __init__(
        self,
        pub_topic: str,
        sub_topic: str,
        bootstrap_servers: str,
        project: Optional[str] = None,
        group_id: str = "lit_kafka",
        auto_offset: str = "earliest",
        num_partitions: int = 1,
    ):
        super().__init__()
        kafka_works = []
        for _ in range(num_partitions):
            kafka_works.append(
                KafkaWork(
                    pub_topic,
                    sub_topic,
                    bootstrap_servers,
                    project=project,
                    group_id=group_id,
                    auto_offset=auto_offset,
                )
            )

        self.kafka_works: List[L.LightningWork] = LightningList(*kafka_works)

    def run(self, *args, **kwargs) -> None:
        for work in self.kafka_works:
            work.run()
