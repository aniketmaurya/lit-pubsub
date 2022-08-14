import lightning as L

from lit_pubsub.lit_kafka import KafkaRootFlow

if __name__ == "__main__":
    app = L.LightningApp(
        KafkaRootFlow(
            "kafka-test", bootstrap_servers="localhost:9092", num_partitions=2
        )
    )
