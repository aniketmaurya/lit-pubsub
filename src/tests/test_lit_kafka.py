from lit_pubsub.lit_kafka import KafkaRootFlow


def test_kafka_root_flow():
    KafkaRootFlow("kafka-test", bootstrap_servers="localhost:9092", num_partitions=2)
