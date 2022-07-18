from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import KafkaError, NewTopic

TOPIC = "kafka-test"
bootstrap_servers = "localhost:9092"
num_partitions = 2
replication_factor = 1
conf = {"bootstrap.servers": bootstrap_servers}
admin = AdminClient(conf)

if __name__ == "__main__":
    new_topic = NewTopic(TOPIC, num_partitions=num_partitions, replication_factor=replication_factor)
    fs = admin.create_topics(new_topics=[new_topic])
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print(f"Topic {topic} created")
        except KafkaError as e:
            print("Topic already exists", e)
        except Exception as e:
            print(f"Failed to create topic {topic}: {e}")

    print("created=", TOPIC + "-request" in admin.list_topics().topics)
