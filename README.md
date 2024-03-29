<!---:lai-name: PubSub--->

<div align="center">
<img src="https://pl-bolts-doc-images.s3.us-east-2.amazonaws.com/lai.png" width="200px">

A Lightning component for GCP PubSub and Kafka messaging
______________________________________________________________________

![Tests](https://github.com/PyTorchLightning/LAI-slack-messenger/actions/workflows/ci-testing.yml/badge.svg)

</div>

# About

This component lets you publish and subscribe to Kafka and GCP PubSub events.


----

## Use the component

<!---:lai-use:--->

```python
import lightning as L

from lit_kafka import KafkaRootFlow


if __name__ == "__main__":
    app = L.LightningApp(
        KafkaRootFlow("kafka-test", bootstrap_servers="localhost:9092", num_partitions=2)
    )
```

## install

Use these instructions to install:

<!---:lai-install:--->

```bash
git clone https://github.com/PyTorchLightning/lightning-pubsub.git
cd lightning-lit_kafka
pip install -r requirements.txt
pip install -e .
```
