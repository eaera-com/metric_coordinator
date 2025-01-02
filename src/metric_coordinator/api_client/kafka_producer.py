from confluent_kafka import Producer
from types import Literal, Type, Dict, List
import pandas as pd

from account_metrics.configs import settings
from account_metrics.model import DataEmiter, MetricData


class KafkaProducer(DataEmiter):
    # Seperate kafka api and KafkaEmit
    def __init__(self) -> None:
        print("Setting up kafka producer")
        self.client = Producer(
            {
                "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS_URL,
                "client.id": settings.KAFKA_CLIENT_ID,
                "security.protocol": "SSL",
                "enable.ssl.certificate.verification": "false",
                "ssl.ca.pem": settings.KAFKA_PRODUCER_SSL_CA,
                "ssl.certificate.pem": settings.KAFKA_PRODUCER_SSL_CERTIFICATE,
                "ssl.key.pem": settings.KAFKA_PRODUCER_SSL_KEY,
            }
        )
        self.prefix = settings.KAFKA_TOPIC_PREFIX
        print("Kafka producer setup complete")

    def flush(self) -> None:
        self.kafka_producer.client.flush()

    def emit(self, data: Dict[MetricData, pd.DataFrame]) -> None:
        for metric in data.keys():
            topic = f"{self.prefix}{metric.__name__}"
            for _, row in data[metric].iterrows():
                self.kafka_producer.client.produce(topic, row.to_json().encode("utf-8"))
            print(f"Produced {data[metric].shape[0]} rows into {topic}")
        self.flush()
