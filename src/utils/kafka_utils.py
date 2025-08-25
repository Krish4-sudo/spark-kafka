from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaConsumer
import json
import os


def create_topic():
    """Create Kafka topic if it doesn't exist"""
    admin_client = KafkaAdminClient(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    )

    topic_list = [
        NewTopic(
            name=os.getenv("KAFKA_TOPIC", "weather-data"),
            num_partitions=3,
            replication_factor=1,
        )
    ]

    try:
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        print("Topic created successfully")
    except Exception as e:
        print(f"Topic creation failed or already exists: {e}")


def test_consumer():
    """Test Kafka consumer to verify data flow"""
    consumer = KafkaConsumer(
        os.getenv("KAFKA_TOPIC", "weather-data"),
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        consumer_timeout_ms=10000,
    )

    print("Testing Kafka consumer...")
    for message in consumer:
        print(f"Received: {message.value}")
        break

    consumer.close()


if __name__ == "__main__":
    create_topic()
    test_consumer()
