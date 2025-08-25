import os
import requests
from kafka import KafkaProducer
import json


def test_api_connection():
    """Test OpenWeatherMap API connectivity"""
    api_key = os.getenv("OPENWEATHER_API_KEY")
    url = "https://api.openweathermap.org/data/2.5/weather?q=London&appid=729a8316167da8d34d39fbb1f9b7b053"

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        print("✓ API connection successful")
        return True
    except Exception as e:
        print(f"✗ API connection failed: {e}")
        return False


def test_kafka_connection():
    """Test Kafka connectivity"""
    try:
        producer = KafkaProducer(
            bootstrap_servers="localhost:9092",
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        producer.send("test-topic", {"test": "message"})
        producer.flush()
        producer.close()
        print("✓ Kafka connection successful")
        return True
    except Exception as e:
        print(f"✗ Kafka connection failed: {e}")
        return False


def run_tests():
    """Run all connectivity tests"""
    print("Running system tests...")
    api_ok = test_api_connection()
    kafka_ok = test_kafka_connection()

    if api_ok and kafka_ok:
        print("\n✓ All tests passed - system ready!")
    else:
        print("\n✗ Some tests failed - check configuration")


if __name__ == "__main__":
    run_tests()
