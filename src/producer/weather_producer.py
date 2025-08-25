import os
import json
import time
import requests
import schedule
from kafka import KafkaProducer
from datetime import datetime
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class WeatherProducer:
    def __init__(self):
        self.api_key = os.getenv(
            "OPENWEATHER_API_KEY", "729a8316167da8d34d39fbb1f9b7b053"
        )
        self.kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.topic = os.getenv("KAFKA_TOPIC", "weather-data")
        self.cities = os.getenv("CITIES", "London,New York,Tokyo,Sydney,Mumbai").split(
            ","
        )

        # Initialize Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=self.kafka_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
        )

        logger.info(f"Weather Producer initialized for cities: {self.cities}")

    def fetch_weather_data(self, city):
        """Fetch weather data for a specific city"""
        try:
            url = "https://api.openweathermap.org/data/2.5/weather"
            params = {"q": city, "appid": self.api_key, "units": "metric"}

            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()

            data = response.json()

            # Transform and enrich data
            weather_record = {
                "timestamp": datetime.utcnow().isoformat(),
                "city": data["name"],
                "country": data["sys"]["country"],
                "temperature": data["main"]["temp"],
                "feels_like": data["main"]["feels_like"],
                "humidity": data["main"]["humidity"],
                "pressure": data["main"]["pressure"],
                "description": data["weather"][0]["description"],
                "wind_speed": data["wind"]["speed"],
                "wind_direction": data["wind"].get("deg", 0),
                "cloudiness": data["clouds"]["all"],
                "visibility": data.get("visibility", 0) / 1000,  # Convert to km
                "latitude": data["coord"]["lat"],
                "longitude": data["coord"]["lon"],
            }

            return weather_record

        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching data for {city}: {e}")
            return None
        except KeyError as e:
            logger.error(f"Missing data field for {city}: {e}")
            return None

    def produce_weather_data(self):
        """Fetch and produce weather data for all cities"""
        logger.info("Starting weather data collection...")

        for city in self.cities:
            city = city.strip()
            weather_data = self.fetch_weather_data(city)

            if weather_data:
                try:
                    # Send to Kafka with city as key for partitioning
                    self.producer.send(self.topic, key=city, value=weather_data)
                    logger.info(f"Sent weather data for {city}")

                except Exception as e:
                    logger.error(f"Error sending data for {city}: {e}")

            # Small delay between API calls to respect rate limits
            time.sleep(1)

        # Ensure all messages are sent
        self.producer.flush()
        logger.info(f"Weather data collection completed for {len(self.cities)} cities")

    def start_scheduler(self):
        """Start the scheduled data collection"""
        # Run immediately on start
        self.produce_weather_data()

        # Schedule every 5 minutes
        schedule.every(5).seconds.do(self.produce_weather_data)

        logger.info("Weather data scheduler started (5-minute intervals)")

        try:
            while True:
                schedule.run_pending()
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Shutting down weather producer...")
        finally:
            self.producer.close()


if __name__ == "__main__":
    producer = WeatherProducer()
    producer.start_scheduler()
