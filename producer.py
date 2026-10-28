"""
Kafka Producer Template for Streaming Data Dashboard
STUDENT PROJECT: Big Data Streaming Data Producer

Weather data source: Open-Meteo Historical Weather API
Location: Quezon City, Philippines

The template structure is preserved while the data-generation
TODO is adapted to retrieve historical weather observations.
"""

import argparse
import json
import time
import random
import math
import requests
from datetime import datetime, timedelta
from typing import Dict, Any, List

# Kafka libraries
from kafka import KafkaProducer
from kafka.errors import KafkaError


class StreamingDataProducer:
    """
    Kafka producer that retrieves historical weather data
    and sends records to a Kafka topic.
    """

    def __init__(self, bootstrap_servers: str, topic: str):
        """
        Initialize Kafka producer configuration.

        Parameters:
        - bootstrap_servers: Kafka broker address
        - topic: Kafka topic to produce messages to
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic

        # Kafka producer configuration
        self.producer_config = {
            'bootstrap_servers': bootstrap_servers,
        }

        # Preserve the template's stateful data attributes
        self.sensor_states = {}
        self.time_counter = 0
        self.base_time = datetime.utcnow()

        # Weather API configuration
        self.weather_url = (
            "https://archive-api.open-meteo.com/v1/archive"
        )
        self.latitude = 14.6760
        self.longitude = 121.0437
        self.city = "Quezon City"

        # Cache API results so we do not request the API
        # every time a Kafka message is produced.
        self.weather_records = []
        self.weather_index = 0

        # Preserve the template's sensor metadata
        self.sensors = [
            {"id": "sensor_001", "location": "server_room_a",
             "type": "temperature", "unit": "celsius"},
            {"id": "sensor_002", "location": "server_room_b",
             "type": "temperature", "unit": "celsius"},
            {"id": "sensor_003", "location": "outdoor_north",
             "type": "temperature", "unit": "celsius"},
            {"id": "sensor_004", "location": "lab_1",
             "type": "humidity", "unit": "percent"},
            {"id": "sensor_005", "location": "lab_2",
             "type": "humidity", "unit": "percent"},
            {"id": "sensor_006", "location": "control_room",
             "type": "pressure", "unit": "hPa"},
            {"id": "sensor_007", "location": "factory_floor",
             "type": "pressure", "unit": "hPa"},
            {"id": "sensor_008", "location": "warehouse",
             "type": "temperature", "unit": "celsius"},
            {"id": "sensor_009", "location": "office_area",
             "type": "humidity", "unit": "percent"},
            {"id": "sensor_010", "location": "basement",
             "type": "pressure", "unit": "hPa"},
        ]

        # Preserve the template's metric configurations
        self.metric_ranges = {
            "temperature": {
                "min": -10, "max": 45,
                "daily_amplitude": 8,
                "trend_range": (-0.5, 0.5)
            },
            "humidity": {
                "min": 20, "max": 95,
                "daily_amplitude": 15,
                "trend_range": (-0.2, 0.2)
            },
            "pressure": {
                "min": 980, "max": 1040,
                "daily_amplitude": 5,
                "trend_range": (-0.1, 0.1)
            },
        }

        # Initialize Kafka producer
        try:
            self.producer = KafkaProducer(**self.producer_config)
            print(
                f"Kafka producer initialized for "
                f"{bootstrap_servers} on topic {topic}"
            )
        except Exception:
            print(
                f"ERROR: No Kafka brokers available at "
                f"{bootstrap_servers}"
            )
            self.producer = None
        except Exception as e:
            print(f"ERROR: Failed to initialize Kafka producer: {e}")
            self.producer = None

    def generate_sample_data(self) -> Dict[str, Any]:
        """
        Retrieve hourly historical weather data from Open-Meteo.

        The data is cached and cycled through to provide records
        to Kafka without making an API request for every message.

        Returned fields are compatible with the dashboard:
        timestamp, value, metric_type, and sensor_id.
        """

        # Fetch weather data only once per producer run.
        if not self.weather_records:
            # Use a date seven days in the past to allow for
            # the historical API's data availability delay.
            weather_date = (
                datetime.now().date() - timedelta(days=7)
            ).isoformat()

            params = {
                "latitude": self.latitude,
                "longitude": self.longitude,
                "start_date": weather_date,
                "end_date": weather_date,
                "hourly": "temperature_2m",
                "timezone": "Asia/Manila",
            }

            print(
                f"Fetching historical weather for {self.city} "
                f"on {weather_date}..."
            )

            response = requests.get(
                self.weather_url,
                params=params,
                timeout=30
            )
            response.raise_for_status()

            weather = response.json()
            hourly = weather.get("hourly", {})
            timestamps = hourly.get("time", [])
            temperatures = hourly.get("temperature_2m", [])

            if not timestamps or not temperatures:
                raise ValueError(
                    "Open-Meteo returned no hourly temperature data."
                )

            # Store the API records for reuse.
            self.weather_records = [
                {
                    "timestamp": timestamp,
                    "value": temperature,
                    "metric_type": "temperature",
                    "sensor_id": "quezon_city_weather",
                    "location": self.city,
                    "unit": "celsius",
                    "source": "Open-Meteo Historical Weather API",
                }
                for timestamp, temperature
                in zip(timestamps, temperatures)
                if temperature is not None
            ]

            if not self.weather_records:
                raise ValueError(
                    "No valid temperature readings were returned."
                )

            print(
                f"Loaded {len(self.weather_records)} hourly "
                f"weather records from Open-Meteo."
            )

        # Cycle through the cached historical observations.
        # Repeated cycles are replayed historical data, not new
        # weather measurements.
        data = self.weather_records[
            self.weather_index % len(self.weather_records)
        ].copy()

        self.weather_index += 1
        return data

    def serialize_data(self, data: Dict[str, Any]) -> bytes:
        """
        Serialize the data dictionary into JSON bytes for Kafka.
        """
        try:
            serialized_data = json.dumps(data).encode("utf-8")
            return serialized_data
        except Exception as e:
            print(f"Serialization error: {e}")
            return None

    def send_message(self, data: Dict[str, Any]) -> bool:
        """
        Send one serialized weather record to Kafka.
        """
        if not self.producer:
            print("ERROR: Kafka producer not initialized")
            return False

        serialized_data = self.serialize_data(data)

        if not serialized_data:
            print("ERROR: Serialization failed")
            return False

        try:
            future = self.producer.send(
                self.topic,
                value=serialized_data
            )
            future.get(timeout=10)

            print(
                f"Message sent successfully - "
                f"Topic: {self.topic}, Data: {data}"
            )
            return True

        except KafkaError as e:
            print(f"Kafka send error: {e}")
            return False
        except Exception as e:
            print(f"Unexpected error during send: {e}")
            return False

    def produce_stream(
        self,
        messages_per_second: float = 0.1,
        duration: int = None
    ):
        """
        Produce weather records at the requested rate.

        Parameters:
        - messages_per_second: messages sent per second
        - duration: total runtime in seconds; None means infinite
        """

        if messages_per_second <= 0:
            print("ERROR: Rate must be greater than zero.")
            self.close()
            return

        print(
            f"Starting producer: {messages_per_second} msg/sec "
            f"({1 / messages_per_second:.1f} second intervals), "
            f"duration: {duration or 'infinite'}"
        )

        start_time = time.time()
        message_count = 0

        try:
            while True:
                if (
                    duration is not None
                    and time.time() - start_time >= duration
                ):
                    print(f"Reached duration limit of {duration} seconds")
                    break

                # Retrieve and send the next weather record.
                data = self.generate_sample_data()
                success = self.send_message(data)

                if success:
                    message_count += 1

                    if message_count % 10 == 0:
                        print(f"Sent {message_count} messages...")

                time.sleep(1.0 / messages_per_second)

        except KeyboardInterrupt:
            print("\nProducer interrupted by user")
        except Exception as e:
            print(f"Streaming error: {e}")
        finally:
            self.close()
            print(
                f"Producer stopped. Total messages sent: "
                f"{message_count}"
            )

    def close(self):
        """Flush and close the Kafka producer."""
        if self.producer:
            try:
                self.producer.flush(timeout=10)
                self.producer.close()
                print("Kafka producer closed successfully")
            except Exception as e:
                print(f"Error closing Kafka producer: {e}")


def parse_arguments():
    """Configure command-line arguments for the producer."""

    parser = argparse.ArgumentParser(
        description="Kafka Historical Weather Data Producer"
    )

    parser.add_argument(
        "--bootstrap-servers",
        type=str,
        default="localhost:9092",
        help="Kafka broker address (default: localhost:9092)"
    )

    parser.add_argument(
        "--topic",
        type=str,
        default="weather-data",
        help="Kafka topic (default: weather-data)"
    )

    parser.add_argument(
        "--rate",
        type=float,
        default=0.1,
        help="Messages per second (default: 0.1)"
    )

    parser.add_argument(
        "--duration",
        type=int,
        default=None,
        help="Run duration in seconds (default: infinite)"
    )

    return parser.parse_args()


def main():
    """Initialize the producer and start the weather data stream."""

    print("=" * 60)
    print("OPEN-METEO WEATHER DATA PRODUCER")
    print("=" * 60)

    args = parse_arguments()

    producer = StreamingDataProducer(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic
    )

    try:
        producer.produce_stream(
            messages_per_second=args.rate,
            duration=args.duration
        )
    except Exception as e:
        print(f"Producer execution error: {e}")
    finally:
        print("Producer execution completed")


if __name__ == "__main__":
    main()