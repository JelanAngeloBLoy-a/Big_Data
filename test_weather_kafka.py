
import json
import requests
from kafka import KafkaProducer

# 1. Get historical temperature data from Open-Meteo
url = "https://archive-api.open-meteo.com/v1/archive"

params = {
    "latitude": 14.6760,
    "longitude": 121.0437,
    "start_date": "2025-10-20",
    "end_date": "2025-10-20",
    "hourly": "temperature_2m",
    "timezone": "Asia/Manila"
}

response = requests.get(url, params=params, timeout=30)
response.raise_for_status()
weather = response.json()

# 2. Prepare one temperature record
record = {
    "city": "Quezon City",
    "timestamp": weather["hourly"]["time"][0],
    "temperature_c": weather["hourly"]["temperature_2m"][0],
    "source": "Open-Meteo Historical Weather API"
}

print("Temperature record:", record)

# 3. Send the record to Kafka
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)

try:
    producer.send("weather-data", record).get(timeout=30)
    producer.flush()
    print("Success! Temperature record sent to Kafka.")
finally:
    producer.close()
