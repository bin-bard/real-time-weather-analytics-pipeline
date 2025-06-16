# weather_producer.py

import requests
import json
import time
from kafka import KafkaProducer

API_KEY = '652c9e80fb464582b3e135206252505'

CITY = 'Hanoi'
KAFKA_TOPIC = 'test'
# VMWare
# KAFKA_BROKER = 'localhost:9092'

# Databricks
KAFKA_BROKER = '0.tcp.ap.ngrok.io:12442'


producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def get_weather():
    url = f"https://api.weatherapi.com/v1/current.json?key={API_KEY}&q={CITY}&aqi=no"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        current = data['current']
        location = data['location']
        return {
            "time": location['localtime'].split(' ')[1],
            "month": int(location['localtime'].split('-')[1]),
            "year": int(location['localtime'].split('-')[0]),
            "temperature": current['temp_c'],
            "feelslike": current['feelslike_c'],
            "wind": current['wind_kph'],
            "direction": current['wind_dir'],
            "gust": current['gust_kph'],
            "cloud": current['cloud'],
            "humidity": current['humidity'],
            "precipitation": current['precip_mm'],
            "pressure": current['pressure_mb'],
            "weather": current['condition']['text'],
            "label": current['condition']['text']
        }
    else:
        print("Error:", response.status_code)
        return None

while True:
    weather_data = get_weather()
    if weather_data:
        producer.send(KAFKA_TOPIC, value=weather_data)
        print("Sent:", weather_data)
    time.sleep(10)  # Gửi mỗi 10 giây
