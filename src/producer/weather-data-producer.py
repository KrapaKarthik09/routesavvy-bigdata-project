from confluent_kafka import Producer
import json
import time
import requests

# Create a Kafka producer
p = Producer({'bootstrap.servers': 'localhost:9092'})

# Topic name
topic = 'weather-data'

# Define parameters for New York City
latitude = 40.7128
longitude = -74.0060

# Make direct request to Open-Meteo API (free, no API key required)
url = f"https://api.open-meteo.com/v1/forecast?latitude={latitude}&longitude={longitude}&current=temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation,weather_code&hourly=temperature_2m,precipitation,wind_speed_10m,visibility,cloud_cover&forecast_days=1"

try:
    # Fetch weather data
    response = requests.get(url)
    response.raise_for_status()
    weather_data = response.json()
    
    # Add processing metadata
    weather_data['producer_timestamp'] = time.time()
    weather_data['location_name'] = 'New York City'
    
    # Weather code descriptions for human-readable conditions
    weather_codes = {
        0: "Clear sky", 1: "Mainly clear", 2: "Partly cloudy", 3: "Overcast",
        45: "Fog", 48: "Depositing rime fog", 51: "Light drizzle", 53: "Moderate drizzle",
        55: "Dense drizzle", 56: "Light freezing drizzle", 57: "Dense freezing drizzle",
        61: "Slight rain", 63: "Moderate rain", 65: "Heavy rain", 66: "Light freezing rain",
        67: "Heavy freezing rain", 71: "Slight snow fall", 73: "Moderate snow fall",
        75: "Heavy snow fall", 77: "Snow grains", 80: "Slight rain showers",
        81: "Moderate rain showers", 82: "Violent rain showers", 85: "Slight snow showers",
        86: "Heavy snow showers", 95: "Thunderstorm", 96: "Thunderstorm with slight hail",
        99: "Thunderstorm with heavy hail"
    }
    
    # Add human-readable weather description
    if 'current' in weather_data and 'weather_code' in weather_data['current']:
        code = weather_data['current']['weather_code']
        weather_data['current']['weather_description'] = weather_codes.get(code, "Unknown")
    
    # Print information about the data being produced
    print(f'Producing weather data for New York City')
    if 'current' in weather_data and 'temperature_2m' in weather_data['current']:
        print(f'Current temperature: {weather_data["current"]["temperature_2m"]}°C')
    if 'current' in weather_data and 'weather_description' in weather_data['current']:
        print(f'Weather conditions: {weather_data["current"]["weather_description"]}')
    
    # Send to Kafka
    p.produce(topic, json.dumps(weather_data))
    
    # Ensure the message is sent
    p.flush()
    print(f"Published weather data to topic: {topic}")
    
except requests.exceptions.RequestException as e:
    print(f"Error fetching weather data: {e}")
except Exception as e:
    print(f"Unexpected error: {e}")
