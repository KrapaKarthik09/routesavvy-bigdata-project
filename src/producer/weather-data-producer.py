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

# Define the date range
start_date = "2024-05-01"
end_date = "2024-05-31"

# Historical Weather API endpoint
url = f"https://archive-api.open-meteo.com/v1/archive?latitude={latitude}&longitude={longitude}&start_date={start_date}&end_date={end_date}&hourly=temperature_2m,precipitation,wind_speed_10m,visibility,cloud_cover,weather_code&timezone=America/New_York"

try:
    # Fetch historical weather data
    print(f"Fetching historical weather data from {start_date} to {end_date}...")
    response = requests.get(url)
    response.raise_for_status()
    weather_data = response.json()
    
    # Add processing metadata
    weather_data['producer_timestamp'] = time.time()
    weather_data['location_name'] = 'New York City'
    weather_data['query_period'] = f"{start_date} to {end_date}"
    
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
    
    # Add human-readable weather descriptions to hourly data if weather_code is present
    if 'hourly' in weather_data and 'weather_code' in weather_data['hourly']:
        weather_descriptions = []
        for code in weather_data['hourly']['weather_code']:
            if code is not None:
                code_int = int(code)
                weather_descriptions.append(weather_codes.get(code_int, "Unknown"))
            else:
                weather_descriptions.append("Unknown")
        weather_data['hourly']['weather_description'] = weather_descriptions
    
    # Preprocess to separate each hourly record into different messages
    hourly = weather_data.get('hourly', {})
    num_records = len(hourly.get('time', []))
    
    print(f'Processing {num_records} hourly records...')
    
    # Process and send each hourly record as a separate message
    for i in range(num_records):
        # Create individual message with metadata and hourly data
        message = {
            'latitude': weather_data['latitude'],
            'longitude': weather_data['longitude'],
            'elevation': weather_data.get('elevation'),
            'timezone': weather_data['timezone'],
            'timezone_abbreviation': weather_data.get('timezone_abbreviation'),
            'producer_timestamp': weather_data['producer_timestamp'],
            'location_name': weather_data['location_name'],
            'query_period': weather_data['query_period'],
            'time': hourly['time'][i],
            'temperature_2m': hourly['temperature_2m'][i],
            'precipitation': hourly['precipitation'][i],
            'wind_speed_10m': hourly['wind_speed_10m'][i],
            'visibility': hourly['visibility'][i] if 'visibility' in hourly else None,
            'cloud_cover': hourly['cloud_cover'][i],
            'weather_code': hourly['weather_code'][i],
            'weather_description': hourly['weather_description'][i]
        }
        
        # Send individual message to Kafka
        p.produce(topic, json.dumps(message))
        
        # Print progress every 100 records
        if (i + 1) % 100 == 0 or i == 0 or i == num_records - 1:
            print(f'Sent record {i + 1}/{num_records}: {message["time"]} - {message["weather_description"]}')
    
    # Ensure all messages are sent
    p.flush()
    print(f"Published {num_records} hourly weather records to topic: {topic}")
    
except requests.exceptions.RequestException as e:
    print(f"Error fetching weather data: {e}")
    print(f"Response content: {response.content if 'response' in locals() else 'No response'}")
except Exception as e:
    print(f"Unexpected error: {e}")
