# config.py - Constants for Urban Mobility Optimizer

KAFKA = {
    'BOOTSTRAP_SERVERS': 'broker:9093',
    'TOPICS': {
        'SERVICE_ALERTS': 'mta-service-alerts',
        'SUBWAY_DATA': 'mta-subway-data', 
        'TRAFFIC_DATA': 'nyc-traffic-data',
        'WEATHER_DATA': 'weather-data'
    },
    'START_OFFSETS': 'earliest'
}

MONGODB = {
    'URI': 'mongodb://mongo:27017',
    'DATABASE': 'urban_mobility',
    'COLLECTIONS': {
        'SUBWAY_STATIONS': 'subway_stations',
        'TRANSIT_ALERTS': 'transit_alerts',
        'ROAD_TRAFFIC': 'road_traffic',
        'WEATHER_CONDITIONS': 'weather_conditions',
        'ROUTE_OPTIMIZATION': 'route_optimization'
    }
}

CHECKPOINTS = {
    'BASE_PATH': '/tmp/checkpoint',
    'SUBWAY_NODES': '/tmp/checkpoint/subway_nodes',
    'SERVICE_ALERTS': '/tmp/checkpoint/service_alerts',
    'TRAFFIC_DATA': '/tmp/checkpoint/traffic_data',
    'WEATHER_DATA': '/tmp/checkpoint/weather_data',
    'INTEGRATED': '/tmp/checkpoint/integrated'
}

STREAMING = {
    'WATERMARKS': {
        'ALERTS': '1 hour',
        'TRAFFIC': '15 minutes',
        'WEATHER': '30 minutes'
    },
    'TRIGGER_INTERVAL': '1 minute'
}

WEATHER = {
    'CODES': {
        'THUNDERSTORM': [95, 96, 99],
        'HEAVY_RAIN': [61, 63, 65, 80, 81, 82],
        'SNOW': [71, 73, 75, 85, 86],
        'FOG': [45, 48]
    },
    'IMPACT_FACTORS': {
        'THUNDERSTORM': 0.5,
        'HEAVY_RAIN': 0.4,
        'SNOW': 0.6,
        'FOG': 0.3
    },
    'PRECIPITATION_THRESHOLDS': {
        'HEAVY': 5.0,
        'MODERATE': 2.0,
        'LIGHT': 0.5
    },
    'PRECIPITATION_IMPACTS': {
        'HEAVY': 0.3,
        'MODERATE': 0.2,
        'LIGHT': 0.1
    },
    'VISIBILITY_THRESHOLDS': {
        'VERY_LOW': 1000,
        'LOW': 5000
    },
    'VISIBILITY_IMPACTS': {
        'VERY_LOW': 0.4,
        'LOW': 0.2
    }
}

TRAFFIC = {
    'CONGESTION_THRESHOLDS': {
        'SEVERE': 10,  # Speed < 10 = Level 3
        'MODERATE': 25,  # Speed < 25 = Level 2
        'LIGHT': 40     # Speed < 40 = Level 1
    },
    'CONGESTION_LEVELS': {
        'SEVERE': 3,
        'MODERATE': 2,
        'LIGHT': 1,
        'NORMAL': 0
    }
}

MOBILITY = {
    'ALERT_SCORE_PENALTY': 15,  # Points deducted per alert
    'BASE_SCORE': 100  # Perfect mobility score
}

GEOSPATIAL = {
    'EARTH_RADIUS_KM': 6371.0  # Earth's radius in kilometers
}
