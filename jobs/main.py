import os
from confluent_kafka import SerializingProducer
import json
from datetime import datetime, timedelta
import logging
import random
import uuid
import time


logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%y-%m-%d %H:%M:%S",
    level=logging.DEBUG
)

HOCHIMINH_COORDINATES = {
    "lat": 10.762622,
    "lon": 106.660172
}
QUANGNAM_COORDINATES = {
    "lat": 15.5980,
    "lon": 107.8584
}

LAT_INCRE = (QUANGNAM_COORDINATES["lat"] - HOCHIMINH_COORDINATES["lat"]) / 100
LON_INCRE = (QUANGNAM_COORDINATES["lon"] - HOCHIMINH_COORDINATES["lon"]) / 100

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:29092")
VEHICLE_TOPIC = os.getenv("VEHICLE_TOPIC", "vehicle_data")
GPS_TOPIC = os.getenv("GPS_TOPIC", "gps_data")
TRAFFIC_TOPIC = os.getenv("TRAFFIC_TOPIC", "traffic_data")
WEATHER_TOPIC = os.getenv("WEATHER_TOPIC", "weather_data")
EMERGENCY_TOPIC = os.getenv("EMERGENCY_TOPIC", "emergency_data")

random.seed(42)
start_time = datetime.now()
start_location = HOCHIMINH_COORDINATES.copy()


def get_next_timestamp():
    global start_time
    start_time += timedelta(seconds=random.randint(30, 60))
    return start_time


def simulate_vehicle_movement():
    global start_location

    start_location["lat"] += LAT_INCRE
    start_location["lon"] += LON_INCRE

    start_location["lat"] += random.uniform(-0.0005, 0.0005)
    start_location["lon"] += random.uniform(-0.0005, 0.0005)

    return start_location


def generate_vehicle_data(device_id):
    location = simulate_vehicle_movement()
    return {
        "id": uuid.uuid4(),
        "device_id": device_id,
        "timestamp": get_next_timestamp().isoformat(),
        "location": (location["lat"], location["lon"]),
        "speed": random.uniform(10, 40),
        "direction": "North-East",
        "make": "Toyota",
        "model": "X5",
        "year": 2025,
        "fuel_type": "Hybrid"
    }


def generate_gps_data(device_id, timestamp, vehicle_type="private"):
    return {
        "id": uuid.uuid4(),
        "device_id": device_id,
        "timestamp": timestamp,
        "speed": random.uniform(10, 40),
        "direction": "North-East",
        "vehicle_type": vehicle_type
    }


def generate_traffic_camera_data(device_id, timestamp, location, camera_id):
    return {
        "id": uuid.uuid4(),
        "device_id": device_id,
        "camera_id": camera_id,
        "timestamp": timestamp,
        "location": location,
        "snapshot": "base64_encoded_image"
    }


def generate_weather_data(device_id, timestamp, location):
    return {
        "id": uuid.uuid4(),
        "device_id": device_id,
        "timestamp": timestamp,
        "location": location,
        "temperature": random.uniform(19, 35),
        "weather_condition": random.choice(["Sunny", "Cloudy", "Rain"]),
        "precipitation": random.uniform(0, 25),
        "wind_speed": random.uniform(0, 100),
        "humidity": random.randint(0, 100),
        "air_quality_index": random.uniform(0, 500)
    }


def generate_emergency_incident_data(device_id, timestamp, location):
    return {
        "id": uuid.uuid4(),
        "device_id": device_id,
        "incident_id": uuid.uuid4(),
        "type": random.choice(["Accident", "Fire", "Medical", "Policy", "None"]),
        "timestamp": timestamp,
        "location": location,
        "status": random.choice(["Active", "Resolved"]),
        "description": "Description of the incident."
    }


def json_serializer(obj):
    if isinstance(obj, uuid.UUID):
        return str(obj)
    raise TypeError(f"Object of type {obj.__class__.__name__} is not json serializable.")


def delivery_report(err, msg):
    if err:
        logging.info(f"Message delivery failed: {err}.")
    else:
        logging.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")


def produce_data_to_kafka(producer, topic, data):
    try:
        producer.produce(
            topic,
            key=str(data["id"]),
            value=json.dumps(data, default=json_serializer).encode("utf-8"),
            on_delivery=delivery_report
        )
    except Exception as e:
        logging.error(f"Failed to produce data to {topic}: {e}", exc_info=True)
    producer.flush()


def simulate_journey(producer, device_id):
    while True:
        vehicle_data = generate_vehicle_data(device_id)
        gps_data = generate_gps_data(device_id, vehicle_data["timestamp"])
        traffic_data = generate_traffic_camera_data(device_id, vehicle_data["timestamp"], vehicle_data["location"], "Nikon")
        weather_data = generate_weather_data(device_id, vehicle_data["timestamp"], vehicle_data["location"])
        emergency_incident_data = generate_emergency_incident_data(device_id, vehicle_data["timestamp"], vehicle_data["location"])

        if (vehicle_data["location"][0] >= QUANGNAM_COORDINATES["lat"]) and (vehicle_data["location"][1] >= QUANGNAM_COORDINATES["lon"]):
            logging.info("Vehicle has reached Quang Nam. Simulation ends.")
            break

        produce_data_to_kafka(producer, VEHICLE_TOPIC, vehicle_data)
        produce_data_to_kafka(producer, GPS_TOPIC, gps_data)
        produce_data_to_kafka(producer, TRAFFIC_TOPIC, traffic_data)
        produce_data_to_kafka(producer, WEATHER_TOPIC, weather_data)
        produce_data_to_kafka(producer, EMERGENCY_TOPIC, emergency_incident_data)

        time.sleep(5)


if __name__ == "__main__":
    producer_config = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "error_cb": lambda err: print(f"Error: {err}")
    }
    producer = SerializingProducer(producer_config)

    try:
        simulate_journey(producer, "Vehicle")
    except KeyboardInterrupt:
        logging.info("Simulation ended by the user.")
    except Exception as e:
        logging.error(f"Simulation ended with unexpected error: {e}", exc_info=True)


# docker exec -it smartcity-spark-master-1 python jobs/main.py