import json, time, random
from faker import Faker
from kafka import KafkaProducer
from decimal import Decimal

fake = Faker()

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

while True:
    message = {
        "vehicle_id": fake.uuid4(),
        "route_id": f"route_{random.randint(1,20)}",
        "timestamp": int(time.time()),
        "lat": float(fake.latitude()),   
        "lon": float(fake.longitude()), 
        "speed_kmh": random.randint(0,80),
        "delay_seconds": random.randint(-60, 600),
        "occupancy": random.randint(0,50)
    }

    producer.send('vehicle_telemetry', value=message)
    print(f"Sent: {message}")
    time.sleep(3)