from kafka import KafkaProducer
import json
import time
import random
import uuid


with open('ig-raw.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

#Kafka Producer
producer = KafkaProducer(
    'ingestion-pipeline_instagram_mobile_raw',
    bootstrap_servers=[
        '10.27.10.29:9092', 
        '10.27.10.43:9092', 
        '10.27.10.145:9092'
    ],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def send_location_updates():
    while True:
        # location = {
        #     "driver_id": str(uuid.uuid4()),
        #     "latitude": round(random.uniform(40.0, 41.0), 6),
        #     "longitude": round(random.uniform(-74.0, -73.0), 6),
        #     "timestamp": time.time()
        # }
        producer.send('ingestion-ig', data)
        print(f"Sent: {data}")
        time.sleep(10)

send_location_updates()