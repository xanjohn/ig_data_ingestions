from kafka import KafkaConsumer
import json
# from conn import get_db_connection

# conn = get_db_connection()
# cur = conn.cursor()


consumer = KafkaConsumer(
    'ingestion-pipeline_instagram_mobile_raw',
    bootstrap_servers=[
        '10.27.10.29:9092', 
        '10.27.10.43:9092', 
        '10.27.10.145:9092'
    ],
    auto_offset_reset='latest',
    enable_auto_commit=False,
    group_id='ingestion-instagram-mobile',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

buffer_data = [] 
batch_size = 10 

print(f"--- Waiting for data (Batch Size: {batch_size}) ---")

try:
    for message in consumer:
        print(message.value)
        # location = message.value
        
        # data_tuple = (
        #     location['driver_id'], 
        #     location['latitude'], 
        #     location['longitude'], 
        #     location['timestamp'],
        #     'Pending'
        # )
        # buffer_data.append(message)
        
        # print(f"Buffer: {len(buffer_data)}/{batch_size}", end='\r')

        # if len(buffer_data) >= batch_size:
        #     query = "INSERT INTO driver (driver_id, latitude, longitude, timestamp, status) VALUES (?, ?, ?, ?, ?)"
        #     cur.executemany(query, buffer_data)
        #     conn.commit()
        #     consumer.commit()
        #     print(f"\n[SUCCESS] Berhasil bulk insert {batch_size} data!")
        #     buffer_data = []

except KeyboardInterrupt:
    print("\nStopping...")
    if buffer_data:
        # cur.executemany(query, buffer_data)
        # conn.commit()
        consumer.commit()
finally:
    cur.close()
    conn.close()