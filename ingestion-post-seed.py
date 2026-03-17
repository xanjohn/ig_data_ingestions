from kafka import KafkaConsumer
import json
from conn import get_db_connection
from datetime import datetime

conn = get_db_connection()
cur = conn.cursor()


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
        # print(message.value)
        data = message.value
        sections = data.get('raw', {}).get('media_grid', {}).get('sections', [])
        
        for section in sections:
            layout_content = section.get('layout_content', {})
            
            media_items = []
            
            if 'one_by_two_item' in layout_content:
                clips = layout_content['one_by_two_item'].get('clips', {}).get('items', [])
                media_items.extend(clips)
            
            if 'fill_items' in layout_content:
                media_items.extend(layout_content['fill_items'])
                
            for item in media_items:
                media = item.get('media', {})
                if not media:
                    continue
                
                #Microsecond
                device_timestamp = datetime.fromtimestamp(float(media.get('device_timestamp')) / 1000000).strftime('%Y-%m-%d %H:%M:%S')
                
                #Seconds
                created_at = datetime.fromtimestamp(float(media.get('taken_at'))).strftime('%Y-%m-%d %H:%M:%S')
                edited_at = datetime.fromtimestamp(float(media.get('edited_at'))) .strftime('%Y-%m-%d %H:%M:%S') if media.get('edited_at') else None
                data_tuple = (
                    str(media.get('pk')),
                    'instagram',
                    media.get('code'),
                    device_timestamp,
                    created_at,                    
                    edited_at,
                    'pending',
                    None,
                )
                buffer_data.append(data_tuple)
                
                print(f"Buffer: {len(buffer_data)}/{batch_size}", end='\r')
                
                if len(buffer_data) >= batch_size:
                    query = """
                        INSERT INTO post_link_seeds 
                        (original_id, social_media, alternative_original_id, timestamp, created_at, updated_at, status, error) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE updated_at = VALUES(updated_at)
                    """
                    cur.executemany(query, buffer_data)
                    conn.commit()
                    consumer.commit()
                    print(f"\n Success inserting {len(buffer_data)} data!")
                    buffer_data = []

except KeyboardInterrupt:
    print("\nStopping...")
    if buffer_data:
        cur.executemany(query, buffer_data)
        conn.commit()
        consumer.commit()
finally:
    cur.close()
    conn.close()