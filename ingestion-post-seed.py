import json
import argparse
import sys
from datetime import datetime
from kafka import KafkaConsumer
from conn import get_db_connection

def get_args():
    parser = argparse.ArgumentParser(description='Kafka Ingestion Worker for Instagram Data')
    parser.add_argument('--topic', type=str, default='ingestion-pipeline_instagram_mobile_raw', help='Nama Topic Kafka')
    parser.add_argument('--groupid', type=str, default='ingestion-instagram-mobile', help='Group ID Consumer')
    parser.add_argument('--batch', type=int, default=10, help='Jumlah data per sekali simpan ke DB')
    return parser.parse_args()

def format_ig_timestamp(ts):
    if ts is None or ts == 0 or ts == "":
        return None
    
    try:
        ts_str = str(ts)
        if len(ts_str) > 10:
            divisor = 10**(len(ts_str) - 10)
            ts_float = float(ts) / divisor
        else:
            ts_float = float(ts)
            
        return datetime.fromtimestamp(ts_float).strftime('%Y-%m-%d %H:%M:%S')
    except Exception:
        return None

def main():
    args = get_args()
    
    conn = get_db_connection()
    if not conn:
        print("Can't connect database. Cek file conn.py")
        return
    cur = conn.cursor()

    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=[
            '10.27.10.29:9092', 
            '10.27.10.43:9092', 
            '10.27.10.145:9092'
        ],
        auto_offset_reset='latest',
        enable_auto_commit=False, 
        group_id=args.groupid,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    buffer_data = [] 
    
    print(f"--- Ingestion Worker Started ---")
    print(f"Topic    : {args.topic}")
    print(f"Group ID : {args.groupid}")
    print(f"Batch    : {args.batch}")
    print(f"----------------------")

    try:
        for message in consumer:
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
                    

                    data_tuple = (
                        str(media.get('pk')),             # original_id
                        'instagram',                      # social_media
                        media.get('code'),                # alternative_original_id (shortcode)
                        format_ig_timestamp(media.get('device_timestamp')), 
                        format_ig_timestamp(media.get('taken_at')),         # created_at
                        None,                                            
                        'pending',                        # status
                        None                              # error
                    )
                    buffer_data.append(data_tuple)
                    
                    print(f"Buffer: {len(buffer_data)}/{args.batch}", end='\r')
                    
                    if len(buffer_data) >= args.batch:
                        query = """
                            INSERT INTO post_link_seeds 
                            (original_id, social_media, alternative_original_id, timestamp, created_at, updated_at, status, error) 
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                            ON DUPLICATE KEY UPDATE updated_at = VALUES(updated_at)
                        """
                        cur.executemany(query, buffer_data)
                        conn.commit()
                        consumer.commit()
                        
                        print(f"\n[SUCCESS] Inserted {len(buffer_data)} data to DB.")
                        buffer_data = []

    except KeyboardInterrupt:
        print("\nStopping worker...")
    except Exception as e:
        print(f"\n[CRITICAL ERROR] {e}")
    finally:
        if buffer_data:
            cur.executemany(query, buffer_data)
            conn.commit()
            consumer.commit()
        cur.close()
        conn.close()
        consumer.close()

if __name__ == "__main__":
    main()