import requests
import time
from datetime import datetime
from conn import get_db_connection

def get_args():
    parser = argparse.ArgumentParser(description='Worker Lookup to API and push to Kafka')
    parser.add_argument('--topic_out', type=str, required=True, help='Topic Kafka (Output)')
    parser.add_argument('--batch', type=int, default=10, help='Batch from DB')
    return parser.parse_args()


def process_batch(args, producer):
    conn = get_db_connection()
    if not conn: return
    cur = conn.cursor()

    try:
        cur.execute("SELECT alternative_original_id FROM post_link_seeds WHERE status = 'pending' LIMIT 10")
        rows = cur.fetchall()

        if not rows:
            print("Waiting for data...")
            return

        target_shortcode = [row[0] for row in rows]
        
        format_strings = ','.join(['%s'] * len(target_shortcode)) # %s
        update_running_query = f"""
        UPDATE post_link_seeds
        SET status = 'running', updated_at = NOW()
        WHERE alternative_original_id IN ({format_strings})
        """
        cur.execute(update_running_query, target_shortcode)
        conn.commit()
        print(f"Processing {len(target_shortcode)} Shortcode...")

        endpoint = "http://103.191.17.93:9779/get-instagram-post"
        
        for shortcode in target_shortcode:
            payloads = {'shortcode': shortcode}
            final_status = 'error'
            error_message = None
            
            try:
                response = requests.get(endpoint, params=payloads, timeout=15)
                
                if response.status_code == 200:
                    
                    json_data = response.json()
                    
                    if json_data.get('status') == 'ok' and json_data.get('data'):
                        
                        raw_content = json_data.get('data')
                        media_info = raw_content.get('xdt_shortcode_media',{})
                        owner_info = media_info.get('owner', {})
                        
                        enriched_data = {
                            "raw": json_data,
                            "metadata": {
                                "crawltime": int(time.time()),
                                "account_id": owner_info.get('id'),
                                "account": owner_info.get('username')
                            }
                        }
                        
                        producer.send(args.topic_out, value=enriched_data)
                        final_status = 'success'
                    else:
                        final_status = 'error'
                        error_message = 'status not ok'
                        
                else:
                    final_status = 'error'
                    error_message = f'error {response.status_code}'
            except Exception as e:
                final_status = 'error'
                error_message = f"error network timeout"
            
            update_final_query = """
            UPDATE post_link_seeds
            SET status = %s, error = %s, updated_at = NOW()
            WHERE alternative_original_id = %s
            """
            
            cur.execute(update_final_query, (final_status, error_message, shortcode))
            print(f"-> {shortcode}: {final_status} ({error_message})")

        conn.commit()
        producer.flush()

    except Exception as e:
        print(f"Database Error: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    args = get_args()
    
    producer = KafkaProducer(
        bootstrap_servers=['10.27.10.29:9092', '10.27.10.43:9092', '10.27.10.145:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print(f"Worker Lookup Started. Output Topic: {args.topic_out}")
    
    while True:
        process_batch(args, producer)
        time.sleep(5)