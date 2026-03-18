import requests
import time
from datetime import datetime
from conn import get_db_connection

def process_batch():
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
                    
                    if json_data.get('status') == 'ok':
                        data_content = json_data.get('data')
                        if not data_content or data_content == []:
                            final_status = 'error'
                            error_message = 'empty data'
                        else:
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

    except Exception as e:
        print(f"Database Error: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    while True:
        process_batch()
        time.sleep(5)