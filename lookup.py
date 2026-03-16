import requests
import time
from conn import get_db_connection

def process_batch():
    conn = get_db_connection()
    if not conn: return
    cur = conn.cursor()

    try:
        cur.execute("SELECT id FROM driver WHERE status = 'Pending' LIMIT 10")
        rows = cur.fetchall()

        if not rows:
            print("Tidak ada data pending. Menunggu...")
            return

        target_ids = [row[0] for row in rows]
        
        format_strings = ','.join(['?'] * len(target_ids)) # Buat "?,?,?,..."
        cur.execute(f"UPDATE driver SET status = 'Running' WHERE id IN ({format_strings})", target_ids)
        conn.commit()
        print(f"Sedang memproses {len(target_ids)} data...")

        endpoint = "https://httpbin.org/post" #Dummy
        try:
            response = requests.post(endpoint, json={"processed_ids": target_ids}, timeout=10)
            
            if response.status_code == 200:
                cur.execute(f"UPDATE driver SET status = 'Success' WHERE id IN ({format_strings})", target_ids)
                print("[OK] API Success! Status updated to Success.")
            else:
                cur.execute(f"UPDATE driver SET status = 'Error' WHERE id IN ({format_strings})", target_ids)
                print(f"[FAIL] API returned {response.status_code}. Status updated to Error.")
        
        except Exception as e:
            cur.execute(f"UPDATE driver SET status = 'Error' WHERE id IN ({format_strings})", target_ids)
            print(f"[FAIL] Network Error: {e}. Status updated to Error.")

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