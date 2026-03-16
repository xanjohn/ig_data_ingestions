import mariadb
import sys

pool_config = {
        'user' : 'admin',
        'password': 'MainNoLimitrac98ss',
        'host' : 'localhost',
        'port' : 3306,
        'database' : 'instagram_users',
        'pool_name' : 'ig_ingestion_pool',
        'pool_size': 5
    }

try:
    pool_conn = mariadb.ConnectionPool(**pool_config)
    print("Connection Pool Succesfully Created")

except mariadb.Error as e:
    print(f"Error creating connection pool: {e}")

def get_db_connection():
    try:
        conn = pool_conn.get_connection()
        return conn
    except mariadb.Error as e:
        print(f"Error connecting to MariaDB: {e}")
        return None

