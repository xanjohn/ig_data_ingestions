import mariadb
import sys

def get_db_connection():
    try:
        conn = mariadb.connect(
            user = 'root',
            password = '123',
            host = 'localhost',
            port = 3306,
            database = 'ingestion_try'
        )
        return conn
    except mariadb.Error as e:
        print(f"Error connecting to MariaDB: {e}")
        return None
