import mariadb
import sys

def get_db_connection():
    try:
        conn = mariadb.connect(
            host = 'localhost',
            port = 3306,
            database = 'instagram_users'
        )
        print("DB Connect")
        return conn
    except mariadb.Error as e:
        print(f"Error connecting to MariaDB: {e}")
        return None

get_db_connection()