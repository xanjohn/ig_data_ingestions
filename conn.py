import mysql.connector
from mysql.connector import pooling

pool_config = {
        'user' : 'admin',
        'password': 'MainNoLimitrac98ss',
        'host' : '103.191.17.67',
        'port' : 3306,
        'database' : 'instagram_users'
    }

try:
    pool_conn = mysql.connector.pooling.MySQLConnectionPool(
        pool_name='ig_ingestion_pool',
        pool_size=5,
        **pool_config
    )
    print("Connection Pool Succesfully Created")

except mysql.connector.Error as e:
    print(f"Error creating connection pool: {e}")
    sys.exit(1)

def get_db_connection():
    try:
        conn = pool_conn.get_connection()
        return conn
    except mariadb.Error as e:
        print(f"Error connecting to MariaDB: {e}")
        return None

