import os
from psycopg import connect

SOURCE_DB_NAME=os.getenv('SOURCE_DB_NAME')
SOURCE_DB_USER=os.getenv('SOURCE_DB_USER')
SOURCE_DB_PASS=os.getenv('SOURCE_DB_PASS')
SOURCE_DB_PORT=os.getenv('SOURCE_DB_PORT')
SOURCE_DB_HOST=os.getenv('SOURCE_DB_HOST')

def get_connection(db_name: str):
    print(db_name)
    return connect(
        host=SOURCE_DB_HOST,
        port=SOURCE_DB_PORT,
        dbname=db_name,
        user=SOURCE_DB_USER,
        password=SOURCE_DB_PASS
    )

