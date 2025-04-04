import clickhouse_connect
import clickhouse_connect.driver
import clickhouse_connect.driver.client
from dotenv import load_dotenv
import os

load_dotenv('.env', override=True)

CK_HOST=os.getenv('CK_HOST')
CK_USER=os.getenv('CK_USER')
CK_PASS=os.getenv('CK_PASS')

def get_ck_client():
    return clickhouse_connect.get_client(host=CK_HOST, username=CK_USER, password=CK_PASS)