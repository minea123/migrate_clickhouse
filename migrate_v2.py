from db import get_connection
from ck import get_ck_client
import datetime

ck_client = get_ck_client()

CHUNK = 10000
offset = ck_client.query('SELECT COUNT(*) FROM sms_prod.sms_logs').result_rows[0][0]

while True:
    query = f'INSERT INTO sms_prod.sms_logs SETTINGS async_insert=1, wait_for_async_insert=1 SELECT id,username,user_agent,client_ip,request_method,request_url,controller,action,response_code,request_date,response_date,request_payload,duration FROM server214.sms_logs LIMIT {CHUNK} OFFSET {offset}'
    query_summary = ck_client.command(query)
    
    if query_summary.summary.get('written_rows') is None:
        print('Out of records for sync')
        break

    offset += CHUNK
    current_datetime = datetime.datetime.now()
    
    print(f'{current_datetime} Migrated rows {offset}')
    break

