import clickhouse_connect
import clickhouse_connect.driver
import clickhouse_connect.driver.client
from dotenv import load_dotenv
import os
from db import get_connection
import time
import pandas as pd
import numpy as np
import json

print('Migrate Postgres table to Clickhouse tool')
batch_size = int(input('Batch Size (Default 50K) : ') or 50000)
clickhouse_table = input('Clickhouse Table: ')
postgres_db = input('Postgres Database: ')
postgres_table = input('Postgres Table: ')

load_dotenv('.env', override=True)

CK_HOST=os.getenv('CK_HOST')
CK_USER=os.getenv('CK_USER')
CK_PASS=os.getenv('CK_PASS')


client = clickhouse_connect.get_client(host=CK_HOST, username=CK_USER, password=CK_PASS)

def migrate(ch_client: clickhouse_connect.driver.client.Client, ch_table, batch_rows, column_names: list[str]):
    ch_client.insert(ch_table, data=batch_rows, column_names=column_names)
    
# migrate sms logs

con = get_connection(postgres_db)

cursor = con.cursor(name=f'migrate_{postgres_db}/{postgres_table}')
cursor.itersize = batch_size

cursor.execute(f'SELECT * FROM {postgres_table}')

migrated_rows = 0
columns = None

while True:
    rows = cursor.fetchmany(batch_size)
    
    if not rows or len(rows) == 0:
        print('No more data for migration, skiping...')
        break
        
    if columns is None:
        columns = [desc[0] for desc in cursor.description]
        
    df = pd.DataFrame(rows, columns=columns)
    
    for col in columns:
        is_numeric = np.issubdtype(df[col].dtype, np.number)
        is_object = df[col].dtype == 'object'

        # we dont store null value on clickhouse, 
        # instead provide default value
        # for example, null on type string, can be empty string instead
        # and null on type number, can be 0
        
        # colum type number is null
        if is_numeric:
            df[col] = df[col].fillna(0)
        
        # column type string is null
        if is_object:
            df[col] = df[col].fillna('')
            
    df['request_payload'] = df['request_payload'].apply(json.dumps)
    df['client_ip'] = df['client_ip'].astype(str)
    
    normalized_list = df.to_numpy().tolist()
    migrate(client, clickhouse_table, batch_rows=normalized_list, column_names=columns)
        
    migrated_rows += len(rows)
    print(f'Migrated {migrated_rows} records')
    #time.sleep(3)
    
cursor.close()
con.close()
print('Migration done')