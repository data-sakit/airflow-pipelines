from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.utils.dates import days_ago
from clickhouse_driver import Client
import pandas as pd
import pymssql

clickhouse_host = Variable.get('CH_HOST')
clickhouse_port = Variable.get('CH_PORT')
clickhouse_user = Variable.get('CH_USER')
clickhouse_password = Variable.get('CH_PASSWORD')

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 5
}
clickhouse_table_name = 'pixbi.umiat_zadol_client'
file_path="/mnt/shares/umiat/Задолженность клиентов.xlsx"

with DAG('umiat_zadol_client', default_args=default_args, schedule='40 5 * * *') as dag:
    
    @task(task_id="zadol_client", retries=2)
    def zadol_client():
        df = pd.read_excel(file_path,skiprows=20)
        d = pd.read_excel(file_path).iloc[2,3]
        df['D']=d
        df= df.rename(columns={
            df.columns[1]:'Name',
            df.columns[8]:'Summa',
            df.columns[11]:'DolyaDolqa',
            df.columns[12]:'Prosrocheno',
            df.columns[13]:'Prosrocheno_per',
            df.columns[14]:'Dney',
            df.columns[15]:'Dolq',
            df.columns[16]:'KPostupleniyu',
            df.columns[17]:'NeProsrocheno',
            df.columns[18]:'Ot1Do29',
            df.columns[19]:'Ot30Do179',
            df.columns[20]:'Sviwe180'})
        df=df.iloc[:, [1,8,11, 12,13,14,15,16,17,18,19,20,21]]
        return df

    @task(task_id="create_table", retries=2)
    def create_table():
        with Client(host=clickhouse_host, port=clickhouse_port, user=clickhouse_user, password=clickhouse_password) as client:
            client.execute(f'''
                CREATE TABLE IF NOT EXISTS {clickhouse_table_name}
                (
                    `id` UInt32,
                    `Name` Nullable(String),
                    `Summa` Nullable(Float64),
                    `DolyaDolqa` Nullable(Float64),
                    `Prosrocheno` Nullable(Float64),
                    `Prosrocheno_per` Nullable(Float64),
                    `Dney` Nullable(Float64),
                    `Dolq` Nullable(Float64),
                    `KPostupleniyu` Nullable(Float64),
                    `NeProsrocheno` Nullable(Float64),
                    `Ot1Do29` Nullable(Float64),
                    `Ot30Do179` Nullable(Float64),
                    `Sviwe180` Nullable(Float64),
                    `D` Nullable(String)
                )
                ENGINE = MergeTree
                ORDER BY id
                SETTINGS index_granularity = 8192;
            ''')

    @task(task_id="truncate_table", retries=2)
    def truncate_table():
        with Client(host=clickhouse_host, port=clickhouse_port, user=clickhouse_user, password=clickhouse_password) as client:
            client.execute(f'TRUNCATE TABLE IF EXISTS {clickhouse_table_name}')


    @task(task_id="insert_into_clickhouse", retries=2)
    def insert_into_clickhouse(df):
        with Client(host=clickhouse_host, port=clickhouse_port, user=clickhouse_user, password=clickhouse_password) as client:
            data = [(index, row['Name'], row['Summa'],row['DolyaDolqa'], row['Prosrocheno'],  row['Prosrocheno_per'], row['Dney'], row['Dolq'], row['KPostupleniyu'], row['NeProsrocheno'], row['Ot1Do29'],row['Ot30Do179'],row['Sviwe180'],row['D']) for index, row in df.iterrows()]
            client.execute(
                f'INSERT INTO {clickhouse_table_name} (id,Name, Summa, DolyaDolqa, Prosrocheno, Prosrocheno_per, Dney, Dolq, KPostupleniyu,NeProsrocheno, Ot1Do29, Ot30Do179, Sviwe180, D ) VALUES',
                data
            )
    _df = zadol_client()
    create_table() >> truncate_table() >> insert_into_clickhouse(_df)
