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

clickhouse_table_name_omnikom = 'pixbi.umiat_omnikom'

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 5
}

file_path_omnikom = "/mnt/shares/umiat/Омникомм.xlsx"  # Corrected indentation

with DAG('umiat_omnikom', default_args=default_args, schedule='40 5 * * *') as dag:

    @task(task_id="omnikom_xls", retries=2)
    def omnikom_xls():
        df = pd.read_excel(file_path_omnikom, skiprows=8)
        d = pd.read_excel(file_path_omnikom).iloc[2,2]
        df['D']=d
        df.rename(columns={ 
            df.columns[0]: 'Data',
            df.columns[3]: 'TS',
            df.columns[6]: 'Probeq',
            df.columns[7]: 'VremyaDviqChas',
            df.columns[8]: 'VremyaChas',
            df.columns[9]: 'UrovenToplivaLitr',
            df.columns[10]: 'RasxodToplivaLitr',
            df.columns[11]: 'RasxodTopZaVremyaChas'}, inplace=True)
        df = df[['Data', 'TS', 'Probeq', 'VremyaDviqChas', 'VremyaChas', 'UrovenToplivaLitr', 'RasxodToplivaLitr', 'RasxodTopZaVremyaChas', 'D']]
        column_str=['Data','TS','D']
        df[column_str]=df[column_str].astype(str)
        column_float=['Probeq','VremyaDviqChas','VremyaChas','UrovenToplivaLitr','RasxodToplivaLitr','RasxodTopZaVremyaChas']
        df[column_float]=df[column_float].astype(float)
        return df

    @task(task_id="create_table", retries=2)
    def create_table():
        with Client(host=clickhouse_host, port=clickhouse_port, user=clickhouse_user, password=clickhouse_password) as client:
            client.execute(f'''
                CREATE TABLE IF NOT EXISTS {clickhouse_table_name_omnikom}
                (
                    `id` UInt32,
                    `Data` Nullable(String),
                    `TS` String,
                    `Probeq` Nullable(Float64),
                    `VremyaDviqChas` Nullable(Float64),
                    `RasxodToplivaLitr` Nullable(Float64),
                    `UrovenToplivaLitr` Nullable(Float64),
                    `VremyaChas` Nullable(Float64),
                    `RasxodTopZaVremyaChas` Nullable(Float64),
                    `D` Nullable(String)
                )
                ENGINE = MergeTree
                ORDER BY id
                SETTINGS index_granularity = 8192;
            ''')

    @task(task_id="truncate_table", retries=2)
    def truncate_table():
        with Client(host=clickhouse_host, port=clickhouse_port, user=clickhouse_user, password=clickhouse_password) as client:
            client.execute(f'TRUNCATE TABLE IF EXISTS {clickhouse_table_name_omnikom}')

    @task(task_id="insert_into_clickhouse", retries=2)
    def insert_into_clickhouse(df):
        with Client(host=clickhouse_host, port=clickhouse_port, user=clickhouse_user, password=clickhouse_password) as client:
            data = [(index, row['Data'], row['TS'], row['Probeq'], row['VremyaDviqChas'], row['VremyaChas'], row['UrovenToplivaLitr'], row['RasxodToplivaLitr'], row['RasxodTopZaVremyaChas'], row['D']) for index, row in df.iterrows()]
            client.execute(
                f'INSERT INTO {clickhouse_table_name_omnikom} (id, Data, TS, Probeq, VremyaDviqChas, VremyaChas, UrovenToplivaLitr, RasxodToplivaLitr, RasxodTopZaVremyaChas, D) VALUES',
                data
            )

    _df = omnikom_xls()
    create_table() >> truncate_table() >> insert_into_clickhouse(_df)
