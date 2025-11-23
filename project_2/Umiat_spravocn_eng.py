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

clickhouse_table_name = 'pixbi.umiat_spravocn'

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 5
}

file_path_spravocn = "/mnt/shares/umiat/Справочник_затратам на ТС.xlsx"

with DAG('umiat_spravocn', default_args=default_args, schedule='40 5 * * *') as dag:

    @task(task_id="spravocn_xls", retries=2)
    def spravocn_xls():
        df = pd.read_excel(file_path_spravocn)
        return df


    @task(task_id="create_table", retries=2)
    def create_table():
        with Client(host=clickhouse_host, port=clickhouse_port, user=clickhouse_user, password=clickhouse_password) as client:
            client.execute(f'''
                CREATE TABLE IF NOT EXISTS {clickhouse_table_name}
                (
                    `id` UInt32,
                    `Statya` Nullable(String),
                    `Qruppa` Nullable(String)
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
            data = [(index, row['Статья'], row['Группа']) for index, row in df.iterrows()]
            client.execute(
                f'INSERT INTO {clickhouse_table_name} (id,Statya, Qruppa) VALUES',
                data
            )
    
    _df = spravocn_xls()
    create_table() >> truncate_table() >> insert_into_clickhouse(_df)
