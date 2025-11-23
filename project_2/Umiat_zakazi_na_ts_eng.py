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

clickhouse_table_name = 'pixbi.umiat_zakazi_na_ts'

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 5
}

file_path_zakazi_na_ts = "/mnt/shares/umiat/Заказы на ТС.xlsx"

with DAG('umiat_zakazi_na_ts', default_args=default_args, schedule='40 5 * * *') as dag:

    @task(task_id="zakazi_na_ts_xls", retries=2)
    def zakazi_na_ts_xls():
        df = pd.read_excel(file_path_zakazi_na_ts,skiprows=5)
        d = pd.read_excel(file_path_zakazi_na_ts).iloc[0,2]
        df['D']=d
        df=df.rename(columns={
            df.columns[0]:'Prinadlej',
            df.columns[3]:'TipTS',
            df.columns[4]:'TIP',
            df.columns[6]:'Zakazchik',
            df.columns[7]:'TrebKTS',
            df.columns[8]:'TS',
            df.columns[9]:'VladelesTS',
            df.columns[10]:'StatusZakaz',
            df.columns[11]:'PricinaOtklon',
            df.columns[12]:'NZakaz',
            df.columns[13]:'Otkrit',
            df.columns[14]:'Naznacheno',
            df.columns[15]:'Otkloneno',
            df.columns[16]:'Ispolneno',
            df.columns[17]:'Itoqo'
        })
        columns_to_convert_str = ['Prinadlej', 'TipTS', 'TIP', 'Zakazchik', 'TrebKTS', 'TS', 'VladelesTS', 'StatusZakaz','PricinaOtklon','NZakaz']
        df[columns_to_convert_str]=df[columns_to_convert_str].astype(str)
        columns_to_convert_float = ['Otkrit','Naznacheno','Otkloneno','Ispolneno','Itoqo']
        df[columns_to_convert_float]=df[columns_to_convert_float].astype(float)
        df = df[['Prinadlej', 'TipTS', 'TIP', 'Zakazchik', 'TrebKTS', 'TS', 'VladelesTS', 'StatusZakaz','PricinaOtklon','NZakaz','Otkrit','Naznacheno','Otkloneno','Ispolneno','Itoqo','D']]
        return df

    @task(task_id="create_table", retries=2)
    def create_table():
        with Client(host=clickhouse_host, port=clickhouse_port, user=clickhouse_user, password=clickhouse_password) as client:
            client.execute(f'''
                CREATE TABLE IF NOT EXISTS {clickhouse_table_name}
                (
                    `id` UInt32,
                    `Prinadlej` Nullable(String),
                    `TipTS` Nullable(String),
                    `TIP` Nullable(String),
                    `Zakazchik` Nullable(String),
                    `TrebKTS` Nullable(String),
                    `TS` Nullable(String),
                    `VladelesTS` Nullable(String),
                    `StatusZakaz` Nullable(String),
                    `PricinaOtklon` Nullable(String),
                    `NZakaz` Nullable(String),
                    `Otkrit` Nullable(Float64),
                    `Naznacheno` Nullable(Float64),
                    `Otkloneno` Nullable(Float64),
                    `Ispolneno` Nullable(Float64),
                    `Itoqo` Nullable(Float64),
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
            data = [(index, row['Prinadlej'], row['TipTS'], row['TIP'], row['Zakazchik'], row['TrebKTS'], row['TS'], row['VladelesTS'], row['StatusZakaz'],row['PricinaOtklon'],row['NZakaz'],row['Otkrit'],row['Naznacheno'],row['Otkloneno'],row['Ispolneno'],row['Itoqo'],row['D']) for index, row in df.iterrows()]
            client.execute(
                f'INSERT INTO {clickhouse_table_name} (id, Prinadlej, TipTS, TIP, Zakazchik, TrebKTS, TS, VladelesTS, StatusZakaz,PricinaOtklon,NZakaz,Otkrit,Naznacheno,Otkloneno,Ispolneno,Itoqo, D) VALUES',
                data
            )

    _df = zakazi_na_ts_xls()
    create_table() >> truncate_table() >> insert_into_clickhouse(_df)
