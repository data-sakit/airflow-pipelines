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

clickhouse_table_name = 'pixbi.umiat_sklad'

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 5
}

file_path_sklad = "/mnt/shares/umiat/Склад.xlsx"

with DAG('umiat_sklad', default_args=default_args, schedule='40 5 * * *') as dag:

    @task(task_id="sklad_xls", retries=2)
    def sklad_xls():
        df = pd.read_excel(file_path_sklad,skiprows=9)
        d = pd.read_excel(file_path_sklad).iloc[2,3]
        df['D']=d
        df=df.rename(columns={'Unnamed: 1':'Склад','Unnamed: 4':'Артикул','Unnamed: 5':'Номенклатура','Unnamed: 8':'Группа настр фин. учета','Unnamed: 9':'Вид номенклатуры','Unnamed: 10':'Ед изм','Количество':'НО Количество','Оценка':'НО Оценка','Количество.1':'П Количество','Оценка.1':'П Оценка','Количество.2':'Р Количество','Оценка.2':'Р Оценка','Количество.3':'КО Количество','Оценка.3':'КО Оценка'})
        df=df[['Склад','Артикул','Номенклатура','Группа настр фин. учета','Вид номенклатуры','Ед изм','НО Количество','НО Оценка','П Количество','П Оценка','Р Количество','Р Оценка','КО Количество','КО Оценка','D']]
        return df


    @task(task_id="create_table", retries=2)
    def create_table():
        with Client(host=clickhouse_host, port=clickhouse_port, user=clickhouse_user, password=clickhouse_password) as client:
            client.execute(f'''
                CREATE TABLE IF NOT EXISTS {clickhouse_table_name}
                (
                    `id` UInt32,
                    `Sklad` Nullable(String),
                    `Artikul` Nullable(String),
                    `Nomenklatura` Nullable(String),
                    `QrupNastrFin` Nullable(String),
                    `VidNomen` Nullable(String),
                    `EdIzm` Nullable(String),
                    `NO_kolich` Nullable(Float64),
                    `NO_oshenka` Nullable(Float64),
                    `P_kolich` Nullable(Float64),
                    `P_oshenka` Nullable(Float64),
                    `R_kolich` Nullable(Float64),
                    `R_oshenka` Nullable(Float64),
                    `KO_kolich` Nullable(Float64),
                    `KO_oshenka` Nullable(Float64),
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
            data = [(index, row['Склад'], row['Артикул'], row['Номенклатура'], row['Группа настр фин. учета'], row['Вид номенклатуры'],row['Ед изм'],row['НО Количество'],row['НО Оценка'],row['П Количество'],row['П Оценка'],row['Р Количество'],row['Р Оценка'],row['КО Количество'],row['КО Оценка'],row['D']) for index, row in df.iterrows()]
            client.execute(  
                f'INSERT INTO {clickhouse_table_name} (id,Sklad, Artikul, Nomenklatura, QrupNastrFin, VidNomen, EdIzm, NO_kolich, NO_oshenka, P_kolich, P_oshenka, R_kolich, R_oshenka, KO_kolich, KO_oshenka,D) VALUES',
                data
                )
  
    _df = sklad_xls()
    create_table() >> truncate_table() >> insert_into_clickhouse(_df)
