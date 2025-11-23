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

clickhouse_table_name = 'pixbi.umiat_sdacha_pl'

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 5
}

file_path_sdacha_pl = "/mnt/shares/umiat/Отчет о сдаче ПЛ.xlsx"

with DAG('umiat_sdacha_pl', default_args=default_args, schedule='40 5 * * *') as dag:

    @task(task_id="sdacha_pl_xls", retries=2)
    def sdacha_pl_xls():
        df = pd.read_excel(file_path_sdacha_pl,skiprows=3)
        d = pd.read_excel(file_path_sdacha_pl).iloc[0,2]
        df['D']=d
        df=df[['ТС.Гаражный номер','Водитель 1','Регистратор','Дата','Контрагент','Заказ','Дата окончания','Закрыт водителем','Командировка','Подразделение контрагента','Организация','Дата.1','Количество', 'D']]
        return df


    @task(task_id="create_table", retries=2)
    def create_table():
        with Client(host=clickhouse_host, port=clickhouse_port, user=clickhouse_user, password=clickhouse_password) as client:
            client.execute(f'''
                CREATE TABLE IF NOT EXISTS {clickhouse_table_name}
                (
                    `id` UInt32,
                    `Qaraj_nom` Nullable(String),
                    `Voditel` Nullable(String),
                    `Reqistrator` Nullable(String),
                    `Data` Nullable(String),
                    `Kontragent` Nullable(String),
                    `Zakaz` Nullable(String),
                    `Data_okonch` Nullable(String),
                    `Zakrit_vodit` Nullable(String),
                    `Komandirovka` Nullable(String),
                    `Podrazd` Nullable(String),
                    `Orqaniz` Nullable(String),
                    `Data_2` Nullable(String),
                    `Kolich` Nullable(Float64),
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
            data = [(index, row['ТС.Гаражный номер'], row['Водитель 1'], row['Регистратор'], row['Дата'], row['Контрагент'],row['Заказ'],row['Дата окончания'],row['Закрыт водителем'],row['Командировка'],row['Подразделение контрагента'],row['Организация'],row['Дата.1'],row['Количество'],row['D']) for index, row in df.iterrows()]
            client.execute(
                f'INSERT INTO {clickhouse_table_name} (id,Qaraj_nom, Voditel, Reqistrator, Data, Kontragent,Zakaz,Data_okonch,Zakrit_vodit,Komandirovka,Podrazd,Orqaniz,Data_2,Kolich,D) VALUES',
                data
            )
    _df = sdacha_pl_xls()
    create_table() >> truncate_table() >> insert_into_clickhouse(_df)
