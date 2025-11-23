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

clickhouse_table_name = 'pixbi.umiat_zatrat_sobstv_ts'

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 5
}

file_path_zatrat_sobstv_ts = "/mnt/shares/umiat/Затраты по собственным ТС.xlsx"

with DAG('umiat_zatrat_sobstv_ts', default_args=default_args, schedule='40 5 * * *') as dag:

    @task(task_id="zatrat_sobstv_ts_xls", retries=2)
    def zatrat_sobstv_ts_xls():
        df = pd.read_excel(file_path_zatrat_sobstv_ts,skiprows=3)
        d = pd.read_excel(file_path_zatrat_sobstv_ts).iloc[0,2]
        df['D']=d
        df = df[['Документ', 'Контрагент', 'Подразделение контрагента', 'Проект', 'Транспортное средство', 'Тариф', 'Тип.Родитель', 'Начало дня','Время','Сумма','D']]
        return df

    @task(task_id="create_table", retries=2)
    def create_table():
        with Client(host=clickhouse_host, port=clickhouse_port, user=clickhouse_user, password=clickhouse_password) as client:
            client.execute(f'''
                CREATE TABLE IF NOT EXISTS {clickhouse_table_name}
                (
                    `id` UInt32,
                    `Document` Nullable(String),
                    `Kontrag` Nullable(String),
                    `Podrazd` Nullable(String),
                    `Proeykt` Nullable(String),
                    `TS` Nullable(String),
                    `Tarif` Nullable(Float64),
                    `TipRod` Nullable(String),
                    `NachDnya` Nullable(String),
                    `Vremya` Nullable(Float64),
                    `Summa` Nullable(Float64),
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
            data = [(index, row['Документ'], row['Контрагент'], row['Подразделение контрагента'], row['Проект'], row['Транспортное средство'], row['Тариф'], row['Тип.Родитель'], row['Начало дня'],row['Время'],row['Сумма'],row['D']) for index, row in df.iterrows()]
            client.execute(
                f'INSERT INTO {clickhouse_table_name} (id,Document, Kontrag, Podrazd, Proeykt, TS, Tarif, TipRod, NachDnya, Vremya, Summa, D) VALUES',
                data
            )

    _df = zatrat_sobstv_ts_xls()
    create_table() >> truncate_table() >> insert_into_clickhouse(_df)
