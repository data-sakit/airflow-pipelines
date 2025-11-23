from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.utils.dates import days_ago
from clickhouse_driver import Client
import pandas as pd
import numpy as np
import pymssql

clickhouse_host = Variable.get('CH_HOST')
clickhouse_port = Variable.get('CH_PORT')
clickhouse_user = Variable.get('CH_USER')
clickhouse_password = Variable.get('CH_PASSWORD')

clickhouse_table_name = 'pixbi.solut_activity_assign'
clickhouse_table_name_daily = 'pixbi.solut_manual_daily'

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 5
}

with DAG('solut_activity_assign', 
 default_args=default_args, schedule_interval='00 2,8 * * *',catchup=False) as dag:

    @task(task_id="execute_sql", retries=2)
    def execute_sql():
        ms_server = Variable.get('MS_SERVER')
        ms_user = Variable.get('MS_USER')
        ms_password = Variable.get('MS_PASSWORD')

        conn = pymssql.connect(server=ms_server, user=ms_user, password=ms_password, database='DWH')

        query = """
            SELECT
                CAST(eswa.assign_time AS DATE) AS Date,
                eswa.employee_id,
                eswa.name,
                e.has_smartwatch,
                eswa.profession,
                eswa.project,
                eswa.division,
                eswa.assign_time,
                eswa.return_time,
                eswa.shift,
                eswa.manager,                
                MAX(CASE 
                    WHEN esa.activity_type = 'Break' AND CAST(esa.activity_time AS TIME) > '06:00:00'
                    THEN DATEADD(SECOND, -3070, esa.activity_time) -- 51m10s = 3070 секунд
                    when esa.activity_type  ='Break'
                    then esa.activity_time
                END) AS Break_activity_time,

                MAX(CASE 
                    WHEN esa.activity_type = 'Movements' AND CAST(esa.activity_time AS TIME) > '06:00:00'
                    THEN DATEADD(SECOND, -350, esa.activity_time) -- 5m50s = 350 секунд
                    when esa.activity_type  ='Movements'
                    then esa.activity_time
                END) AS Movements_activity_time,

                MAX(CASE 
                    WHEN esa.activity_type = 'Work' AND CAST(esa.activity_time AS TIME) > '06:00:00'
                    THEN DATEADD(SECOND, -180, esa.activity_time) -- 3m = 180 секунд
                    when esa.activity_type  ='Work'
                    then esa.activity_time
                END) AS Work_activity_time
                
            FROM dbo.employee_smart_watch_assignment eswa
            LEFT JOIN dbo.employee_solut_activity esa
                ON esa.employee_id = eswa.employee_id
                AND CAST(eswa.assign_time AS DATE) = CAST(esa.start_date AS DATE)
            LEFT JOIN dbo.employee e
                ON e.id = eswa.employee_id
            GROUP BY
                CAST(eswa.assign_time AS DATE),
                eswa.employee_id,
                eswa.name,
                e.has_smartwatch,
                eswa.profession,
                eswa.project,
                eswa.division,
                eswa.assign_time,
                eswa.return_time,
                eswa.shift,
                eswa.manager
            order by CAST(eswa.assign_time AS DATE) desc"""
        df = pd.read_sql(query, conn)
        conn.close()


        df['Movements_activity_duration'] = pd.to_timedelta(df['Movements_activity_time'].astype(str),  errors='coerce') \
                                                .dt.total_seconds().fillna(0).astype(int)
        df['Work_activity_duration'] = pd.to_timedelta(df['Work_activity_time'].astype(str), errors='coerce') \
                                                .dt.total_seconds().fillna(0).astype(int)
        df['Break_activity_duration'] = pd.to_timedelta(df['Break_activity_time'].astype(str),  errors='coerce') \
                                                .dt.total_seconds().fillna(0).astype(int)
        df['Total_duration'] = df['Break_activity_duration']+df['Movements_activity_duration']+df['Work_activity_duration']
        
        df['Break_activity_percent'] = round((df['Break_activity_duration']/df['Total_duration'])*100,0)
        df['Movements_activity_percent'] = round((df['Movements_activity_duration']/df['Total_duration'])*100,0)
        df['Work_activity_percent'] =round((df['Work_activity_duration']/df['Total_duration'])*100,0)
        df['Total_percent'] = round((df['Total_duration']/df['Total_duration'])*100,0)

        
        numeric_columns = ['has_smartwatch', 'Break_activity_duration', 'Break_activity_percent',
                           'Movements_activity_duration','Total_duration','Total_percent','Movements_activity_percent',
                           'Work_activity_duration', 'Work_activity_percent']
        text_columns = [col for col in df.columns if col not in numeric_columns]

        df[text_columns] = df[text_columns].astype(str)
        df[numeric_columns] = df[numeric_columns].fillna(0).astype(int)  
        return df

    @task(task_id="solut_activity_assign_trunc", retries=2)
    def truncate_table():
        with Client(host=clickhouse_host, port=clickhouse_port, user=clickhouse_user, password=clickhouse_password) as client:
            client.execute(f'TRUNCATE TABLE IF EXISTS {clickhouse_table_name}')


    @task(task_id="insert_into_clickhouse", retries=2)
    def insert_into_clickhouse(df):
        with Client(host=clickhouse_host, port=clickhouse_port,
                    user=clickhouse_user, password=clickhouse_password) as client:
            data = [(index, *row) for index, row in df.iterrows()]
            client.execute(
                f"""
                INSERT INTO {clickhouse_table_name} (
                    id, dt, employee_id, full_name, has_smartwatch, profession, project, division,
                    assign_time, return_time, shift, manager,
                    Break_activity_time,Movements_activity_time,Work_activity_time,
                    Movements_activity_duration,Work_activity_duration,Break_activity_duration,Total_duration,
                    Break_activity_percent,Movements_activity_percent,Work_activity_percent,Total_percent
                ) VALUES
                """,
                data
            )

    st1 = truncate_table()
    st2 = execute_sql()
    st3 = insert_into_clickhouse(st2)
    st1 >> st2 >> st3




    @task(task_id="excel_daily", retries=2)
    def excel_daily():
        df_m_laes = pd.read_excel("/mnt/shares/solut/laes_2/manual.xlsm", sheet_name='DailyTable1')
        df_m_skif = pd.read_excel("/mnt/shares/solut/skif/manual.xlsm", sheet_name='DailyTable1')
        df_m=pd.concat([df_m_skif,df_m_laes],ignore_index=True)
        
        df_m_laes_2 = pd.read_excel("/mnt/shares/solut/laes_2/manual.xlsm", sheet_name='DailyTable2')
        df_m_skif_2 = pd.read_excel("/mnt/shares/solut/skif/manual.xlsm", sheet_name='DailyTable2')
        
        df_m_2=pd.concat([df_m_skif_2,df_m_laes_2],ignore_index=True)
        
        df_m = df_m.rename(columns={'Project': 'project', 'Date': 'dt'})
        df_m_2 = df_m_2.rename(columns={'Date': 'dt', 'Project': 'project', 'Division': 'org'})

        df_m['dt'] = pd.to_datetime(df_m['dt'], errors='coerce').dt.date
        df_m_2['dt'] = pd.to_datetime(df_m_2['dt'], errors='coerce').dt.date
        
        yesterday=(datetime.now()-timedelta(days=1)).date()
        df_m=df_m[df_m['dt']<=yesterday]
        df_m_2=df_m_2[df_m_2['dt']<=yesterday]
        manual_daily = pd.merge(df_m_2, df_m, how='left', on=['dt', 'project'])

        numeric_columns = ['PersReqNeed', 'SmartW_Issued', 'NoProtectBumper', 'Damaged', 'Lost', 'ChargingFailor', 'TechnicalFailor']
        text_columns = [col for col in manual_daily.columns if col not in numeric_columns]

        manual_daily[text_columns] = manual_daily[text_columns].astype(str)
        manual_daily[numeric_columns] = manual_daily[numeric_columns].fillna(0).astype(int)
        manual_daily = manual_daily.dropna(subset=['project'])
        return manual_daily

    @task(task_id="excel_daily_trunc", retries=2)
    def excel_daily_trunc():
        with Client(host=clickhouse_host, port=clickhouse_port, user=clickhouse_user, password=clickhouse_password) as client:
            client.execute(f'TRUNCATE TABLE IF EXISTS {clickhouse_table_name_daily}')

    @task(task_id="insert_into_clickhouse_excel_daily", retries=2)
    def insert_into_clickhouse_excel_daily(manual_daily):
            with Client(host=clickhouse_host, port=clickhouse_port, user=clickhouse_user, password=clickhouse_password) as client:
                data = [(index, *row) for index, row in manual_daily.iterrows()]
                client.execute(
                    f'INSERT INTO {clickhouse_table_name_daily} (id,dt,project,org,PersReqNeed,SmartW_Issued,NoProtectBumper,Damaged,Lost,ChargingFailor,TechnicalFailor) VALUES',
                    data
                )
    
    st1 = excel_daily_trunc()
    st2 = excel_daily()
    st3 = insert_into_clickhouse_excel_daily(st2)
    st1 >> st2 >> st3

