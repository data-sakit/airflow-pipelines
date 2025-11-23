from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.utils.dates import days_ago
from clickhouse_driver import Client
import pandas as pd
import pymssql
from datetime import timedelta
import pandas as pd
import numpy as np
import os

 



clickhouse_host = Variable.get('CH_HOST')
clickhouse_port = Variable.get('CH_PORT')
clickhouse_user = Variable.get('CH_USER')
clickhouse_password = Variable.get('CH_PASSWORD')

 



clickhouse_table_name = 'pixbi.parsec_Eg'

 



default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 5
}

 



with DAG('parsec_Eg', default_args=default_args, schedule='45 5 * * *') as dag:


    @task(task_id="execute_sql", retries=2)
    def execute_sql():
        ms_server = Variable.get('MS_SERVER')
        ms_user = Variable.get('MS_USER')
        ms_password = Variable.get('MS_PASSWORD')

        conn = pymssql.connect(server=ms_server, user=ms_user, password=ms_password, database='DWH')
        query = """ 
                WITH ou_hier AS (
                    SELECT
                        id AS org_id,
                        CAST(name AS NVARCHAR(MAX)) AS path
                    FROM dbo.parsec_org_unit
                    WHERE parent_id IN (
                        '4BF8A796-F898-4B14-A7FB-5092E19F8BAC',
                        '56D1A700-525F-401F-A76F-CFFF08E974A4'
                    )
                    AND id <> parent_id

                    UNION ALL

                    SELECT
                        pou.id,
                        ou_hier.path + N' / ' + pou.name
                    FROM dbo.parsec_org_unit pou
                    INNER JOIN ou_hier ON pou.parent_id = ou_hier.org_id
                ),

                event_filtered AS (
                    SELECT
                        pe.*,
                        pp.last_name, pp.first_name, pp.middle_name, pp.org_id,
                        pp.position_name, e.outstaff, ef.name AS func_name,
                        ptt.description, pt.type_id, ms.code
                    FROM dbo.parsec_event pe
                    LEFT JOIN dbo.parsec_person pp ON pp.id = pe.person_id
                    LEFT JOIN dbo.employee e ON pp.employee_id = e.id
                    LEFT JOIN dbo.employee_position ep ON e.position_id = ep.id
                    LEFT JOIN dbo.parsec_transaction_type ptt ON ptt.id = pe.transaction_type
                    LEFT JOIN dbo.master_system ms ON ms.guid = pe.master_system_id
                    LEFT JOIN dbo.parsec_territory pt ON pt.id = pe.territory_id
                    LEFT JOIN dbo.employee_function ef ON ef.id = ep.function_id
                    WHERE
                        ptt.description IN ('Normal exit by key', 'Normal entry by key')
                        AND pe.moment BETWEEN '2025-01-01T00:00:00+03:00' AND '2026-01-01T00:00:00+03:00'
                        AND ms.code = 'parsec_egypt'
                )

                SELECT
                    CAST(ef.moment AS DATETIME) AS [DateTime],
                    ef.description AS [Event],
                    ef.last_name + ' ' + ef.first_name + ' ' + ef.middle_name AS [FullName],
                    CASE
                        WHEN ef.outstaff = 1 THEN 'Да'
                        WHEN ef.outstaff = 0 OR ef.outstaff IS NULL THEN 'Нет'
                    END AS outstaff,
                    ef.position_name,
                    ef.func_name AS [PositionName],
                    oh.path
                FROM event_filtered ef
                INNER JOIN ou_hier oh ON ef.org_id = oh.org_id
        """
      


        cursor = conn.cursor(as_dict=True)
        cursor.execute(query)
        rows = cursor.fetchall()
        df = pd.DataFrame(rows)
        cursor.close()
        conn.close()      

   
        df['DateTime']=pd.to_datetime(df['DateTime'])
        df['Date']=df['DateTime'].dt.date
        column_to_fill=['Event', 'FullName','outstaff','position_name','PositionName','path']
        df[column_to_fill]=df[column_to_fill].fillna('Unknown')
        df['Shift']=''
        df['Shift']=df.apply(
                    lambda row:'Morning'if (row['Event']=='Normal entry by key' and row['DateTime'].hour<=16)
                    else 'Evening' if (row['Event']=='Normal entry by key' and row['DateTime'].hour>16)
                    else 'Morning' if (row['Event']=='Normal exit by key' and row['DateTime'].hour>9)
                    else 'Evening' if (row['Event']=='Normal exit by key' and  row['DateTime'].hour<=9)
                    else row['Event'],axis =1)
        df_new=df.groupby(['Event','FullName','outstaff','position_name','PositionName','path', 'Shift','Date'],as_index=False).agg({'DateTime':'max'})
        df_day=df_new[df_new['Shift']=="Morning"]
        df_night=df_new[df_new['Shift']=="Evening"]
        df_pivot_day=df_day.pivot(index=[ 'FullName','outstaff','position_name','PositionName','path', 'Shift','Date'],columns='Event',values='DateTime').reset_index()
        df_night['New_Date'] = df_night['Date'].where(df_night['Event'] == 'Normal entry by key', df_night['Date'] - pd.Timedelta(days=1))
        df_pivot_night=df_night.pivot(index=['FullName','outstaff','position_name','PositionName','path','Shift','New_Date'],columns='Event',values='DateTime').reset_index()
        df_pivot_night=df_pivot_night.rename(columns={'New_Date':'Date'})
        df_full_join = pd.concat([df_pivot_day,df_pivot_night],ignore_index=True)
        df_full_join.rename(columns={'Normal exit by key':'DateTimeExit','Normal entry by key':'DateTimeEntry'},inplace =True)
        df_full_join = df_full_join[['Shift','FullName','outstaff','position_name','PositionName','path', 'Date', 'DateTimeEntry', 'DateTimeExit']]
        df_full_join['Error_Describe'] = df_full_join.apply(
            lambda row: 'No entry' if pd.isna(row['DateTimeEntry']) else (
                'No exit' if pd.isna(row['DateTimeExit']) else None),axis=1)

        df_full_join=df_full_join.drop_duplicates()
        df_full_join['Date'] = pd.to_datetime(df_full_join['Date'])
        max_date = df_full_join['Date'].max()
        df_full_join['Flag'] = (max_date - df_full_join['Date']).dt.days == 1

        df_full_join['Flag'] = df_full_join['Flag'].apply(lambda x: 'Yesterday' if x else 'Not yesterday')
        df_full_join['Differ_entry_exit'] = df_full_join.apply(
            lambda row: f"{(row['DateTimeExit'] - row['DateTimeEntry']).seconds // 3600:02}:{((row['DateTimeExit'] - row['DateTimeEntry']).seconds % 3600) // 60:02}"
            if pd.notnull(row['DateTimeEntry']) and pd.notnull(row['DateTimeExit'])
            else "нет данных",
            axis=1
        )
        valid_times = df_full_join[df_full_join['Differ_entry_exit'] != "нет данных"].copy()


        valid_times['Hour'] = valid_times['Differ_entry_exit'].apply(lambda x: int(x.split(':')[0]) + int(x.split(':')[1])/60)


        average_hour = valid_times['Hour'].mean()


        df_full_join['average_hour'] = round(average_hour)

        df_full_join = df_full_join.astype(str)
        df_full_join =df_full_join.sort_values(["Date"])
        df_full_join=df_full_join.fillna('-',inplace=False)
        df_full_join['DateTimeEntry'] = df_full_join['DateTimeEntry'].replace('NaT', '-')
        df_full_join['DateTimeExit'] = df_full_join['DateTimeExit'].replace('NaT', '-')


        

        return df_full_join  # Возвращаем DataFrame



    @task(task_id="truncate_table", retries=2)
    def truncate_table():
        with Client(host=clickhouse_host, port=clickhouse_port, user=clickhouse_user, password=clickhouse_password) as client:
            client.execute(f'TRUNCATE TABLE IF EXISTS {clickhouse_table_name}')


    @task(task_id="insert_into_clickhouse", retries=2)
    def insert_into_clickhouse(df_full_join):
        with Client(host=clickhouse_host, port=clickhouse_port, user=clickhouse_user, password=clickhouse_password) as client:
            data = [(index,row['FullName'],row['outstaff'], row['position_name'], row['PositionName'],row['path'],row['Date'],row['Shift'],row['DateTimeEntry'],row['DateTimeExit'],row['Error_Describe'],row['Flag'],row['Differ_entry_exit'], row['average_hour']) for index, row in df_full_join.iterrows()]

            client.execute(
                f'''INSERT INTO {clickhouse_table_name} (id, Name,outstaff, position_name, PositionName,path, Date, Shift, Entry_datetime, Exit_datetime,  Error, Flag, Differ_entry_exit, average_hour
                   )VALUES''',
            data
        )


 


    ms_df = execute_sql()

    truncate_table() >> insert_into_clickhouse(ms_df) 
