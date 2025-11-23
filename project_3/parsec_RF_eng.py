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

 



clickhouse_table_name = 'pixbi.parsec_RF_2'

 



default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 5
}



with DAG('parsec_RF_2', default_args=default_args, schedule='50 5 * * *') as dag:

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
                    AND pt.type_id = '6CFFFDC4-BF2F-47F9-846C-8985EFF20AF3'
                    AND ms.code = 'parsec_rus'
            )

            SELECT
                CAST(ef.moment AS DATETIME) AS [DateTime],
                ef.description AS [Event],
                ef.last_name + ' ' + ef.first_name + ' ' + ef.middle_name AS [FullName],
                ef.position_name,
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
        
        df['DateTime'] = pd.to_datetime(df['DateTime'], format='%d.%m.%y %H:%M')
        df['event_type'] = df['Event'].str.contains('вход', case=False).map({True: 'in', False: 'out'})
        df = df.sort_values(['FullName', 'DateTime']).reset_index(drop=True)

        df['PairID'] = (
            ((df['event_type'] != 'out') | (df['event_type'].shift() != 'in'))
            .groupby(df['FullName'])
            .cumsum()
        )

        df_in = df[df['event_type'] == 'in'].copy()
        df_out = df[df['event_type'] == 'out'].copy()

        df_full_join = pd.merge(
            df_in[['FullName', 'DateTime', 'position_name', 'path', 'PairID']],
            df_out[['FullName', 'DateTime', 'position_name', 'path', 'PairID']],
            on=['FullName', 'PairID'],
            how='outer',
            suffixes=('Entry', 'Exit')
        )


        df_full_join['position_name'] = df_full_join['position_nameEntry'].combine_first(
            df_full_join['position_nameExit']
        )
        df_full_join['path'] = df_full_join['pathEntry'].combine_first(
            df_full_join['pathExit']
        )


        df_same = df_full_join[
            df_full_join['DateTimeEntry'].notna() &
            df_full_join['DateTimeExit'].notna() &
            (df_full_join['DateTimeEntry'].dt.date == df_full_join['DateTimeExit'].dt.date)
        ].copy()

        df_cross = df_full_join[
            df_full_join['DateTimeEntry'].notna() &
            df_full_join['DateTimeExit'].notna() &
            (df_full_join['DateTimeEntry'].dt.date != df_full_join['DateTimeExit'].dt.date)
        ].copy()

        df_error = df_full_join[
            df_full_join['DateTimeEntry'].isna() | df_full_join['DateTimeExit'].isna()
        ].copy()


        df_cross['Midnight'] = df_cross['DateTimeEntry'].dt.normalize() + pd.Timedelta(days=1)

        df_part1 = df_cross.copy()
        df_part1['DateTimeExit'] = df_part1['Midnight']

        df_part2 = df_cross.copy()
        df_part2['DateTimeEntry'] = df_part2['Midnight']

        result = pd.concat(
            [df_same, df_part1, df_part2, df_error],
            ignore_index=True
        )


        result['Error_Describe'] = ''
        result.loc[result['DateTimeEntry'].isna(), 'Error_Describe'] = 'No entry'
        result.loc[result['DateTimeExit'].isna(), 'Error_Describe'] = 'No exit'


        delta = result['DateTimeExit'] - result['DateTimeEntry']
        result['Differ_entry_exit'] = delta.dt.components.apply(
            lambda r: f"{int(r.hours):02d}:{int(r.minutes):02d}" if not pd.isna(r.hours) else None,
            axis=1
        )


        result['Hours'] = delta.dt.total_seconds() / 3600
        result['average_hour'] = result['Hours'].mean()


        yesterday = pd.Timestamp.today().normalize() - pd.Timedelta(days=1)
        result['Flag'] = result['DateTimeEntry'].dt.normalize().apply(
            lambda d: 'Yesterday' if d == yesterday else 'Not yesterday'
        )


        result['Shift'] = np.where(
            (result['DateTimeEntry'].dt.time >= pd.to_datetime('06:30').time()) &
            (result['DateTimeEntry'].dt.time <= pd.to_datetime('18:30').time()),
            'Дневная',
            'Ночная'
        )


        result['Date'] = np.where(
            result['DateTimeEntry'].notna(),
            result['DateTimeEntry'].dt.date,
            result['DateTimeExit'].dt.date
        )



        result['DateTimeEntry'] = result['DateTimeEntry'].dt.strftime('%Y-%m-%dT%H:%M:%S+03:00')
        result['DateTimeExit']  = result['DateTimeExit'].dt.strftime('%Y-%m-%dT%H:%M:%S+03:00')

        df_full_join = result[['Shift','FullName','position_name','path','Date', 'DateTimeEntry',
                            'DateTimeExit','Error_Describe', 'Differ_entry_exit',
                            'average_hour', 'Flag']]

        df_full_join = df_full_join.astype(str)



        return df_full_join  # Возвращаем DataFrame



    @task(task_id="truncate_table", retries=2)
    def truncate_table():
        with Client(host=clickhouse_host, port=clickhouse_port, user=clickhouse_user, password=clickhouse_password) as client:
            client.execute(f'TRUNCATE TABLE IF EXISTS {clickhouse_table_name}')


    @task(task_id="insert_into_clickhouse", retries=2)
    def insert_into_clickhouse(df_full_join):
        with Client(host=clickhouse_host, port=clickhouse_port, user=clickhouse_user, password=clickhouse_password) as client:
            df_full_join = df_full_join.astype(str)
            data = [(index,row['FullName'],row['position_name'],row['path'],row['Date'],row['Shift'],row['DateTimeEntry'],row['DateTimeExit'],row['Error_Describe'],row['Flag'],row['Differ_entry_exit'], row['average_hour']) for index, row in df_full_join.iterrows()]

            client.execute(
                f'''INSERT INTO {clickhouse_table_name} (id, Name,position_name, path, Date, Shift, Entry_datetime, Exit_datetime,  Error, Flag, Differ_entry_exit, average_hour
                   )VALUES''',
            data
        )



    ms_df = execute_sql()
    truncate_table() 
    insert_into_clickhouse(ms_df) 