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

 


clickhouse_table_name = 'pixbi.PLAN_APP'

 


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(0),
    'retries': 5
}

 


with DAG('plan_app_dag', default_args=default_args, schedule='@daily') as dag:


    @task(task_id="count_row_ms", retries=2)
    def count_row_ms():
        server = Variable.get('MS_SERVER')
        user = Variable.get('MS_USER')
        password = Variable.get('MS_PASSWORD')

        conn = pymssql.connect(server, user, password, "DWH")
        cursor = conn.cursor()
        cursor.execute("select COUNT(*) as row_count from dbo.app_internal_order_plan_item")
        rows = cursor.fetchall()
        conn.close()

        return rows[0][0]  # Возвращаем количество строк


    @task(task_id="execute_sql", retries=2)
    def execute_sql():
        ms_server = Variable.get('MS_SERVER')
        ms_user = Variable.get('MS_USER')
        ms_password = Variable.get('MS_PASSWORD')

        conn = pymssql.connect(server=ms_server, user=ms_user, password=ms_password, database='DWH')
        query = """
        SELECT
                c.name AS contractor_name,
                cs.name AS site_name,
                procurement_plan_type,
                co.name AS construction_name,
                en.name AS enlarged_nomenclature,
                mu.name AS measure,
                nomenclature,
                internal_order AS reserve_document,
                m.name month_name,
                CAST([date] AS nvarchar(150)) AS Dt,
                division,
                iit.name AS type_nomen,
                previous_period_fact_exists,
                planned,
				cast(report_period_year as Varchar) as Year,               
                quantity,
                cost,
                CASE 
                    when c.name  ='Акционерное общество "КОНЦЕРН ТИТАН-2"' 
                    and cs.name='Прорыв'
                    and (co.name is NUll or co.name in('Прорыв','-',''))
                    and division = 'Дирекция "Прорыв"'
                    then '1_Дирекция Прорыв_1_Дирекция Прорыв'
                    
                    when c.name  ='Акционерное общество "КОНЦЕРН ТИТАН-2"' 
                    and cs.name='Курская АЭС-2'
                    and (co.name is NUll or co.name in('КуАЭС-2','-',''))
                    and (division in('Дирекция Курская АЭС','ОП "Дирекция по строительству объектов в Курской области"',
                      '',';')or division is Null)
                    then '2_Дирекция по строительству_2.1_АЭС Курская'	                
                    
                    when c.name  ='Акционерное общество "КОНЦЕРН ТИТАН-2"' 
                    and cs.name='НИТИ'
                    and (co.name is NUll or co.name in('НИТИ','-',''))
	                  and (division in('Филиал "Собственное управление строительством"',
									 'Дирекция НИТИ',';','') or division is Null)
                    then '3_Дирекция по строительству_2.2_НИТИ'

                    when c.name  ='Акционерное общество "КОНЦЕРН ТИТАН-2"' 
                    and cs.name='Прорыв'
                    and (co.name is NUll or co.name in('Прорыв','-',''))
                    and division = 'ОП «Сибирь»' 
                    then '4_Дирекция по строительству_2.3_ОП «Сибирь»'
                    
                    when c.name  ='Акционерное общество "КОНЦЕРН ТИТАН-2"' 
                    and cs.name='ЦКП "СКИФ"'
                    and (co.name is NUll or co.name in('СКИФ','','-'))
                    and (division in('Дирекция ЦКП «СКИФ»',
                    'Дирекция собственных сил программы ЦКП "СКИФ" (вспомогательные здания)',
                    '',';') or division is Null)
                    then '5_Дирекция по строительству_2.4_СКИФ'
                    
                    when c.name  ='Акционерное общество "КОНЦЕРН ТИТАН-2"' 
                    and cs.name='ФДРЦ'
                    and (co.name is NUll or co.name in('ФДРЦ','','-'))
                    and division ='Филиал "Собственное управление строительством"'
                    then '6_Дирекция по строительству_2.5_ФДРЦ'	
                    
                    when c.name  ='Акционерное общество "КОНЦЕРН ТИТАН-2"' 
                    and cs.name='ЦКП "СКИФ"'
                    and (co.name is NUll or co.name in('СКИФ','','-'))
                    and (division in('Дирекция ЦКП «СКИФ»','Дирекция собственных сил программы ЦКП "СКИФ" (вспомогательные здания)','',';')
                      or division is Null)
                    then '7_ДПО_3.1_СКИФ'	                
                    
                    when c.name  ='Акционерное общество "КОНЦЕРН ТИТАН-2"' 
                    and cs.name='Строительство ПТК'
                    and co.name  in('Утилизация (Горный)','Утилизация (Щучье)')
                    and (division in('Дирекция по поставкам оборудования',
                                      'Утилизация Горный','Дирекция "Прорыв"',
                                      ';','')or division is Null)
                    then '8_ДПО_3.2_ПТК (Горный, Щучье)'
                    
                    when c.name  ='Акционерное общество "КОНЦЕРН ТИТАН-2"' 
                    and cs.name='Строительство ПТК'
                    and co.name ='Утилизация (Марадыковский)'
                    and (division in('Утилизация Марадыковский', ';','') 
                         or division is Null)
                    then '9_ДПО_3.3_ПТК (Марадыковскй, Камбарка)'

                    when c.name  ='Акционерное общество "КОНЦЕРН ТИТАН-2"' 
                    and cs.name='Строительство ПТК'
                    and co.name ='Утилизация (Западная Сибирь)'
                    and (division in('Дирекция "Прорыв"','',';')
                      or division is Null)
                    then '10_ДПО_3.4_ПТК (Западная Сибирь)'
                    
                    when c.name  ='Акционерное общество "КОНЦЕРН ТИТАН-2"' 
                    and cs.name='НИТИ'
                    and (co.name is NUll or co.name in('НИТИ','','-'))
                    and (division in ('Дирекция НИТИ','',';')
                      or division is Null )
                    then '11_ДПО_3.5_НИТИ'
                    
                    when c.name  ='Акционерное общество "КОНЦЕРН ТИТАН-2"' 
                    and cs.name='ФДРЦ'
                     and (co.name is NUll or co.name in('ФДРЦ','','-'))
                    and division = 'Дирекция ФДРЦ'
                    then '12_ДРНАП_4.1_ФДРЦ'
                    
                    when c.name  ='Акционерное общество "КОНЦЕРН ТИТАН-2"' 
                    and cs.name='СЕНЕЖ'
                     and (co.name is NUll or co.name in('СЕНЕЖ','','-'))
                    and division = 'Дирекция СЕНЕЖ'
                    then '13_ДРНАП_4.2_СЕНЕЖ'
                    
                    when c.name  ='Акционерное общество "КОНЦЕРН ТИТАН-2"' 
                    and cs.name='НМИЦ Вишневского'
                    and (co.name is NUll or co.name in('НМИЦ им А.В. Вишневского','',';'))
                    and (division in('Дирекция НМИЦ Вишневского','',';')
                         or division is Null)
                    then '14_ДРНАП_4.3_Вишневского'
                    
                                        
                    when c.name  ='Акционерное общество "КОНЦЕРН ТИТАН-2"' 
                    and cs.name='Норильский Никель'
                    and co.name ='Норильский Никель'
                    and division = 'Дирекция Норильск'
                    then '15_ДРНАП_4.4_Норильск'
                    
                    when c.name  ='Акционерное общество "КОНЦЕРН ТИТАН-2"' 
                    and cs.name='Строительство ПТК'
                    and co.name  in('Утилизация (Горный)','Утилизация (Щучье)')
                    and division = 'Дирекция по строительству объектов ПТК'
                    then '16_ПТК (Горный, Щучье)_5_ПТК (Горный, Щучье)'
                                        
                    when c.name  ='Акционерное общество "КОНЦЕРН ТИТАН-2"' 
                    and cs.name='Строительство ПТК'
                    and co.name ='Утилизация (Марадыковский)'
                    then '17_ПТК (Марадыковскй, Камбарка)_6_ПТК (Марадыковскй, Камбарка)'
                    
                    when c.name  ='АО "Монтажно-строительное управление №90"' 
                    and cs.name='ЛАЭС-2'
                    and (co.name is NUll or co.name in('ЛАЭС-2','',';'))
                    then '18_МСУ-90_7.1_ЛАЭС-2'

                    when c.name  ='АО "Монтажно-строительное управление №90"' 
                    and cs.name='ФДРЦ'
                    and (co.name is NUll or co.name in('ФДРЦ','',';'))
                    then '19_МСУ-90_7.2_ФДРЦ'
                    

                    when c.name  ='АО "Монтажно-строительное управление №90"' 
                    and cs.name='НИТИ'
                    and (co.name is NUll or co.name in('НИТИ','',';'))
                    then '19_МСУ-90_7.3_НИТИ'
                    
                    when c.name  ='АО "Монтажно-строительное управление №90"' 
                    and cs.name='РАО'
                    and (co.name is NUll or co.name in('РАО','',';'))
                    then '19_МСУ-90_7.4_РАО'
                    
                    when c.name ='АО "Монтажно-строительное управление №90"' 
                    and cs.name='Прорыв'
                    and (co.name is NUll or co.name in('Прорыв','',';'))
                    then '22_МСУ-90_7.5_Прорыв'
                    
                    when c.name  ='Акционерное общество "Сосновоборэлектромонтаж"'
                    and cs.name='ЛАЭС-2'
                    and division !='СЭМ, МЗУ'
                    then '23_АО СЭМ_8.1_ЛАЭС-2'
                                        
                    when c.name  ='Акционерное общество "Сосновоборэлектромонтаж"'
                    and cs.name='Прорыв'
                    and division !='СЭМ, МЗУ'
                    then '24_АО СЭМ_8.2_Прорыв'
                    
                    when c.name  ='Акционерное общество "Сосновоборэлектромонтаж"'
                    and cs.name='Курская АЭС-2'
                    and division !='СЭМ, МЗУ'
                    then '25_АО СЭМ_8.3_Курская АЭС-2'
                    
                    when c.name  ='Акционерное общество "Сосновоборэлектромонтаж"'
                    and cs.name='Строительство ПТК'
                    and division !='СЭМ, МЗУ'
                    then '26_АО СЭМ_8.4_Строительство ПТК'
                    
                    when c.name  ='Акционерное общество "Сосновоборэлектромонтаж"'
                    and cs.name='НИТИ'
                    and division !='СЭМ, МЗУ'
                    then '27_АО СЭМ_8.5_НИТИ'
                    
                    when c.name  ='Акционерное общество "Сосновоборэлектромонтаж"'
                    and cs.name='ЦКП "СКИФ"'
                    and division !='СЭМ, МЗУ'
                    then '28_АО СЭМ_8.6_ЦКП "СКИФ"'
                    
                    when c.name  ='Акционерное общество "Сосновоборэлектромонтаж"'
                    and cs.name='ФДРЦ'
                    and division !='СЭМ, МЗУ'
                    then '29_АО СЭМ_8.7_ЦКП "ФДРЦ'
                                        
                    when c.name  ='Акционерное общество "Сосновоборэлектромонтаж"'
                    and cs.name='Эль-Дабаа'
                    and division !='СЭМ, МЗУ'
                    then '30_АО СЭМ_8.8_Эль-Дабаа'
                                                            
                    when c.name  ='Акционерное общество "Сосновоборэлектромонтаж"'
                    and cs.name='РАО'
                    and division !='СЭМ, МЗУ'
                    then '31_АО СЭМ_8.9_РАО'
                    
                    when c.name  ='Акционерное общество "Сосновоборэлектромонтаж"'
                    and cs.name='Сенеж'
                    and division !='СЭМ, МЗУ'
                    then '32_АО СЭМ_8.10_Сенеж'
                    
                    when c.name  ='Публичное акционерное общество «Северное управление строительства»'
                    and cs.name='ЛАЭС-2'
                    then '33_ПАО СУС_9.1_ЛАЭС-2'
                    
                    when c.name  ='Публичное акционерное общество «Северное управление строительства»'
                    and cs.name='Прорыв'
                    then '34_ПАО СУС_9.2_Прорыв'
                                        
                    when c.name  ='Публичное акционерное общество «Северное управление строительства»'
                    and cs.name='Строительство ПТК'
                    then '35_ПАО СУС_9.3_Строительство ПТК'      

                    when c.name  ='Публичное акционерное общество «Северное управление строительства»'
                    and cs.name='Программа не выделена'
                    then '36_ПАО СУС_9.4_Программа не выделена'             
                    else Null
                    
                END as Directory
                
            FROM dbo.app_internal_order_plan_item app
            left join dbo.contractor c on c.id = app.organization_id 
            left join dbo.construction_site cs on cs.id = app.site_id 
            left join dbo.construction_object co on co.id = app.construction_object_id
            left join dbo.app_enlarged_nomenclature en on en.id = app.nomenclature_id 
            left join dbo.measure_unit mu on mu.id = app.measure_unit_id
            left join dbo.[month] m on m.id= app.month_id 
            left join dbo.inventory_item_type iit on iit.id = app.inventory_item_type_id
        """
        df = pd.read_sql(query, conn)
        conn.close()
        print()
        print(df.dtypes)
        
        df['construction_name'] = df['construction_name'].astype(str)
        return df  # Возвращаем DataFrame


    @task(task_id="truncate_table", retries=2)
    def truncate_table():
        with Client(host=clickhouse_host, port=clickhouse_port, user=clickhouse_user, password=clickhouse_password) as client:
            client.execute(f'TRUNCATE TABLE IF EXISTS {clickhouse_table_name}')


    @task(task_id="create_table", retries=2)
    def create_table():
        with Client(host=clickhouse_host, port=clickhouse_port, user=clickhouse_user, password=clickhouse_password) as client:
            client.execute(f'''
                CREATE TABLE IF NOT EXISTS {clickhouse_table_name}
                (
                    `id` UInt32,
                    `contractor_name` Nullable(String),
                    `site_name` String,
                    `procurement_plan_type` Nullable(String),
                    `construction_name` String,
                    `enlarged_nomenclature` Nullable(String),
                    `measure` Nullable(String),
                    `nomenclature` Nullable(String),
                    `reserve_document` Nullable(String),
                    `month_name` String,
                    `Dt` Nullable(String),
                    `division` Nullable(String),
                    `type_nomen` Nullable(String),
                    `previous_period_fact_exists` Nullable(String),
                    `planned` Nullable(UInt8),
					`Year` Nullable(String),
                    `quantity` Nullable(Float64),
                    `cost` Nullable(Float64),
                    `Directory` Nullable(String),
					INDEX contractor_name_idx contractor_name TYPE set(0) GRANULARITY 1,
                    INDEX site_name_idx site_name TYPE set(0) GRANULARITY 1,
                    INDEX dt_idx Dt TYPE set(0) GRANULARITY 1,
                    INDEX procurement_plan_type_idx procurement_plan_type TYPE set(0) GRANULARITY 1,
                    INDEX enlarged_nomenclature_idx enlarged_nomenclature TYPE set(0) GRANULARITY 1,
                    INDEX measure_idx measure TYPE set(0) GRANULARITY 1,
                    INDEX month_name_idx month_name TYPE set(0) GRANULARITY 1,
					INDEX Directory_idx Directory TYPE set(0) GRANULARITY 1
                )
                ENGINE = MergeTree
                ORDER BY id
                SETTINGS index_granularity = 8192;
            ''')


    @task(task_id="insert_into_clickhouse", retries=2)
    def insert_into_clickhouse(df):
        with Client(host=clickhouse_host, port=clickhouse_port, user=clickhouse_user, password=clickhouse_password) as client:
            data = [(index, *row) for index, row in df.iterrows()]
            client.execute(
                f'INSERT INTO {clickhouse_table_name} (id, contractor_name, site_name, procurement_plan_type, construction_name, enlarged_nomenclature, measure, nomenclature, reserve_document,month_name, Dt, division, type_nomen,previous_period_fact_exists, planned, Year, quantity, cost,Directory) VALUES',
                data
            )


    @task(task_id="count_row_click", retries=2)
    def count_row_click():
        with Client(host=clickhouse_host, port=clickhouse_port, user=clickhouse_user, password=clickhouse_password) as client:
            result = client.execute(f'SELECT COUNT(*) as row_count FROM {clickhouse_table_name}')
            print(result)  # Логируем результат для отладки
            return result[0][0]  # Возвращаем количество строк

 

    ms_df = execute_sql()
    count_row_ms()
    create_table() >> truncate_table() >> insert_into_clickhouse(ms_df) >> count_row_click()
