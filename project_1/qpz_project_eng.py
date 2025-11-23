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

clickhouse_table_name = 'pixbi.qpz_project'

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 5
}

with DAG('qpz_project', 
         default_args=default_args, schedule='00 1 * * *') as dag: 

    @task(task_id="truncate_table", retries=2)
    def truncate_table():
        with Client(host=clickhouse_host, port=clickhouse_port, user=clickhouse_user, password=clickhouse_password) as client:
            client.execute(f'TRUNCATE TABLE IF EXISTS {clickhouse_table_name}')

    @task(task_id="insert_into_clickhouse", retries=2)
    def insert_into_clickhouse():       
        with Client(host=clickhouse_host, port=clickhouse_port, user=clickhouse_user, password=clickhouse_password) as client:
            query = f"""
                INSERT INTO {clickhouse_table_name}
            SELECT
                    rowNumberInAllBlocks() as id,
                    contractor_name,
                    division,
                    multiIf(Year is Null,'',Year='None','',CONCAT(Year,' год')) Year,
                    site_name,
                    Dt,
                    procurement_plan_type,
                    construction_name,
                    enlarged_nomenclature,
                    measure,
                    multiIf(isNUll(measure),'пусто',measure) as measure2,
                    concat(toString(enlarged_nomenclature), ' ', toString(measure)) AS EnlNomen,
                    concat(toString(enlarged_nomenclature), ' ', toString(measure)) AS EnlNomen2,
                    multiIf(isNUll(EnlNomen),concat('',toString(measure2)),EnlNomen) as s,
                    month_name,
                    month_number,
                    nomenclature,
                    reserve_document,
                    type_nomen,
                    multiIf(previous_period_fact_exists = '', 'Нет', previous_period_fact_exists) previous_period_fact_exists,
                    planned,
                    Directory,
                    SUM(e_q) AS e_q,
                    SUM(e_c) AS e_c,
                    SUM(p_q) AS p_q,
                    SUM(p_c) AS p_c,
                    multiIf(p_c = 0, 0, 1) AS p_c_n,
                    SUM(f_q) AS f_q,
                    multiIf(previous_period_fact_exists='Нет',f_q,0) f_q_2,
                    SUM(f_c) AS f_c,
                    multiIf(previous_period_fact_exists='Нет',f_c,0) f_c_2,
                    multiIf(e_c = 0, p_c, e_c) AS e_p_c,
                    multiIf(e_q = 0, p_q, e_q) AS e_p_q,
                    SUM(total_cost) AS total_cost,
                    sum(total_quantity) AS total_quantity,
                    SUM(f_c_1_5) AS f_c_1_5,
                    multiIf(isNULL(previous_period_fact_exists),0,f_c_1_5) f_c_1_5_2,
                    SUM(f_q_1_5) AS f_q_1_5,
                    multiIf(isNULL(previous_period_fact_exists),0,f_q_1_5) f_q_1_5_2

                from(
                    Select
                        contractor_name,
                        division,
                        site_name,
                        Dt,
                        procurement_plan_type,
                        construction_name,
                        enlarged_nomenclature,
                        measure,
                        ml.month_name AS month_name,
                        ml.month_number AS month_number,
                        NULL AS nomenclature,
                        NULL AS reserve_document,
                        NULL AS type_nomen,
                        NULL AS planned,
                        NULL AS previous_period_fact_exists,
                        Directory,
                        Year,
                        multiIf(isNaN(quantity), 0, quantity) AS e_q,
                        multiIf(isNaN(cost), 0, cost) AS e_c,
                        0 AS p_q,
                        0 AS p_c,
                        0 AS f_q,
                        0 AS f_c,
                        0 AS total_cost,
                        0 AS total_quantity,
                        0 as f_c_1_5,
                        0 as f_q_1_5
                    FROM pixbi.PLAN_APP_ENL
                    LEFT JOIN pixbi.months_list ml ON pixbi.PLAN_APP_ENL.month_name = ml.month_name
                    UNION ALL
                select 
                    contractor_name,
                    division,
                    site_name,
                    Dt,
                    procurement_plan_type,
                    construction_name,
                    enlarged_nomenclature,
                    measure,
                    month_name,
                    ml.month_number,
                    nomenclature,
                    reserve_document,
                    type_nomen,
                    planned, 
                    previous_period_fact_exists,
                    Directory,
                    Year,
                    0 AS e_q,
                    0 AS e_c,
                    multiIf(isNaN(quantity), 0, quantity) AS p_q,
                    multiIf(isNaN(cost), 0, cost) AS p_c,
                    0 AS f_q,
                    0 AS f_c,
                    0 AS total_cost,
                    0 AS total_quantity,
                    0 AS f_c_1_5,
                    0 AS f_q_1_5
                    FROM pixbi.PLAN_APP pa
                    LEFT JOIN pixbi.months_list ml ON month_name = ml.month_name
                UNION ALL
                    SELECT
                        fa.contractor_name,
                        fa.division,
                        fa.site_name,
                        fa.Dt,
                        fa.procurement_plan_type,
                        fa.construction_name,
                        fa.enlarged_nomenclature,
                        fa.measure,
                        ml.month_name,
                        ml.month_number,
                        fa.nomenclature,
                        fa.reserve_document,
                        NULL AS type_nomen,
                        NULL AS planned,
                        pa.previous_period_fact_exists,
                        Directory,
                        fa.Year,
                        0 AS e_q,
                        0 AS e_c,
                        0 AS p_q,
                        0 AS p_c,
                        multiIf(isNaN(quantity), 0, quantity) AS f_q,
                        multiIf(isNaN(cost), 0, cost) AS f_c,
                        0 AS total_cost,
                        0 AS total_quantity,
                        0 as f_c_1_5,
                        0 as f_q_1_5
                    FROM pixbi.FACT_APP fa
                    LEFT JOIN pixbi.months_list ml ON fa.month_name = ml.month_name
                    LEFT JOIN (
                        SELECT DISTINCT previous_period_fact_exists,
                            concat(toString(reserve_document), toString(nomenclature)) AS key_column
                        FROM pixbi.PLAN_APP
                    ) pa ON concat(toString(fa.reserve_document), toString(fa.nomenclature)) = pa.key_column

                    UNION ALL
                    
                    SELECT
                        pa.contractor_name,
                        pa.division,
                        pa.site_name,
                        pa.Dt,
                        pa.procurement_plan_type,
                        pa.construction_name,
                        pa.enlarged_nomenclature,
                        pa.measure,
                        ml.month_name,
                        ml.month_number,
                        pa.nomenclature,
                        pa.reserve_document,
                        pa.type_nomen,
                        pa.planned,
                        pa.previous_period_fact_exists,
                        pa.Directory,
                        pa.Year,
                        0 AS e_q,
                        0 AS e_c,
                        0 AS p_q,
                        0 AS p_c,
                        0 AS f_q,
                        0 AS f_c,
                        SUM(multiIf(isNaN(pa_inner.cost), 0, pa_inner.cost)) AS total_cost,
                        SUM(multiIf(isNaN(pa_inner.quantity), 0, pa_inner.quantity)) AS total_quantity,
                        0 as f_c_1_5,
                        0 as f_q_1_5
                    FROM pixbi.PLAN_APP pa
                    LEFT JOIN pixbi.months_list ml ON pa.month_name = ml.month_name
                    LEFT JOIN pixbi.FACT_APP f ON pa.reserve_document = f.reserve_document AND pa.nomenclature = f.nomenclature
                    LEFT JOIN pixbi.months_list ml_fact ON f.month_name = ml_fact.month_name
                    LEFT JOIN pixbi.months_list ml_plan ON pa.month_name = ml_plan.month_name
                    INNER JOIN (
                        SELECT DISTINCT concat(toString(contractor_name), toString(site_name)) AS key_column
                        FROM pixbi.PLAN_APP_ENL
                    ) pae ON concat(toString(pa.contractor_name), toString(pa.site_name)) = pae.key_column
                    LEFT JOIN pixbi.FACT_APP pa_inner ON pa.reserve_document = pa_inner.reserve_document
                        AND ml_fact.month_number < ml_plan.month_number
                        AND ml_fact.month_number BETWEEN 1 AND 5
                    GROUP BY
                        pa.contractor_name,
                        pa.division,
                        pa.site_name,
                        pa.Dt,
                        pa.procurement_plan_type,
                        pa.construction_name,
                        pa.enlarged_nomenclature,
                        pa.measure,
                        ml.month_name,
                        ml.month_number,
                        pa.nomenclature,
                        pa.reserve_document,
                        pa.type_nomen,
                        pa.planned,
                        pa.previous_period_fact_exists,
                        pa.Year,
                        pa.Directory,
                        pa.quantity,
                        pa.cost
                    union ALL 
                    

                SELECT 
                        fa.contractor_name,
                        fa.division,
                        fa.site_name,
                        fa.Dt,
                        fa.procurement_plan_type,
                        fa.construction_name,
                        fa.enlarged_nomenclature,
                        fa.measure,
                        ml.month_name,
                        ml.month_number,
                        fa.nomenclature,
                        fa.reserve_document,
                        NULL AS type_nomen,
                        NULL AS planned,
                        NULL as previous_period_fact_exists,
                        fa.Directory,
                        fa.Year,
                        0 AS e_q,
                        0 AS e_c,
                        0 AS p_q,
                        0 AS p_c,
                        0 as f_q,
                        0 as f_c,
                        0 AS total_cost,
                        0 AS total_quantity,
                        SUm(multiIf(isNaN(fa.cost), 0, fa.cost)) AS f_c_1_5,
                        sum(multiIf(isNaN(fa.quantity), 0, fa.quantity)) AS f_q_1_5
                FROM pixbi.FACT_APP fa
                left join pixbi.months_list ml on ml.month_name = fa.month_name
                where ml.month_number between 1 and 5
                and fa.reserve_document in (
                    select DISTINCT pa.reserve_document from pixbi.PLAN_APP pa
                    left join pixbi.months_list ml on ml.month_name = pa.month_name
                    group by pa.reserve_document,ml.month_number
                    having ml.month_number between 6 and 12 and sum(pa.cost)>0)
                group by       
                        fa.contractor_name,
                        fa.division,
                        fa.Year,
                        fa.site_name,
                        fa.Dt,
                        fa.procurement_plan_type,
                        fa.construction_name,
                        fa.enlarged_nomenclature,
                        fa.measure,
                        ml.month_name,
                        ml.month_number,
                        fa.nomenclature,
                        fa.reserve_document,
                        type_nomen,
                        planned,
                        previous_period_fact_exists,
                        Directory
                        )
                    group by 
                    contractor_name,
                    division,
                    Year,
                    site_name,
                    Dt,
                    procurement_plan_type,
                    construction_name,
                    enlarged_nomenclature,
                    measure,
                    month_name,
                    month_number,
                    nomenclature,
                    reserve_document,
                    type_nomen,
                    previous_period_fact_exists,
                    Directory,
                    planned 
            """
            client.execute(query)

    truncate_table() >> insert_into_clickhouse()
   
