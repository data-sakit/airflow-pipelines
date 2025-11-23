from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.operators.email_operator import EmailOperator
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import base64
import io
from PIL import Image
from io import BytesIO
from PIL import Image, ImageOps
from datetime import datetime,timedelta
import pygwalker as pyg






from clickhouse_driver import Client


clickhouse_host = Variable.get('CH_HOST')
clickhouse_port = Variable.get('CH_PORT')
clickhouse_user = Variable.get('CH_USER')
clickhouse_password = Variable.get('CH_PASSWORD')


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 2,
}

with DAG('email_parsec_Egypt_pygwalker', default_args=default_args, schedule='30 7 * * *'):
    @task(task_id="fetch_data")
    def fetch_data():

        with Client(host=clickhouse_host, port=clickhouse_port, user=clickhouse_user, password=clickhouse_password) as client:
            query = """with total_data as (select
                        id,
                        trim(splitByChar('/', ifNull(path, ''))[1]) AS "Площадка",
                        trim(splitByChar('/', ifNull(path, ''))[2]) AS "Организация",
                        trim(splitByChar('/', ifNull(path, ''))[3]) AS "Участок",
                        --arrayStringConcat(arraySlice(splitByChar('/', ifNull(path, '')), 3, 5), '//')AS "Участок",
                        outstaff "Outstaff",
                        Name "Name",
                        --replaceRegexpOne(PositionName, '^([^ ]+).*', '\\1') "Profession",
                        --replaceRegexpOne(PositionName, '\\s(\\d+\\s*разряд)', ' / \\1') "Profession",
                        position_name "Profession",
                        CASE
                        WHEN match(lowerUTF8(p.position_name),'генераль|руководитель|главный инженер|главный маркшейдер|главный механик|главный энергетик|директор|заместитель|      начальник') THEN 'Руководители'
                            WHEN match(lowerUTF8(p.position_name),'аналитик|старший|специалист|ведущий|инженер|менеджер|помощник|программист|проектировщик|разработчик|системный архитектор') THEN 'Специалисты'
                            WHEN match(lowerUTF8(p.position_name),'мастер|прораб') THEN 'Линейный персонал'											
                            WHEN match(lowerUTF8(p.position_name),'арматурщик|уборщи|стропальщик|электрогазосварщик|электромонтажник|электромонтер|штукатур|слесарь|рабочий|бетонщик|бригадир|водитель|крановщик|кровельщик|куратор|лаборант|маляр|машинист|механизатор|монолитчик|монтажник|изолировщик|дробеструйщик|оператор|отделочник|плиточник|плотник-бетонщик|подсобник|подсобный|пожарный|сборщик|сварной|сварщик') THEN 'Рабочие'
                            ELSE ppd.group
                        END AS category,
                        multiIf(ppd.group='',category,ppd.group)"Profession(группа)",
                        Shift "Shift",
                        Date "Date",
                        CASE 
                            WHEN path like '%Уволенные'
                            THEN 'уволен'
                            ELSE 'работает'
                        END AS "Cтатус",
                        --toMonth(toDate(Entry_datetime))"Месяц",
                        toYear(toDate(Entry_datetime))"Год",
                        multiIf(Shift ='Morning',1,0) "Morning",
                        multiIf(Shift ='Evening',1,0) "Evening",
                        
                        multiIf(
                                isNull(Entry_datetime) OR Entry_datetime IN ('-', ''),NULL, 
                            parseDateTimeBestEffort(Entry_datetime)-toIntervalHour(3)) AS "Date и время входа", 
                        
                        multiIf(
                                isNull(Exit_datetime) OR Exit_datetime IN ('-', ''),NULL, 
                            parseDateTimeBestEffort(Exit_datetime)-toIntervalHour(3)) AS  "Date и время выхода",
                        
                        concat(
                            toString(multiIf(
                                isNull(Entry_datetime) OR Entry_datetime IN ('-', ''), NULL,
                                parseDateTimeBestEffort(Entry_datetime) - toIntervalHour(3)
                            )),
                            ' / ',
                            toString(multiIf(
                                isNull(Exit_datetime) OR Exit_datetime IN ('-', ''), NULL,
                                parseDateTimeBestEffort(Exit_datetime) - toIntervalHour(3)
                            ))
                        ) AS "Вход / Выход",
                        
                        
                        multiIf(isNull(Entry_datetime) OR Entry_datetime IN ('-', ''), 'Не прошли проверку', 'Есть вход')"Статус входа",
                        multiIf(isNull(Exit_datetime) OR Exit_datetime IN ('-', ''), 'Не прошли проверку', 'Есть выход')"Статус выхода",
                        multiIf(Error='None','',Error)"Описание ошибки",
                        multiIf(Error='None','','Не прошли проверку')"Не прошли проверку",
                        formatDateTime(toDate(Date), '%d.%m.%y') "Период",
                        Differ_entry_exit as  "Длительность в ч:м",
                        average_hour as  "Среднее время в ч",
                        Flag
                    from pixbi.parsec_RF p
                    left join pixbi.parsec_profession_directory ppd on lowerUTF8(p.position_name) = ppd.name
                    order by id)
                    select *,
                    CASE
                        WHEN (lowerUTF8("Организация") LIKE '%кт2%'
                            OR lowerUTF8("Организация") LIKE '%мсу-90%'
                            OR lowerUTF8("Организация") LIKE '%сэм%'
                            OR lowerUTF8("Организация") LIKE '%сус%'
                            OR lowerUTF8("Организация") LIKE '%титан-2%')
                        THEN 'Собственные силы'
                        WHEN (lowerUTF8("Outstaff") = 'да')
                        THEN 'Аутстаферы'
                        ELSE 'Субподряд'
                    end as group_type
                    from total_data
                    where Flag='Yesterday'"""

            result = client.execute(query, with_column_types=True)
            

            columns = [col[0] for col in result[1]]  # Извлечение названий столбцов
            df = pd.DataFrame(result[0], columns=columns)
            return df  # Возвращаем DataFrame

    @task(task_id="generate_pygwalker_report")
    def generate_pygwalker_report(df):

        walker_html = pyg.to_html(df)

        report_filename = "/opt/airflow/dags/QPZ_ETL_Image/parsec_egypt_pygwalker.html"
        with open(report_filename, "w", encoding="utf-8") as f:
            f.write(walker_html)

        return report_filename

    @task(task_id="send_email")
    def send_email(report_filename):

        yesterday = (datetime.now() - timedelta(days=1)).strftime("%d.%m.%Y")
        html_content = f"""
        <html>
            <body>
                <h1>Отчет Парсек Египет</h1>
                <p>Нажмите <a href="https://pixbi.titan2.ru/shared/view/aa0f11a9-ae4c-4d73-8a4a-78009fd40637?ticket=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJodHRwOi8vc2NoZW1hcy54bWxzb2FwLm9yZy93cy8yMDA1LzA1L2lkZW50aXR5L2NsYWltcy9uYW1lIjoiVGlja2V0VXNlciIsIm5iZiI6MTc1NzYwNjk0NywiZXhwIjoyMDczMTM5NzQ3fQ.JB3Ohihc0LFk49Nq6itAHKhZVkHt5aN8Dhg8GkzcvzU">здесь</a>, чтобы посмотреть отчет.</p>
                <h2>За {yesterday}:</h2>

                <h3>Если ссылка не открывается:</h3>
                <ul>
                    <li>Очистите кеш вашего браузера:
                        <ul>
                            <li><strong>Google Chrome</strong>: нажмите Ctrl + Shift + Delete (Windows) или Command + Shift + Delete (Mac).</li>
                            <li><strong>Safari</strong>: откройте меню "История" → "Очистить историю".</li>
                            <li><strong>Firefox</strong>: зайдите в "Настройки" → "Конфиденциальность и защита" → "Очистить данные".</li>
                        </ul>
                    </li>
                    <li>Попробуйте обновить страницу:
                        <ul>
                            <li>Нажмите Ctrl + F5 или Shift + F5 для принудительного обновления страницы.</li>
                        </ul>
                    </li>
                    <li>Попробуйте открыть систему в другом браузере:
                        <ul>
                            <li>Если вы используете Chrome, попробуйте Firefox, Edge или Safari.</li>
                        </ul>
                    <li>Если ничего не помогло, пожалуйста, обратитесь по следующему email: <a href="mailto:s.mamedov@titan2.ru">s.mamedov@titan2.ru</a>.</li>
                    </li>
                </ul>
            </body>
        </html>

        """

        email_task = EmailOperator(
            task_id='send_email_task',
            to=['s.mamedov@titan2.ru','e.mezhiritskaya@titan2.ru','i.elnikova@titan2.ru'],

            subject='Ежедневный отчет',
            html_content=html_content,
            files=[report_filename]  # Передаем только путь к файлу
        )
        email_task.execute(context={})

  

    report_filename =  generate_pygwalker_report(fetch_data())

    send_email(report_filename)