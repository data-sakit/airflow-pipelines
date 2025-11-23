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
import seaborn as sns







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

with DAG('email_parsec_Egypt', default_args=default_args, schedule='0 7 * * *'):
    @task(task_id="fetch_data")
    def fetch_data():

        with Client(host=clickhouse_host, port=clickhouse_port, user=clickhouse_user, password=clickhouse_password) as client:
            query = """
            with total_data as (select
                id,
                trim(splitByChar('/', ifNull(path, ''))[1]) AS "Площадка",
                splitByChar('/', ifNull(path, ''))[2] AS "Организация",
                arrayStringConcat(arraySlice(splitByChar('/', ifNull(path, '')), 3, 5), '//')AS "Участок",
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
                Flag as "."
            from pixbi.parsec_Eg p
            left join pixbi.parsec_profession_directory ppd on lowerUTF8(p.position_name) = ppd.name
            order by id)
            select *,
            CASE
                WHEN (lowerUTF8("Организация") LIKE '%кт 2%'
                    OR lowerUTF8("Организация") LIKE '%мсу-90%'
                    OR lowerUTF8("Организация") LIKE '%сэм%'
                    OR lowerUTF8("Организация") LIKE '%сус%'
                    OR lowerUTF8("Организация") LIKE '%титан-2%')
                THEN 'Собственные силы'
                WHEN (lowerUTF8("Организация") LIKE '%allam%')
                THEN 'Аутстаферы'
                ELSE 'Субподряд'
            end as group_type
            from total_data
            where `.`='Yesterday'
            """
            result = client.execute(query, with_column_types=True)
            

            columns = [col[0] for col in result[1]]  # Извлечение названий столбцов
            df = pd.DataFrame(result[0], columns=columns)
            return df  # Возвращаем DataFrame

    @task(task_id="generate_plot_pc_1")
    def generate_plot_pc_1(df):
        unique_counts = df.groupby(['Площадка','group_type','Shift'])['Name'].nunique().reset_index()
        unique_counts.rename(columns={'Name':'Количество сотрудников'}, inplace=True)


        order = unique_counts.groupby('Площадка')['Количество сотрудников'].sum().sort_values(ascending=False).index


        col_order = ['Morning', 'Evening']


        fig, axes = plt.subplots(1, len(col_order), figsize=(14, 4), sharey=True)

        for i, shift in enumerate(col_order):
            ax = axes[i]
            data_shift = unique_counts[unique_counts['Shift'] == shift]
            palette={
                'Аутстаферы':'#F5A623',
                'Собственные силы':'#4A90E2',
                'Субподряд':'#90ED7D'
            }  
            sns.barplot(
                data=data_shift,
                y='Площадка',
                x='Количество сотрудников',
                hue='group_type',
                order=order,
                ax=ax,
                width=0.2,
                palette=palette
            )
            ax.set_title(shift, fontsize=10)

            for p in ax.patches:
                width = p.get_width()
                if width > 0:
                    ax.text(
                        width + 0.1,
                        p.get_y() + p.get_height()/2,
                        int(width),
                        va='center',
                        fontsize=6
                    )
            if i == 0:
                ax.set_ylabel('Площадка')
            else:
                ax.set_ylabel('')
            ax.set_xlabel('Количество сотрудников')
            ax.legend(title='Тип группы', fontsize=6, title_fontsize=6, loc='lower right') 
        plt.suptitle('Количество сотрудников по площадкам', fontsize=12)

        plt.tight_layout()
        buffer = io.BytesIO()
        plt.savefig(buffer, format='png', bbox_inches='tight')
        buffer.seek(0)
        plt.close()

        img_base64_1 = base64.b64encode(buffer.getvalue()).decode('utf-8')
        return img_base64_1

    @task(task_id="generate_plot_pc_2")
    def generate_plot_pc_2(df):

        df['Не прошли проверку_флаг'] = df['Не прошли проверку'] == 'Не прошли проверку'


        counts = df.groupby(['Площадка','group_type','Shift'])['Не прошли проверку_флаг'].sum().reset_index()
        counts.rename(columns={'Не прошли проверку_флаг':'Количество не прошедших'}, inplace=True)

        order = counts.groupby('Площадка')['Количество не прошедших'].sum().sort_values(ascending=False).index


        col_order = ['Morning', 'Evening']

        fig, axes = plt.subplots(1, len(col_order), figsize=(14, 4), sharey=True)

        for i, shift in enumerate(col_order):
            ax = axes[i]
            data_shift = counts[counts['Shift'] == shift]
            palette={
                'Аутстаферы':'#F5A623',
                'Собственные силы':'#4A90E2',
                'Субподряд':'#90ED7D'
            }            
            sns.barplot(
                data=data_shift,
                y='Площадка',
                x='Количество не прошедших',  # <-- правильно
                hue='group_type',
                order=order,
                ax=ax,
                width=0.2,
                palette=palette
            )
            ax.set_title(shift, fontsize=10)
            

            for p in ax.patches:
                width = p.get_width()
                if width > 0:
                    ax.text(
                        width + 0.1,
                        p.get_y() + p.get_height()/2,
                        int(width),
                        va='center',
                        fontsize=6
                    )
            if i == 0:
                ax.set_ylabel('Площадка')
            else:
                ax.set_ylabel('')
            ax.set_xlabel('Количество не прошедших')
            

            ax.legend(title='Тип группы', fontsize=6, title_fontsize=6, loc='lower right')

        plt.suptitle('Количество сотрудников, не прошедших проверку, по площадкам', fontsize=12)
        plt.tight_layout()
        buffer = io.BytesIO()
        plt.savefig(buffer, format='png', bbox_inches='tight')
        buffer.seek(0)
        plt.close()

        img_base64_2 = base64.b64encode(buffer.getvalue()).decode('utf-8')
        return img_base64_2
    

    @task(task_id="send_email")
    def send_email(img_base64_1,img_base64_2):

        def decode_base64_to_image(base64_string):
            image_data = base64.b64decode(base64_string)
            return Image.open(io.BytesIO(image_data))


        img1 = decode_base64_to_image(img_base64_1)
        img2 = decode_base64_to_image(img_base64_2)


        def add_white_background(image):
            if image.mode in ('RGBA', 'LA') or (image.mode == 'P' and 'transparency' in image.info):

                background = Image.new('RGB', image.size, (255, 255, 255))
                background.paste(image, mask=image.split()[-1])  # Используем альфа-канал как маску
                return background
            else:
                return image.convert('RGB')


        img1 = add_white_background(img1)
        img2 = add_white_background(img2)


        pdf_filename = "/opt/airflow/dags/QPZ_ETL_Image/Парсек_Египет.pdf"  # Укажите нужный путь


        img1.save(pdf_filename, format="PDF", resolution=100.0, save_all=True,append_images=[img2])

        yesterday = (datetime.now() - timedelta(days=1)).strftime("%d.%m.%Y")

        html_content = f"""
        <html>
            <body>
                <h1>Отчет Парсек Египет</h1>
                <p>Нажмите <a href="https://pixbi.titan2.ru/shared/view/a2efc8fb-ec1e-4eba-85db-ef16835a4d73?ticket=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJodHRwOi8vc2NoZW1hcy54bWxzb2FwLm9yZy93cy8yMDA1LzA1L2lkZW50aXR5L2NsYWltcy9uYW1lIjoiVGlja2V0VXNlciIsIm5iZiI6MTc1ODExMDc1MiwiZXhwIjoyMDczNjQzNTUyfQ.fkJK_KyXpItPVH9aSqq0g3V8BtQPtE2vOlY87N3jRRY">здесь</a>, чтобы посмотреть отчет.</p>
                <h2>За {yesterday}:</h2>
                <table align="center" width="100%">
                    <tr align="center">
                        <td>
                            <img src="data:image/png;base64,{img_base64_1}" alt="График1" width="400" height="300">
                        </td>
                    </tr>
                    <tr><td style="height:30px;">&nbsp;</td></tr>
                    <tr align="center">
                        <td>
                            <img src="data:image/png;base64,{img_base64_2}" alt="График1" width="400" height="300">
                        </td>
                    </tr>
                </table>

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

            to=['t.chervyakova@titan2.ru','roman.berezenkov@t2-egypt.com','vladimir.petrik@t2-egypt.com','a.prosvirnina@titan2.ru','e.petrische@titan2.ru','v.khrisanfova@titan2.ru','artem.telnov@t2-egypt.com','tarek.mukhammed@t2-egypt.com','amin.mukhammad@t2-egypt.com','r.berezenkov@titan2.ru','regina.gazafarova@t2-egypt.com','aleksandr.zhidkov@t2-egypt.com','kirill.semenov@t2-egypt.com','yury.tsiolta@t2-egypt.com','s.dzyuba@titan2.ru','andrei.pavliuk@t2-egypt.com','valentina.nikiforova@t2-egypt.com','a.serzhantova@titan2.ru','v.shaiakhmetov@t2-egypt.com','s.zimina@titan2.ru','a.yafaeva@titan2.ru','s.kolesnikova@titan2.ru','v.khrisanfova@titan2.ru','n.mohova@titan2.ru','g.yalhovskih@titan2.ru','k.openko@titan2.ru','e.mezhiritskaya@titan2.ru','i.elnikova@titan2.ru','s.mamedov@titan2.ru'],
            subject='Ежедневный отчет',
            html_content=html_content,
            files=[pdf_filename]  # Передаем только путь к файлу
        )
        email_task.execute(context={})


    d_1 = fetch_data()
    img_base64_1 = generate_plot_pc_1(d_1)
    d_2 = fetch_data()
    img_base64_2 = generate_plot_pc_2(d_2)
    

    send_email(img_base64_1,img_base64_2)