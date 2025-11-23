from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.operators.email_operator import EmailOperator
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import base64
import math
import io
from PIL import Image
from io import BytesIO
from clickhouse_driver import Client
from PIL import Image, ImageOps
from datetime import datetime
from datetime import timedelta
from PIL import Image


clickhouse_host = Variable.get('CH_HOST')
clickhouse_port = Variable.get('CH_PORT')
clickhouse_user = Variable.get('CH_USER')
clickhouse_password = Variable.get('CH_PASSWORD')


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025,9,9),

    'retries': 2,
}


to_emails_СКИФ=['t.chervyakova@titan2.ru','d.shilovskiy@titan2.ru','aa.petrov@titan2.ru','k.lyalyuga-bizin@titan2.ru','k.openko@titan2.ru','i.elnikova@titan2.ru','s.mamedov@titan2.ru']


with DAG('solut_parsek_email_skif', default_args=default_args, schedule='15 2 * * *',catchup=False) as dag:
    
    @task(task_id="fetch_data")
    def fetch_data():

        with Client(host=clickhouse_host, port=clickhouse_port, user=clickhouse_user, password=clickhouse_password) as client:
            query = '''
                SELECT 
                    dt,
                    project,
                    division AS org,
                    employee_id,
                    full_name,
                    has_smartwatch,
                    profession,
                    assign_time,
                    return_time,
                    shift,
                    manager,
                    Break_activity_duration,
                    Break_activity_time,
                    Break_activity_percent,
                    Movements_activity_duration,
                    Movements_activity_time,
                    Movements_activity_percent,
                    Work_activity_duration,
                    Work_activity_time,
                    Work_activity_percent,
                    NULL AS PersReqNeed,
                    NULL AS SmartW_Issued,
                    NULL AS NoProtectBumper,
                    NULL AS Damaged,
                    NULL AS Lost,
                    NULL AS ChargingFailor,
                    NULL AS TechnicalFailor,
                    'manual_daily' AS source_table

                FROM pixbi.solut_activity_assign

                UNION ALL

                -- Вторая таблица: solut_activity_assign
                SELECT 
                    dt,
                    project,
                    org,
                    NULL AS employee_id,
                    NULL AS full_name,
                    NULL AS has_smartwatch,
                    NULL AS profession,
                    NULL AS assign_time,
                    NULL AS return_time,
                    NULL AS shift,
                    NULL AS manager,
                    NULL AS Break_activity_duration,
                    NULL AS Break_activity_time,
                    NULL AS Break_activity_percent,
                    NULL AS Movements_activity_duration,
                    NULL AS Movements_activity_time,
                    NULL AS Movements_activity_percent,
                    NULL AS Work_activity_duration,
                    NULL AS Work_activity_time,
                    NULL AS Work_activity_percent,
                    PersReqNeed,
                    SmartW_Issued,
                    NoProtectBumper,
                    Damaged,
                    Lost,
                    ChargingFailor,
                    TechnicalFailor,
                    'activity_assign' AS source_table
                FROM pixbi.solut_manual_daily            
            '''
            result = client.execute(query, with_column_types=True)
            columns = [col[0] for col in result[1]] 
            df = pd.DataFrame(result[0], columns=columns)
            df['dt']=pd.to_datetime(df['dt']).dt.date
            df['nonreturn']=np.where(df['return_time'].astype(str).isin(['NaT']),1,0)
            df['Total_duration'] = (
                df['Break_activity_duration'] +
                df['Movements_activity_duration'] +
                df['Work_activity_duration']
            )
            df['Total_percent'] = 100

            return df  # Возвращаем DataFrame



    d = fetch_data()

    @task(task_id="vis1_skif")
    def vis1_skif(df):
        yesterday=(datetime.now()-timedelta(days=1)).date()
        df_yes=df[(df['dt']==yesterday)&(df['project']=='ЦКП "СКИФ"')]
        df_manual=df_yes.groupby(['dt','project'],as_index=False).agg({
            'SmartW_Issued':'max',
            'NoProtectBumper':'max',
            'Damaged':'max',
            'Lost':'max'})
        сяч = max(df_yes.has_smartwatch.sum() - df_yes.assign_time.count(), 0)
        rows=[
            ['Not returned:',int(df_yes.nonreturn.sum())],
            ['Без защитного бампера',int(df_yes.NoProtectBumper.sum())],
            ['Повреждено (нельзя использовать)',int(df_manual.Damaged.sum())],
            ['Утерянные',int(df_manual.Lost.sum())],
            ['Выдано',int(df_yes.assign_time.count())],
            ['Не выдано (не пришло на получение по СЯЧ)',int(max(df_yes.has_smartwatch.sum()-df_yes.assign_time.count(),0))],
            ["Не выдано (не востребовано)", int(df_manual.SmartW_Issued.sum() 
                                            - df_yes.nonreturn.sum() 
                                            - df_manual.Damaged.sum() 
                                            - df_manual.Lost.sum() 
                                            - df_yes.assign_time.count()
                                            - сяч)]]

        total_devices=df_yes.SmartW_Issued.max()
        if pd.isna(total_devices):
            total_devices = 0
        else:
            total_devices = int(total_devices)



        yesterday_new_format = yesterday.strftime('%d.%m.%Y')
        title_text=f'Good morning! За {yesterday_new_format}г. всего {int(total_devices)} устройств, из них:'

        fig,ax = plt.subplots(figsize = (10,0.1))
        ax.axis('off')
        table=ax.table(
            cellText=rows,
            colLabels=['Наименование','Значение'])

        for (i,j),cell in table.get_celld().items():
            if i==0:
                cell.set_facecolor('#ADD8E6')
                cell.set_text_props(weight='bold')
            elif i in [6,7]:
                cell.set_text_props(color='red')
            if j==0:
                cell.set_text_props(ha='left')
            elif j==1:
                cell.set_text_props(ha='center')

        table.auto_set_font_size(False)
        table.set_fontsize(10)
        table.scale(1, 2)
        ax.set_title(title_text,fontsize=12,fontweight='bold',loc='left')

        buffer = io.BytesIO()
        plt.savefig(buffer, format='png', bbox_inches='tight')
        buffer.seek(0)
        plt.close()
        vis_1_skif = base64.b64encode(buffer.getvalue()).decode('utf-8')
        return vis_1_skif

    @task(task_id="vis2_skif")
    def vis2_skif(df):
        yesterday = (datetime.now() - timedelta(days=1)).date()
        df_yes=df[(df['dt']==yesterday)&(df['project']=='ЦКП "СКИФ"')]
        df_not_return=df_yes[df_yes['nonreturn']==1][['full_name','manager','org']].drop_duplicates().reset_index(drop=True)
        df_not_return=df_not_return.sort_values('full_name')

        df_not_return.columns = ["ФИО", "Manager", "Organization"]
        header_row = [["Not returned", "", ""]]
        column_names_row = [df_not_return.columns.tolist()]
        table_data = header_row + column_names_row + df_not_return.values.tolist()

        fig, ax = plt.subplots(figsize=(9, 6))
        ax.axis('off')

        table = ax.table(
            cellText=table_data,
            cellLoc='center',
            loc='center'
        )

        for (i, j), cell in table.get_celld().items():
            if i == 0:
                cell.set_text_props(weight='bold', fontsize=14)
                cell.visible_edges = ''
            elif i == 1:
                cell.set_facecolor('#ADD8E6')
                cell.set_text_props(weight='bold', fontsize=12)

        table.auto_set_font_size(False)
        table.set_fontsize(8)

        buffer = io.BytesIO()
        plt.savefig(buffer, format='png', bbox_inches='tight')
        buffer.seek(0)
        plt.close()
        vis_2_skif = base64.b64encode(buffer.getvalue()).decode('utf-8')
        return vis_2_skif

    @task(task_id="vis3_skif")
    def vis3_skif(df):
        yesterday = (datetime.now() - timedelta(days=1)).date()
        df_yes=df[(df['dt']==yesterday)&(df['project']=='ЦКП "СКИФ"')]

        table=(
            df_yes.groupby(['dt', 'org']).agg(
            Fact_чел=('assign_time', 'nunique'),
            Парсек_всего_участников_мониторинга_чел=('has_smartwatch', 'sum')
            ).reset_index().fillna(0).astype({
                'Fact_чел': 'int',
                'Парсек_всего_участников_мониторинга_чел': 'int'}))


        table['Не_пришли_на_получение_чел'] = (
            table['Парсек_всего_участников_мониторинга_чел'] - table['Fact_чел']
        ).astype(int)

        table = table[['org', 'Fact_чел', 'Парсек_всего_участников_мониторинга_чел','Не_пришли_на_получение_чел']]

        table= table[~(
            (table['Fact_чел'] == 0)&(table['Парсек_всего_участников_мониторинга_чел'] == 0)&(table['Не_пришли_на_получение_чел'] == 0)
        )]

        total_row = pd.DataFrame([['TOTAL:'] + list(table.iloc[:, 1:].sum())], columns=table.columns)
        table_with_total = pd.concat([table, total_row], ignore_index=True)
        column_labels = [
            'Organization',
            'Fact,чел',
            'Парсек всего\nучастников\nмониторинга, чел',
            'Did not come\nна получение,\nчел']

        if len(table_with_total)>0:
            n_rows = len(table_with_total)
            row_height = 0.4          # подберите под себя
            header_height = 0.8
            fig_height = header_height + n_rows * row_height
            fig, ax = plt.subplots(figsize=(10,fig_height))
            ax.axis('off')

            table = ax.table(
                cellText=table_with_total.values,
                colLabels=column_labels,
                cellLoc='center',
                loc='center'
            )

            
            for key, cell in table.get_celld().items():
                row, col = key
                if row == 0:
                    cell.set_text_props(fontweight='bold', fontsize=16)
                    cell.set_facecolor('#ADD8E6')
                    cell.set_height(0.2)
                    if col == 3:
                        cell.set_text_props(color='red')
                elif row == n_rows:  # Итоговая строка
                    cell.set_text_props(fontweight='bold', fontsize=16)
                elif col == 3:
                    cell.set_text_props(color='red', fontsize=16)
                elif col == 0:
                    cell.set_text_props(ha='left')
                else:
                    cell.set_text_props(fontsize=16)
            table.auto_set_font_size(False)
            table.set_fontsize(8)

        else:
            return None
        buffer = io.BytesIO()
        plt.savefig(buffer, format='png', bbox_inches='tight')
        buffer.seek(0)
        plt.close()
        vis_3_skif = base64.b64encode(buffer.getvalue()).decode('utf-8')
        return vis_3_skif

    @task(task_id="vis4_skif")
    def vis4_skif(df):
        yesterday = (datetime.now() - timedelta(days=1)).date()
        df_yes=df[(df['dt']<=yesterday)&(df['project']=='ЦКП "СКИФ"')]

        table = df_yes[['dt', 'assign_time', 'SmartW_Issued', 'Damaged', 'Lost', 'nonreturn']].copy()
        table['plan'] = table['SmartW_Issued'] - table['Damaged'] - table['Lost']

        agg = table.groupby('dt').agg(
            nonreturn_sum=('nonreturn', 'sum'),
            dt_count=('assign_time', 'count')
        ).reset_index()

        table = table.merge(agg, on='dt', how='left')[['dt', 'plan', 'dt_count']].sort_values('dt')
        table = (
            table.groupby('dt')
            .agg(Plan=('plan', 'max'), Fact=('dt_count', 'max'))
            .reset_index()
            .fillna(0)
            .astype({'Plan': 'int', 'Fact': 'int'})
        )

        today = pd.Timestamp.today()
        current_month = today.month
        current_year = today.year
        table['dt'] = pd.to_datetime(table['dt'], errors='coerce')
        table['is_current_month'] = (
            (table['dt'].dt.month == current_month) &
            (table['dt'].dt.year == current_year)
        ).astype(int)
        table = table[table['is_current_month'] == 1]

        if len(table)>0:
            fig, ax = plt.subplots(figsize=(19, 7))
            ax.plot(table['dt'], table['Plan'], marker='o', linewidth=2, markersize=5,
                    label='Plan,чел', color='blue')
            ax.plot(table['dt'], table['Fact'], marker='o', linewidth=2, markersize=5,
                    label='Fact,чел', color='brown')

            for x, y in zip(table['dt'], table['Plan']):
                ax.text(x, y + 3, f'{y}', fontsize=12, ha='center', color='blue')
            for x, y in zip(table['dt'], table['Fact']):
                ax.text(x, y + 3, f'{y}', fontsize=12, ha='left', color='brown')

            ax.set_title('График получение часов план/факт', fontsize=15, fontweight='bold')
            ax.set_ylabel("Кол-во человек")
            ax.tick_params(axis='y', labelcolor='black')
            ax.grid(False)
            ax.legend(loc='right', fontsize=10, frameon=False)
            plt.xticks(ticks=table['dt'], labels=table['dt'].dt.strftime('%Y-%m-%d'), fontsize=10, rotation=45)          
            plt.tight_layout()
        else:
            return None
        buffer = io.BytesIO()
        plt.savefig(buffer, format='png', bbox_inches='tight')
        buffer.seek(0)
        plt.close()
        vis_4_skif = base64.b64encode(buffer.getvalue()).decode('utf-8')
        return vis_4_skif

    @task(task_id="vis5_skif")
    def vis5_skif(df):
        yesterday = (datetime.now() - timedelta(days=1)).date()
        df_yes=df[(df['dt']<=yesterday)&(df['project']=='ЦКП "СКИФ"')]

        table = df_yes[['dt', 'org', 'employee_id','Work_activity_percent']].copy()
        table = table[table['Work_activity_percent'] > 0]


        table = table.groupby(['dt', 'org','employee_id']).agg({'Work_activity_percent': 'mean'}).reset_index()
        table['dt'] = pd.to_datetime(table['dt'])
        table['Month'] = table['dt'].dt.month

        current_month = datetime.now().month
        previous_month = current_month - 1

        table_prev = table[table['Month'] == previous_month].groupby(['Month', 'org']).agg({'Work_activity_percent': 'mean'}).reset_index()
        table_curr = table[table['Month'] == current_month].groupby(['Month', 'dt', 'org']).agg({'Work_activity_percent': 'mean'}).reset_index()

        table_prev['Work_activity_percent'] = table_prev['Work_activity_percent'].round(0)
        table_curr['Work_activity_percent'] = table_curr['Work_activity_percent'].round(0)

        table_curr_final = pd.merge(table_curr, table_prev, how='left', on='org').rename(columns={
            'Work_activity_percent_x': 'Work_activity_percent_current',
            'Work_activity_percent_y': 'Work_activity_percent_previous',
            'Month_x': 'Month'
        })[['Month', 'dt', 'org', 'Work_activity_percent_current', 'Work_activity_percent_previous']]

        orgs = table_curr_final['org'].unique()
        num_orgs = len(orgs)
        cols = 2
        rows = math.ceil(num_orgs / cols)
        
        if table_curr_final.empty:
            print("Нет данных для построения графика ЛАЭС.")
            return None

        fig, axes = plt.subplots(rows, cols, figsize=(18, rows * 4), squeeze=False)
        y_min, y_max = 0, 100
        padding = (y_max - y_min) * 0.05
        y_min -= padding
        y_max += padding

        for idx, org in enumerate(orgs):
                row = idx // cols
                col = idx % cols
                ax = axes[row][col]

                df_org = table_curr_final[table_curr_final['org'] == org].sort_values('dt')
                prev_value = df_org['Work_activity_percent_previous'].iloc[0] if not pd.isna(df_org['Work_activity_percent_previous'].iloc[0]) else 0
                prev_value = int(round(prev_value))
                current_values = df_org['Work_activity_percent_current'].fillna(0).round(0).astype(int).tolist()
                x_labels = ['Среднее за. \n прошл. мес.'] + df_org['dt'].dt.strftime('%Y-%m-%d').tolist()
                y_values = [prev_value] + current_values

                ax.plot(x_labels, y_values, marker='o', color='red')
                for x, y in zip(x_labels, y_values):
                    offset = max(y_values) * 0.07 if max(y_values) != 0 else 1
                    ax.text(x, y + offset, str(y), ha='center', va='bottom', fontsize=10, color='black')

                ax.set_title(f"Средний показатель работа, %: {org}")
                ax.grid(True)
                ax.set_ylim(y_min, y_max)
                ax.tick_params(axis='x', rotation=35)


        for i in range(num_orgs, rows * cols):
                fig.delaxes(axes.flatten()[i])

        fig.tight_layout()

        plt.tight_layout()
      
        buffer = io.BytesIO()
        plt.savefig(buffer, format='png', bbox_inches='tight')
        buffer.seek(0)
        plt.close()
        vis_5_skif = base64.b64encode(buffer.getvalue()).decode('utf-8')
        return vis_5_skif

    @task(task_id="vis6_skif")
    def vis6_skif(df):
        yesterday = (datetime.now() - timedelta(days=1)).date()
        activity_cols = [
        'Break_activity_duration',
        'Break_activity_percent',
        'Movements_activity_duration',
        'Movements_activity_percent',
        'Work_activity_duration',
        'Work_activity_percent'
        ]
        df_yes = df[
            (df['dt'] <= yesterday) &
            (df['project'] == 'ЦКП "СКИФ"') &
            (
            (df['Work_activity_percent'] < 15)|
            (df['Break_activity_percent'] > 60)
            )&
            (df[activity_cols]>0).all(axis=1)

        ]

        table = df_yes[['dt', 'assign_time', 'Break_activity_percent', 'Work_activity_percent']].copy()
        table['dt'] = pd.to_datetime(table['dt'])
        table['Month'] = table['dt'].dt.month
        current_month = datetime.now().month
        table = table[table['Month'] == current_month]
        table = table.groupby('dt')['assign_time'].count().reset_index()

        if len(table)>0:
            fig, ax = plt.subplots(figsize=(19, 7))
            ax.plot(table['dt'], table['assign_time'], marker='o', linewidth=2, markersize=5, color='blue')

            for x, y in zip(table['dt'], table['assign_time']):
                ax.text(x, y + 0.2, f'{y}', fontsize=12, ha='center', color='black')

            ax.set_title('Количество человек с показателями работа <15%; простой >60% за смену',
                        fontsize=15, fontweight='bold')
            ax.tick_params(axis='y', labelcolor='black')
            ax.grid(True)
            ax.legend(loc='center left', fontsize=15, frameon=False)
            plt.xticks(ticks=table['dt'], labels=table['dt'].dt.strftime('%d-%m'), fontsize=10, rotation=45)
            plt.grid(True)
            plt.tight_layout()
        else:
            return None
        buffer = io.BytesIO()
        plt.savefig(buffer, format='png', bbox_inches='tight')
        buffer.seek(0)
        plt.close()
        vis_6_skif = base64.b64encode(buffer.getvalue()).decode('utf-8')
        return vis_6_skif

    @task(task_id="vis7_skif")
    def vis7_skif(df):
        yesterday = (datetime.now() - timedelta(days=1)).date()
        activity_cols = [
        'Break_activity_duration',
        'Break_activity_percent',
        'Movements_activity_duration',
        'Movements_activity_percent',
        'Work_activity_duration',
        'Work_activity_percent'
        ]
        df_yes = df[
            (df['dt'] == yesterday) &
            (df['project'] == 'ЦКП "СКИФ"') &
            (
            (df['Work_activity_percent'] < 15)|
            (df['Break_activity_percent'] > 60)
            )&
            (df[activity_cols]>0).all(axis=1)

        ]


        table=df_yes[['full_name','dt','org','manager','profession','shift','Work_activity_time','Movements_activity_time','Break_activity_time',
                'Work_activity_percent','Movements_activity_percent','Break_activity_percent']]

        table['Total_percent'] = (
            table['Work_activity_percent'] +
            table['Movements_activity_percent'] +
            table['Break_activity_percent']
        )

        num_cols = [
            'Movements_activity_percent', 'Work_activity_percent',
            'Break_activity_percent', 'Total_percent'
        ]

        for col in num_cols:
            table[col] = table[col].fillna(0).astype(int)


        table = table[table['full_name'].notna()]

        table['full_name'] = table['full_name'].fillna('').apply(
            lambda x: '\n'.join([' '.join(x.split()[i:i+2]) for i in range(0, len(x.split()), 2)]) if x else ''
        )

        table['profession'] = table['profession'].fillna('').apply(
            lambda x: '\n'.join([' '.join(x.split()[i:i+2]) for i in range(0, len(x.split()), 2)]) if x else ''
        )
        column_labels = [
            'ФИО', 'Date', 'Подразделение', 'Manager', 'Profession', 'Смена',
            'Work', 'Movements', 'Idle', 'Work,\n%',
            'Movements,\n%', 'Idle,\n%','Всего,\n%'
        ]
        table.columns = column_labels
        table=table.reset_index(drop=True)
        table= table.sort_values('ФИО')

        if len(table)>0:
            fig, ax = plt.subplots(figsize=(17, 8))  # размер
            ax.axis('off')  # убираем оси


            table_vis = ax.table(
                cellText=table.values,   # данные
                colLabels=table.columns, # заголовки
                loc='center'          # расположение по центру
            )


            table_vis.auto_set_font_size(False)
            table_vis.set_fontsize(10)
            table_vis.scale(2, 4) 

            for key, cell in table_vis.get_celld().items():
                row, col = key
                if col == 4:  # 4-я колонка
                    cell.set_width(cell.get_width() * 1.2)  # увеличиваем ширину
                if col >= 6: 
                    cell.set_width(cell.get_width() * 0.7)  # увеличиваем ширину
                if row == 0:
                    cell.set_text_props(fontweight='bold', fontsize=10)
                    cell.set_facecolor('#DDDDDD')


        else:
            return None
        buffer = io.BytesIO()
        plt.savefig(buffer, format='png', bbox_inches='tight')
        buffer.seek(0)
        plt.close()
        vis_7_skif = base64.b64encode(buffer.getvalue()).decode('utf-8')
        return vis_7_skif

    @task(task_id="send_email_skif")
    def send_email_skif(vis_1_skif,vis_2_skif,vis_3_skif,vis_4_skif,vis_5_skif,vis_6_skif,vis_7_skif):

        def decode_base64_to_image(base64_string):
            if not base64_string:
                return None
            try:
                image_data = base64.b64decode(base64_string)
                return Image.open(io.BytesIO(image_data))
            except Exception as e:
                print(f"[ERROR] Failed to decode image: {e}")
                return None



        decoded_images = []
        for i, vis in enumerate([vis_1_skif, vis_2_skif, vis_3_skif, vis_4_skif, vis_5_skif, vis_6_skif, vis_7_skif], start=1):
            img = decode_base64_to_image(vis)
            if img:
                print(f"[INFO] vis_{i} successfully decoded.")
                decoded_images.append(img)
            else:
                print(f"[WARNING] vis_{i} is empty or invalid and will be skipped.")


        def add_white_background(image):
            if image.mode in ('RGBA', 'LA') or (image.mode == 'P' and 'transparency' in image.info):

                background = Image.new('RGB', image.size, (255, 255, 255))
                background.paste(image, mask=image.split()[-1])  # Используем альфа-канал как маску
                return background
            else:
                return image.convert('RGB')
        def resize_image(image, target_size=(2480, 3508)):  # A4 @ 300 DPI
            return image.resize(target_size, Image.LANCZOS)


        decoded_images = [add_white_background(img) for img in decoded_images]
        pdf_filename = "/opt/airflow/dags/Solut/СКИФ.pdf"  # Укажите нужный путь
        cover = "/opt/airflow/dags/Solut/СКИФ.jpg"


        cover_imge = Image.open(cover)


        cover_imge = add_white_background(cover_imge)

        cover_imge.save(pdf_filename, format="PDF", resolution=300.0, save_all=True,append_images=decoded_images)


        visuals = [("Визуал 1", vis_1_skif), ("Визуал 2", vis_2_skif), ("Визуал 3", vis_3_skif),
                ("Визуал 4", vis_4_skif), ("Визуал 5", vis_5_skif), ("Визуал 6", vis_6_skif), ("Визуал 7", vis_7_skif)]


        visuals_html = ""
        for alt_text, base64_img in visuals:
            if base64_img:  # Проверяем, что строка не пустая
                visuals_html += f'<img src="data:image/png;base64,{base64_img}" alt="{alt_text}" style="margin-bottom:20px;"><br>'

        html_content = f"""
        <html>
            <body>
                <p>Нажмите <a href="https://pixbi.titan2.ru/shared/view/3b3a97b9-534a-426a-add7-15ae3873b950?ticket=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJodHRwOi8vc2NoZW1hcy54bWxzb2FwLm9yZy93cy8yMDA1LzA1L2lkZW50aXR5L2NsYWltcy9uYW1lIjoiVGlja2V0VXNlciIsIm5iZiI6MTc1Mzg3NDEzMSwiZXhwIjoyMDY5NDA2OTMxfQ.ackY-GnRFmmXvCoWuAF7-ffMz-qOzEiXXYyevb2Sklw">здесь</a>, чтобы посмотреть отчет.</p>
                {visuals_html}
            </body>
        </html>
        """

        email_task = EmailOperator(
             task_id='send_email_skif_task',
             to=to_emails_СКИФ,
             subject='Ежедневый отчет - ЦКП "СКИФ"',
             html_content=html_content,
             files=[pdf_filename]  # Передаем только путь к файлу
         )
        email_task.execute(context={})



    vis_1_skif = vis1_skif(d)
    vis_2_skif = vis2_skif(d)
    vis_3_skif = vis3_skif(d)
    vis_4_skif = vis4_skif(d)
    vis_5_skif = vis5_skif(d)
    vis_6_skif = vis6_skif(d)
    vis_7_skif = vis7_skif(d)

    send_email_skif(vis_1_skif,vis_2_skif,vis_3_skif,vis_4_skif,vis_5_skif,vis_6_skif,vis_7_skif)
