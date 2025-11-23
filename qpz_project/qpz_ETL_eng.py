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
from clickhouse_driver import Client
from PIL import Image, ImageOps
from datetime import datetime


clickhouse_host = Variable.get('CH_HOST')
clickhouse_port = Variable.get('CH_PORT')
clickhouse_user = Variable.get('CH_USER')
clickhouse_password = Variable.get('CH_PASSWORD')

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025,4,8),
    'retries': 2,
}

with DAG('QPZ_ETL_reports_25', default_args=default_args, schedule='0 6 * * 3') as dag:
    @task(task_id="fetch_data_25")
    def fetch_data_25():
        with Client(host=clickhouse_host, port=clickhouse_port, user=clickhouse_user, password=clickhouse_password) as client:
            query = "SELECT * FROM pixbi.qpz_project"
            result = client.execute(query, with_column_types=True)
            
            columns = [col[0] for col in result[1]]  # Извлечение названий столбцов
            data = pd.DataFrame(result[0], columns=columns)
            

            month_order = {'Январь': 1, 'Февраль': 2, 'Март': 3, 'Апрель': 4, 'Май': 5, 'Июнь': 6,
            'Июль': 7, 'Август': 8, 'Сентябрь': 9, 'Октябрь': 10, 'Ноябрь': 11, 'Декабрь': 12}

            data['month_order'] = data['month_name'].map(month_order)
            data = data.sort_values('month_order')

            file_path  = "/opt/airflow/dags/QPZ_ETL_Image/qpz_s_2.xlsx"
            df=pd.read_excel(file_path)
            df['№ пп'] = df['№ пп'].astype(str)
            df.loc[df['Conc'] == '8.10Сенеж', '№ пп'] = '8.10'
            data_split= data['Directory'].str.split('_',expand=True)
            data_split.columns=['C1','C2','C3','C4']
            data_split['Conc'] = data_split['C3']+data_split['C4']
            data_split=data_split[['Conc']]
            data_c= pd.concat([data,data_split],axis=1)
            data = pd.merge(data_c,df,on= 'Conc',how = 'left')

            data['f_c_vne_plan'] = data.apply(
                        lambda row: row['f_c'] if row['previous_period_fact_exists'] == 'Да' else 0, axis=1
                    )
            data['f_c_v_plan'] = data.apply(
                        lambda row: row['f_c'] if row['previous_period_fact_exists'] == 'Нет' else 0, axis=1
                    )

            data['f_q_vne_plan'] = data.apply(
                        lambda row: row['f_q'] if row['previous_period_fact_exists'] == 'Да' else 0, axis=1
                    )
            data['f_q_v_plan'] = data.apply(
                        lambda row: row['f_q'] if row['previous_period_fact_exists'] == 'Нет' else 0, axis=1
                    )

            data_25 =data[data['Year']=='2025 год']
            return data_25  # Возвращаем DataFrame
    d_25 = fetch_data_25()
    @task(task_id="vis1")
    def vis1(data_25):        
        grouped_data = data_25[['month_order','month_name', 'e_c', 'p_c', 'f_c_vne_plan','f_c_v_plan']].groupby(['month_order','month_name']).sum().reset_index()
        grouped_data = grouped_data.sort_values('month_order')

        grouped_data['e_c'] = grouped_data['e_c'].replace(0, 0.001)
        grouped_data['p_c'] = grouped_data['p_c'].replace(0, 0.001)
        grouped_data['f_c_v_plan'] = grouped_data['f_c_v_plan'].replace(0, np.nan)
        grouped_data['f_c_vne_plan'] = grouped_data['f_c_vne_plan'].replace(0, np.nan)

        total_e_c = round(data_25['e_c'].sum())
        total_p_c = round(data_25['p_c'].sum())
        total_f_c_v_plan = round(data_25['f_c_v_plan'].sum())
        total_f_c_vne_plan = round(data_25['f_c_vne_plan'].sum())

        if total_e_c != 0:
                difference_e_c_p_c = np.round(((total_p_c - total_e_c) / total_e_c) * 100, 0)
        else:
            difference_e_c_p_c = 0

        if total_p_c != 0:
                difference_p_c_f_c_v_plan = np.round(((total_f_c_v_plan - total_p_c) / total_p_c) * 100, 0)
        else:
            difference_p_c_f_c_v_plan = 0

        table_2025 = {
            "План ГПЗ": [f'{total_e_c:,}₽'.replace(",", " ")],
            "Факт ВЗ": [f'{total_p_c:,}₽'.replace(",", " ")],
            "Разница": [f'{difference_e_c_p_c:,}%'.replace(",", " ")],
            "Факт ПТиУ \n (в плане)": [f'{total_f_c_v_plan:,}₽'.replace(",", " ")],
            "Разница ": [f'{difference_p_c_f_c_v_plan:,}%'.replace(",", " ")],
            "Факт ПТиУ \n (вне плана)": [f'{total_f_c_vne_plan:,}₽'.replace(",", " ")]}

        table_data = pd.DataFrame(table_2025)

        fig, axs = plt.subplots(2, 1, figsize=(30, 15), gridspec_kw={'height_ratios': [3, 1]})
        current_date = datetime.today().strftime("%d.%m.%Y")
        fig.text(0.9, 0.9, f"Отчет на {current_date}", ha='right', fontsize=20, fontweight='bold')

        bar_width = 0.45
        group_spacing = 1
        index = [i * (3 * bar_width + group_spacing) for i in range(len(grouped_data))]
        bars_e_c = axs[0].bar(index, grouped_data['e_c'], bar_width, label='План ГПЗ', color='#9B9B9B')
        bars_p_c = axs[0].bar([i + bar_width for i in index], grouped_data['p_c'], bar_width, label='Факт ВЗ', color='#7CB5EC')
        bars_f_c_v_plan = axs[0].bar([i + 2 * bar_width for i in index], grouped_data['f_c_v_plan'], bar_width, label='Факт ПТиУ (в плане)', color='#0B67C0')
        bars_f_c_vne_plan = axs[0].bar([i + 3 * bar_width for i in index], grouped_data['f_c_vne_plan'], bar_width, label='Факт ПТиУ (вне плана)', color='#20B2AA')

        def format_value(value):
            if value >= 1_000_000_000:
                return f'{value / 1_000_000_000:.0f}\nмлрд'
            elif value >= 1_000_000:
                return f'{value / 1_000_000:.0f}\nмлн'
            elif value >= 1_000:
                return f'{value / 1_000:.0f}\nтыс'
            else:
                return f'{value:.0f}'

        for bar in bars_e_c:
            value = bar.get_height()
            axs[0].text(bar.get_x() + bar.get_width() / 2, value + 0.02, format_value(value), ha='center', va='bottom', fontsize=15)
        for bar in bars_p_c:
            value = bar.get_height()
            axs[0].text(bar.get_x() + bar.get_width() / 2, value + 0.02, format_value(value), ha='center', va='bottom', fontsize=15)
        for bar in bars_f_c_v_plan:
            value = bar.get_height()
            axs[0].text(bar.get_x() + bar.get_width() / 2, value + 0.02, format_value(value), ha='center', va='bottom', fontsize=15)

        for bar in bars_f_c_vne_plan:
            value = bar.get_height()
            axs[0].text(bar.get_x() + bar.get_width() / 2, value + 0.02, format_value(value), ha='center', va='bottom', fontsize=15)
        
        axs[0].set_xticks([i + bar_width for i in index])
        axs[0].set_xticklabels(grouped_data['month_name'], rotation=45,fontsize=20)
        axs[0].legend(ncol=1, loc='upper left',fontsize=20)
        axs[0].set_title("Обеспечение по месяцам", fontsize=24,fontweight='bold')
        axs[0].spines['top'].set_visible(False)
        axs[0].spines['right'].set_visible(False)
        axs[0].spines['left'].set_visible(False)
        axs[0].yaxis.set_visible(False)


        axs[1].axis('tight')
        axs[1].axis('off')

        table = axs[1].table(cellText=table_data.values,
                            colLabels=table_data.columns,
                            cellLoc='center',
                            loc='center')


        table.auto_set_font_size(False)
        table.set_fontsize(25)

        for (i,key),cell in table.get_celld().items():
            cell.set_width(0.16)
            cell.set_height(0.4)
            if i==0:
                cell.set_facecolor('#D3D3D3')
                cell.set_text_props(weight='bold')


        fig.suptitle("За 2025 год", fontsize=24, weight='bold')

        plt.tight_layout(rect=[0, 0, 1, 0.95])


        buffer = io.BytesIO()
        plt.savefig(buffer, format='png', bbox_inches='tight')
        buffer.seek(0)
        plt.close()

        vis_1 = base64.b64encode(buffer.getvalue()).decode('utf-8')
        return vis_1

    @task(task_id="vis2")
    def vis2(data_25):
        data_g = data_25.groupby(['№ пп','Группа','ЦФО/ДЗО/Мероприятие','Num'], as_index=False)[['e_c', 'p_c','f_c_v_plan','f_c_vne_plan']].sum()
        data_gr = data_g.sort_values('Num')
        data_group = data_gr[['№ пп','Группа','ЦФО/ДЗО/Мероприятие','e_c','p_c','f_c_v_plan','f_c_vne_plan']]
        data_group[['e_c', 'p_c', 'f_c_v_plan','f_c_vne_plan']].isna().sum()
        data_group['Разница %'] = np.where(
            data_group['e_c'] != 0,
            np.round(((data_group['p_c'] - data_group['e_c']) / data_group['e_c']) * 100, 0),
            0
        )

        data_group['Разница % '] = np.where(
            data_group['f_c_v_plan'] != 0,
            np.round(((data_group['f_c_v_plan'] - data_group['p_c']) / data_group['f_c_v_plan']) * 100, 0),
            0
        )
        data_group.rename(columns={'e_c':'План ГПЗ','p_c':'Факт ВЗ','f_c_v_plan':'Факт ПТиУ \n (в плане)','f_c_vne_plan':'Факт ПТиУ \n (вне плана)'},inplace=True)
        columns_to_int =['План ГПЗ','Факт ВЗ','Факт ПТиУ \n (в плане)','Разница %','Разница % ','Факт ПТиУ \n (вне плана)']
        data_group[columns_to_int]=data_group[columns_to_int].astype(int)
        visual_3 = data_group[['№ пп','Группа','ЦФО/ДЗО/Мероприятие','План ГПЗ','Факт ВЗ','Разница %','Факт ПТиУ \n (в плане)','Разница % ','Факт ПТиУ \n (вне плана)']]
        columns_to_format = ['План ГПЗ', 'Факт ВЗ', 'Факт ПТиУ \n (в плане)','Факт ПТиУ \n (вне плана)']
        visual_3[columns_to_format] = visual_3[columns_to_format].applymap(
            lambda x: "{:,.0f}".format(x).replace(",", " ") if pd.notnull(x) else x)

        fig2, ax2 = plt.subplots(figsize=(8, 3))

        ax2.axis('off')

        table2 = ax2.table(cellText=visual_3.values,
                        colLabels=visual_3.columns,
                        cellLoc='center',
                        loc='center')

        table2.auto_set_font_size(False)
        table2.set_fontsize(14)

        for key, cell in table2.get_celld().items():
            row, col = key
            if row == 0:  # Заголовки находятся в первой строке (row == 0)
                cell.set_text_props(fontweight='bold',fontsize = 16)  # Делаем текст жирным
                cell.set_facecolor('#DDDDDD')  # Фон заголовка (серый для наглядности)
            cell.set_width(0.6)
            cell.set_height(0.4)

        plt.tight_layout(rect=[0, 0, 1, 0.92])
        buffer = io.BytesIO()
        plt.savefig(buffer, format='png', bbox_inches='tight')
        buffer.seek(0)
        plt.close()
        vis_2 = base64.b64encode(buffer.getvalue()).decode('utf-8')
        return vis_2

    @task(task_id="vis3")
    def vis3(data_25):
        df_sum=data_25[['month_name','month_order','e_c','p_c','f_c_v_plan','f_c_vne_plan']].groupby(['month_name','month_order']).sum().reset_index()
        df_sum = df_sum.sort_values('month_order')
        df_sum['Cumulative_e_c'] = df_sum['e_c'].cumsum()
        df_sum['Cumulative_p_c'] = df_sum['p_c'].cumsum()
        df_sum['Cumulative_f_c_v_plan'] = df_sum['f_c_v_plan'].cumsum()
        df_sum['Cumulative_f_c_vne_plan'] = df_sum['f_c_vne_plan'].cumsum()

        df_sum['Cumulative_e_c'] = df_sum['Cumulative_e_c'].replace(0, np.nan)
        df_sum['Cumulative_p_c'] = df_sum['Cumulative_p_c'].replace(0, np.nan)
        df_sum['Cumulative_f_c_v_plan'] = df_sum['Cumulative_f_c_v_plan'].replace(0, np.nan)
        df_sum['Cumulative_f_c_vne_plan'] = df_sum['Cumulative_f_c_vne_plan'].replace(0, np.nan)        

        def format_value(value):
            if value >= 1_000_000_000:
                return f'{value / 1_000_000_000:.0f}\nмлрд'
            elif value >= 1_000_000:
                return f'{value / 1_000_000:.0f}\nмлн'
            elif value >= 1_000:
                return f'{value / 1_000:.0f}\nтыс'
            else:
                return f'{value:.0f}'
        

        fig1 = plt.figure(figsize=(35, 15))
        ax1 = fig1.add_subplot(111)
        ax1.plot(df_sum['month_name'], df_sum['Cumulative_e_c'], marker='o', linestyle='-',markersize=7, linewidth=3, label='План ГПЗ', color='#9B9B9B')
        ax1.plot(df_sum['month_name'], df_sum['Cumulative_p_c'], marker='o', linestyle='-',markersize=7, linewidth=3, label='Факт ВЗ', color='#7CB5EC')
        ax1.plot(df_sum['month_name'], df_sum['Cumulative_f_c_v_plan'], marker='o', linestyle='-',markersize=7, linewidth=3, label='Факт ПТиУ (в плане)', color='#0B67C0')
        ax1.plot(df_sum['month_name'], df_sum['Cumulative_f_c_vne_plan'], marker='o', linestyle='-',markersize=7, linewidth=3, label='Факт ПТиУ\n(вне плана)', color='#20B2AA')
        
        ax1.set_title('За текущий год по месяцам в разрезе номенклатуры (накопительный итог, стоимость)',fontsize=20,fontweight='bold')
        ax1.tick_params(axis='y', labelcolor='black')
        ax1.grid(False)
        ax1.legend(loc='upper left', fontsize=20,frameon=False)
        ax1.set_xticklabels(df_sum['month_name'], fontsize=20)
        ax1.set_yscale('log')
        ax1.spines['top'].set_visible(False)
        ax1.spines['right'].set_visible(False)
        ax1.spines['bottom'].set_linewidth(0.4)
        ax1.spines['left'].set_linewidth(False)
        ax1.yaxis.set_visible(False)

        for i in range(len(df_sum)):
            ax1.annotate(format_value(df_sum["Cumulative_e_c"][i]), 
                            xy=(df_sum['month_name'][i], df_sum["Cumulative_e_c"][i]), 
                            xytext=(df_sum['month_name'][i], df_sum["Cumulative_e_c"][i]),
                            ha='right', 
                            color='#9B9B9B',
                            fontweight='bold',
                            fontsize=18)

            ax1.annotate(format_value(df_sum["Cumulative_p_c"][i]), 
                            xy=(df_sum['month_name'][i], df_sum["Cumulative_p_c"][i]), 
                            xytext=(df_sum['month_name'][i], df_sum["Cumulative_p_c"][i]), 
                            ha='right', 
                            color='#7CB5EC',
                            fontweight='bold',
                            fontsize=18)
            
            ax1.annotate(format_value(df_sum["Cumulative_f_c_v_plan"][i]), 
                            xy=(df_sum['month_name'][i], df_sum["Cumulative_f_c_v_plan"][i]), 
                            xytext=(df_sum['month_name'][i], df_sum["Cumulative_f_c_v_plan"][i]),
                            ha='left', 
                            va='bottom',
                            color='#0B67C0',
                            fontweight='bold',
                            fontsize=18)

            ax1.annotate(format_value(df_sum["Cumulative_f_c_vne_plan"][i]), 
                            xy=(df_sum['month_name'][i], df_sum["Cumulative_f_c_vne_plan"][i]), 
                            xytext=(df_sum['month_name'][i], df_sum["Cumulative_f_c_vne_plan"][i]),
                            ha='left', 
                            va='bottom',
                            color='#20B2AA',
                            fontweight='bold',
                            fontsize=18)
      
        buffer = io.BytesIO()
        plt.savefig(buffer, format='png', bbox_inches='tight')
        buffer.seek(0)
        plt.close()
        vis_3= base64.b64encode(buffer.getvalue()).decode('utf-8')
        return vis_3

    @task(task_id="vis4")
    def vis4(data_25):
        df_sum=data_25[['month_name','month_order','e_c','p_c','f_c_v_plan','f_c_vne_plan']].groupby(['month_name','month_order']).sum().reset_index()
        df_sum = df_sum.sort_values('month_order')

        df_sum['e_c'] = df_sum['e_c'].replace(0, 0.001)
        df_sum['p_c'] = df_sum['p_c'].replace(0, 0.001)
        df_sum['f_c_v_plan'] = df_sum['f_c_v_plan'].replace(0, np.nan)
        df_sum['f_c_vne_plan'] = df_sum['f_c_vne_plan'].replace(0, np.nan)


        def format_value(value):
            if value >= 1_000_000_000:
                return f'{value / 1_000_000_000:.0f}\nмлрд'
            elif value >= 1_000_000:
                return f'{value / 1_000_000:.0f}\nмлн'
            elif value >= 1_000:
                return f'{value / 1_000:.0f}\nтыс'
            else:
                return f'{value:.0f}'


        fig2 = plt.figure(figsize=(35, 15))
        ax2 = fig2.add_subplot(111)
        ax2.plot(df_sum['month_name'], df_sum['e_c'], marker='o', linestyle='--',markersize=7, linewidth=3, label='План ГПЗ', color='#9B9B9B')
        ax2.plot(df_sum['month_name'], df_sum['p_c'], marker='o', linestyle='--',markersize=7, linewidth=3, label='Факт ВЗ', color='#7CB5EC')
        ax2.plot(df_sum['month_name'], df_sum['f_c_v_plan'], marker='o', linestyle='--',markersize=7, linewidth=3, label='Факт ПТиУ (в плане)', color='#0B67C0')
        ax2.plot(df_sum['month_name'], df_sum['f_c_vne_plan'], marker='o', linestyle='--',markersize=7, linewidth=3, label='Факт ПТиУ (вне плане)', color='#20B2AA')        

        for i in range(len(df_sum)):
            ax2.annotate(format_value(df_sum['e_c'][i]), 
                            xy=(df_sum['month_name'][i], df_sum['e_c'][i]), 
                            xytext=(df_sum['month_name'][i], df_sum['e_c'][i]),
                            ha='right', 
                            color='#9B9B9B',
                            fontweight='bold',
                            fontsize=18)

            ax2.annotate(format_value(df_sum['p_c'][i]), 
                            xy=(df_sum['month_name'][i], df_sum['p_c'][i]), 
                            xytext=(df_sum['month_name'][i], df_sum['p_c'][i]), 
                            ha='right', 
                            color='#7CB5EC',
                            fontweight='bold',
                            fontsize=18)
            
            ax2.annotate(format_value(df_sum['f_c_v_plan'][i]), 
                            xy=(df_sum['month_name'][i], df_sum['f_c_v_plan'][i]), 
                            xytext=(df_sum['month_name'][i], df_sum['f_c_v_plan'][i]),
                            ha='left', 
                            va='bottom',
                            color='#0B67C0',
                            fontweight='bold',
                            fontsize=18)

            ax2.annotate(format_value(df_sum['f_c_vne_plan'][i]), 
                            xy=(df_sum['month_name'][i], df_sum['f_c_vne_plan'][i]), 
                            xytext=(df_sum['month_name'][i], df_sum['f_c_vne_plan'][i]),
                            ha='left', 
                            va='bottom',
                            color='#20B2AA',
                            fontweight='bold',
                            fontsize=18)

        ax2.set_title('За текущий год по месяцам в разрезе номенклатуры (стоимость)',fontsize=20,fontweight='bold')
        ax2.tick_params(axis='y', labelcolor='black')
        ax2.grid(False)
        ax2.legend(loc='lower left',fontsize=20,frameon=False)
        ax2.set_xticklabels(df_sum['month_name'], fontsize=20)

        ax2.spines['top'].set_visible(False)
        ax2.spines['right'].set_visible(False)
        ax2.spines['bottom'].set_linewidth(1)
        ax2.spines['left'].set_linewidth(False)
        ax2.yaxis.set_visible(False)
        ax2.set_yscale('log')
        
        buffer = io.BytesIO()
        plt.savefig(buffer, format='png', bbox_inches='tight')
        buffer.seek(0)
        plt.close()
        vis_4= base64.b64encode(buffer.getvalue()).decode('utf-8')
        return vis_4

    @task(task_id="vis5")
    def vis5(data_25):
        df_enl=data_25[['s','e_c','p_c','f_c_v_plan','f_c_vne_plan']].groupby(['s']).sum().reset_index()
        df_enl_10 = df_enl.sort_values(by='e_c', ascending=False).head(10).sort_values('e_c',ascending=False)
        df_enl_10['s']=df_enl_10['s'].apply(lambda x: x.replace(' ','\n'))
        df_enl_10[['e_c', 'p_c', 'f_c_v_plan','f_c_vne_plan']] = df_enl_10[['e_c', 'p_c', 'f_c_v_plan','f_c_vne_plan']].fillna(0)
        def format_value(value):
            if value >= 1_000_000_000:
                return f'{value / 1_000_000_000:.0f}\nмлрд'
            elif value >= 1_000_000:
                return f'{value / 1_000_000:.0f}\nмлн'
            elif value >= 1_000:
                return f'{value / 1_000:.0f}\nтыс'
            else:
                return f'{value:.0f}'


        fig3 = plt.figure(figsize=(35, 15))
        ax3 = fig3.add_subplot(111)
        bar_width = 0.45
        group_spacing = 1
        index = [i * (3 * bar_width + group_spacing) for i in range(len(df_enl_10))]


        bars_e_c = ax3.bar(index, df_enl_10['e_c'], bar_width, label='План ГПЗ', color='#9B9B9B')
        bars_p_c = ax3.bar([i + bar_width for i in index], df_enl_10['p_c'], bar_width, label='Факт ВЗ', color='#7CB5EC')
        bars_f_c_v_plan = ax3.bar([i + 2 * (bar_width) for i in index], df_enl_10['f_c_v_plan'], bar_width, label='Факт ПТиУ (в плане)', color='#0B67C0')
        bars_f_c_vne_plan = ax3.bar([i + 3 * (bar_width) for i in index], df_enl_10['f_c_vne_plan'], bar_width, label='Факт ПТиУ (вне плана)', color='#20B2AA')

        for bar in bars_e_c:
            value = bar.get_height()
            ax3.text(bar.get_x() + bar.get_width() / 2, value + 0.02, format_value(value), ha='center', va='bottom',fontweight='bold', fontsize=15)
        for bar in bars_p_c:
            value = bar.get_height()
            ax3.text(bar.get_x() + bar.get_width() / 2, value + 0.02, format_value(value), ha='center', va='bottom',fontweight='bold', fontsize=15)
        for bar in bars_f_c_v_plan:
            value = bar.get_height()
            ax3.text(bar.get_x() + bar.get_width() / 2, value + 0.02, format_value(value), ha='center', va='bottom',fontweight='bold', fontsize=15)
        for bar in bars_f_c_vne_plan:
            value = bar.get_height()
            ax3.text(bar.get_x() + bar.get_width() / 2, value + 0.02, format_value(value), ha='center', va='bottom',fontweight='bold', fontsize=15)

        ax3.set_xticks([i + bar_width for i in index])
        ax3.set_xticklabels(df_enl_10['s'], rotation=20,fontsize=15)
        ax3.legend(ncol=1, loc='upper right',fontsize=20)
        ax3.set_title("Стоимость за текущий год по укр.номенклатуре", fontsize=20,fontweight='bold')
        ax3.spines['top'].set_visible(False)
        ax3.spines['right'].set_visible(False)
        ax3.spines['left'].set_visible(False)
        ax3.yaxis.set_visible(False)



        buffer = io.BytesIO()
        plt.savefig(buffer, format='png', bbox_inches='tight')
        buffer.seek(0)
        plt.close()
        vis_5= base64.b64encode(buffer.getvalue()).decode('utf-8')
        return vis_5

    @task(task_id="vis6")
    def vis6(data_25):
        df_enl=data_25[['s','e_q', 'e_c','p_q','f_q_v_plan','f_q_vne_plan']].groupby(['s']).sum().reset_index()
        df_enl_10 = df_enl.sort_values(by='e_c', ascending=False).head(10).sort_values('e_c',ascending=False)
        df_enl_10['s']=df_enl_10['s'].apply(lambda x: x.replace(' ','\n'))
        df_enl_10[['e_q', 'p_q', 'f_q_v_plan','f_q_vne_plan']] = df_enl_10[['e_q', 'p_q', 'f_q_v_plan','f_q_vne_plan']].fillna(0)

        fig4 = plt.figure(figsize=(35, 15))
        ax4 = fig4.add_subplot(111)
        bar_width = 0.45
        group_spacing = 1
        index = [i * (3 * bar_width + group_spacing) for i in range(len(df_enl_10))]
        
        bars_p_q = ax4.bar([i + bar_width for i in index], 100, bar_width, label='Факт ВЗ (100%)', color='#B3D457')
        bars_e_q = ax4.bar(index, np.where((df_enl_10['e_q'] / df_enl_10['p_q']) * 100 > 300, 300, (df_enl_10['e_q'] / df_enl_10['p_q']) * 100), bar_width, label='План ГПЗ', color='#E18F8F')
        bars_f_q_v_plan = ax4.bar([i + 2 * bar_width for i in index], np.where((df_enl_10['f_q_v_plan'] / df_enl_10['p_q']) * 100 > 300, 300, (df_enl_10['f_q_v_plan'] / df_enl_10['p_q']) * 100), bar_width, label='Факт ПТиУ (в плане)', color='#EFB786')
        bars_f_q_vne_plan = ax4.bar([i + 3 * bar_width for i in index], np.where((df_enl_10['f_q_vne_plan'] / df_enl_10['p_q']) * 100 > 300, 300, (df_enl_10['f_q_vne_plan'] / df_enl_10['p_q']) * 100), bar_width, label='Факт ПТиУ (вне плана)', color='#B98E1D')
        
        vals_p_q = [100] * len(df_enl_10)
        vals_e_q = np.where((df_enl_10['e_q'] / df_enl_10['p_q']) * 100 > 300, 300, (df_enl_10['e_q'] / df_enl_10['p_q']) * 100)
        vals_f_q_v_plan = np.where((df_enl_10['f_q_v_plan'] / df_enl_10['p_q']) * 100 > 300, 300, (df_enl_10['f_q_v_plan'] / df_enl_10['p_q']) * 100)
        vals_f_q_vne_plan = np.where((df_enl_10['f_q_vne_plan'] / df_enl_10['p_q']) * 100 > 300, 300, (df_enl_10['f_q_vne_plan'] / df_enl_10['p_q']) * 100)
        
        for bar, value in zip(bars_p_q, vals_p_q):
            ax4.text(bar.get_x() + bar.get_width() / 2,
                    bar.get_height() + 2,
                    f'{value}%',
                    ha='center', va='bottom',
                    fontsize=14, color='black')

        for bar, value in zip(bars_e_q, vals_e_q):
            ax4.text(bar.get_x() + bar.get_width() / 2,
                    bar.get_height() + 2,
                    f'{value:.0f}%',
                    ha='center', va='bottom',
                    fontsize=14, color='black')

        for bar, value in zip(bars_f_q_v_plan, vals_f_q_v_plan):
            ax4.text(bar.get_x() + bar.get_width() / 2,
                    bar.get_height() + 2,
                    f'{value:.0f}%',
                    ha='center', va='bottom',
                    fontsize=14, color='black')
        
        for bar, value in zip(bars_f_q_vne_plan, vals_f_q_vne_plan):
            ax4.text(bar.get_x() + bar.get_width() / 2,
                    bar.get_height() + 2,
                    f'{value:.0f}%',
                    ha='center', va='bottom',
                    fontsize=14, color='black')        

        ax4.set_xticks([i + bar_width for i in index])
        ax4.set_xticklabels(df_enl_10['s'], rotation=20,fontsize=18,color='black')
        ax4.legend(ncol=1, bbox_to_anchor=(0.1, 0.9), fontsize=20)
        ax4.set_title("% соотношение План ГПЗ - План ВЗ за текущий год по укр.номенклатуре (количество)", fontsize=20,fontweight='bold')
        ax4.spines['top'].set_visible(False)
        ax4.spines['right'].set_visible(False)
        ax4.spines['left'].set_visible(False)
        ax4.yaxis.set_visible(False)


        buffer = io.BytesIO()
        plt.savefig(buffer, format='png', bbox_inches='tight')
        buffer.seek(0)
        plt.close()
        vis_6= base64.b64encode(buffer.getvalue()).decode('utf-8')
        return vis_6

    @task(task_id="vis7")
    def vis7(data_25):
        df_table=data_25[['s','e_q', 'e_c','p_q', 'p_c','f_c_v_plan','f_q_v_plan']].groupby(['s']).sum().reset_index()

        df_table = df_table.round(2)
        df_table = df_table.applymap(lambda x: f"{x:,.0f}".replace(",", " ") if isinstance(x, (int, float)) else x)
        df_table=df_table[["s","e_c","p_c","f_c_v_plan","e_q","p_q","f_q_v_plan"]]

        df_table = df_table.rename(columns={
            "s": "Укрупненная номенклатура",
            "e_c": "План ГПЗ стоимость",
            "p_c": "Факт ВЗ стоимость",
            "f_c_v_plan": "Факт ПТиУ стоимость \n (в плане)",
            "e_q": "План ГПЗ количество",
            "p_q": "Факт ВЗ количество",
            "f_q_v_plan": "Факт ПТиУ количество \n (в плане)",
        })

        fact_table= data_25[['previous_period_fact_exists','f_c']]
        fact_table = pd.DataFrame({
            'Факт ПТиУ (вне плана)': fact_table[fact_table['previous_period_fact_exists'] == 'Да']['f_c'].sum().round(1),
            'Факт ПТиУ (план)': fact_table[fact_table['previous_period_fact_exists'] == 'Нет']['f_c'].sum(),
            'Факт ПТиУ Итог': fact_table[(fact_table['previous_period_fact_exists'] == 'Да') | (fact_table['previous_period_fact_exists'] == 'Нет')]['f_c'].sum(),
        }, index=[0])

        fact_table = fact_table.applymap(lambda x: f"{x:,.0f}".replace(",", " ") if isinstance(x, (int, float)) else x)



        def format_value(value):
            if value >= 1_000_000_000:
                return f'{value / 1_000_000_000:.0f} млрд'
            elif value >= 1_000_000:
                return f'{value / 1_000_000:.0f} млн'
            elif value >= 1_000:
                return f'{value / 1_000:.0f} тыс'
            else:
                return f'{value:.0f}'

        fg = plt.figure(figsize=(10, 28), constrained_layout=True)
        gs = fg.add_gridspec(3, 1, height_ratios=[0.2, 0.45, 0.55])  


        current_date = datetime.today().strftime("%d.%m.%Y")


        ax_table_0 = fg.add_subplot(gs[1])
        ax_table_1 = fg.add_subplot(gs[2])

        table_0 = ax_table_0.table(
            cellText=fact_table.values,
            colLabels=fact_table.columns,
            cellLoc='center',
            loc='center',
            colColours=['lightgrey'] * len(fact_table.columns),
            bbox=[0, 2, 1.6, 0.2]
        )
        table_0.auto_set_font_size(False)
        table_0.set_fontsize(20)  # Уменьшаем шрифт для компактности
        ax_table_0.axis('off')  # Убираем оси

        y_upper = 2.65
        row_height = 0.08  # Высота одной строки
        header_height = 0.1  # Высота заголовка
        num_rows = len(df_table)  # Количество строк в таблице
        height = header_height + row_height * num_rows
        y0 = y_upper - height

        table_1 = ax_table_1.table(
            cellText=df_table.values,
            colLabels=df_table.columns,
            cellLoc='center',
            loc='upper center',
            colColours=['lightblue'] * len(df_table.columns),
            bbox=[0, y0, 4, height],  # Обновленный bbox
            colWidths=[0.5] + [0.2] * (len(df_table.columns) - 1)
        )
        table_1.auto_set_font_size(False)
        table_1.set_fontsize(17)  # Размер шрифта
        ax_table_1.axis('off')

        title_text = f"Стоимость и количество по укр. номенклатуре за текущий год{' ' * 40}Отчет на {current_date}"
        ax_table_1.set_title(title_text, y=2.7, x=2.4, fontsize=25, fontweight='bold')
        
        for (row, col), cell in table_0.get_celld().items():
            if row > 0:  # Пропускаем заголовки
                if col >= 0:  # Для всех столбцов с данными
                    cell.get_text().set_text(f"{fact_table.iloc[row-1, col]} ₽")

        for (row, col), cell in table_1.get_celld().items():
            if row > 0:  # Пропускаем заголовки
                if 1 <= col <= 3:  # Для столбцов 1, 2, 3
                    cell.get_text().set_text(f"{df_table.iloc[row-1, col]} ₽")

        for (i, key) in enumerate(table_0._cells):
            cell = table_0._cells[key]
            if key[0] == 0:  # Первая строка (заголовки)
                cell.set_text_props(fontweight='bold')

        for (i, key) in enumerate(table_1._cells):
            cell = table_1._cells[key]
            if key[0] == 0:  # Первая строка (заголовки)
                cell.set_text_props(fontweight='bold')

        buffer = io.BytesIO()
        plt.savefig(buffer, format='png', bbox_inches='tight')
        buffer.seek(0)
        plt.close()
        vis_7= base64.b64encode(buffer.getvalue()).decode('utf-8')
        return vis_7

    @task(task_id="vis8")
    def vis8(data_25):
        df_table=data_25[['s','e_q', 'e_c','p_q', 'p_c','f_c_v_plan','f_q_v_plan']].groupby(['s']).sum().reset_index()
        df_table = df_table.round(2)
        df_table = df_table.applymap(lambda x: f"{x:,.0f}".replace(",", " ") if isinstance(x, (int, float)) else x)
        df_table=df_table[["s","e_c","p_c","f_c_v_plan","e_q","p_q","f_q_v_plan"]]

        df_table = df_table.rename(columns={
            "s": "Укрупненная номенклатура",
            "e_c": "План ГПЗ \n стоимость",
            "p_c": "Факт ВЗ \n стоимость",
            "f_c_v_plan": "Факт ПТиУ (в плане) \n стоимость",
            "e_q": "План ГПЗ \n количество",
            "p_q": "Факт ВЗ \n количество",
            "f_q_v_plan": "Факт ПТиУ (в плане) \n количество",
        })
        fact_table= data_25[['previous_period_fact_exists','f_c']]
        fact_table = pd.DataFrame({
            'Факт ПТиУ (вне плана)': fact_table[fact_table['previous_period_fact_exists'] == 'Да']['f_c'].sum().round(1),
            'Факт ПТиУ (в плане)': fact_table[fact_table['previous_period_fact_exists'] == 'Нет']['f_c'].sum(),
            'Факт ПТиУ Итог': fact_table[(fact_table['previous_period_fact_exists'] == 'Да') | (fact_table['previous_period_fact_exists'] == 'Нет')]['f_c'].sum(),
        }, index=[0])

        fact_table = fact_table.applymap(lambda x: f"{x:,.0f}".replace(",", " ") if isinstance(x, (int, float)) else x)

        df_fact= data_25.copy()
        df_fact['f_c_vne_plan'] = df_fact.apply(
            lambda row: row['f_c'] if row['previous_period_fact_exists'] == 'Да' else 0, axis=1
        )
        df_fact['f_q_vne_plan'] = df_fact.apply(
            lambda row: row['f_q'] if row['previous_period_fact_exists'] == 'Да' else 0, axis=1
        )
        df_fact = df_fact[['s', 'f_c_vne_plan', 'f_q_vne_plan']].groupby(['s']).sum().reset_index()

        df_fact = df_fact.round(2)
        df_fact = df_fact[(df_fact['f_c_vne_plan'] != 0) | (df_fact['f_q_vne_plan'] != 0)]
        df_fact = df_fact.applymap(lambda x: f"{x:,.0f}".replace(",", " ") if isinstance(x, (int, float)) else x)
        df_fact = df_fact.rename(columns={
            "s": "Укрупненная номенклатура",
            "f_c_vne_plan": "Факт ПТиУ (вне плана) \n стоимость",
            "f_q_vne_plan": "Факт ПТиУ (вне плана) \n количество"
        })
        df_fact["Факт ПТиУ (вне плана) \n стоимость"] = df_fact["Факт ПТиУ (вне плана) \n стоимость"].astype(str) + " ₽"

        fg = plt.figure(figsize=(10, 10), constrained_layout=True)  # Попробуйте уменьшить размеры фигуры
        gs = fg.add_gridspec(1, 1)  # Одно поле для таблицы 2
        current_date = datetime.today().strftime("%d.%m.%Y")
        ax_table_2 = fg.add_subplot(gs[0])

        y_upper = 2.65
        row_height = 0.06  # Высота одной строки
        header_height = 0.1  # Высота заголовка
        num_rows = len(df_table)  # Количество строк в таблице
        height = header_height + row_height * num_rows
        y0 = y_upper - height

        table_2 = ax_table_2.table(
            cellText=df_fact.values,
            colLabels=df_fact.columns,
            cellLoc='center',
            loc='upper center',
            colColours=['lightblue'] * len(df_fact.columns),
            bbox=[-2, y0, 2, height],
            colWidths=[0.5] + [0.2] * (len(df_fact.columns) - 1)

        )

        table_2.auto_set_font_size(False)
        table_2.set_fontsize(15)
        table_2.scale(2, 2.5)
        ax_table_2.axis('off')

        ax_table_2.set_title("Cтоимость и количество по укр.номенклатуре за текущий год (ПТиУ вне плана)", y=2.7,x=-1, fontsize=18, fontweight='bold')


        for (i, key) in enumerate(table_2._cells):
            cell = table_2._cells[key]
            if key[0] == 0:
                cell.set_text_props(fontweight='bold')
                cell.set_height(0.1)



        buffer = io.BytesIO()
        plt.savefig(buffer, format='png', bbox_inches='tight')
        buffer.seek(0)
        plt.close()
        vis_8 = base64.b64encode(buffer.getvalue()).decode('utf-8')
        return vis_8

    @task(task_id="send_email")
    def send_email(vis_1, vis_2,vis_3,vis_4,vis_5,vis_6,vis_7,vis_8):
        def decode_base64_to_image(base64_string):
            image_data = base64.b64decode(base64_string)
            return Image.open(io.BytesIO(image_data))

        img1 = decode_base64_to_image(vis_1)
        img2 = decode_base64_to_image(vis_2)
        img3 = decode_base64_to_image(vis_3)
        img4 = decode_base64_to_image(vis_4)
        img5 = decode_base64_to_image(vis_5)
        img6 = decode_base64_to_image(vis_6)
        img7 = decode_base64_to_image(vis_7)
        img8 = decode_base64_to_image(vis_8)

        def add_white_background(image):
            if image.mode in ('RGBA', 'LA') or (image.mode == 'P' and 'transparency' in image.info):
                background = Image.new('RGB', image.size, (255, 255, 255))
                background.paste(image, mask=image.split()[-1])  # Используем альфа-канал как маску
                return background
            else:
                return image.convert('RGB')

        img1 = add_white_background(img1)
        img2 = add_white_background(img2)
        img3 = add_white_background(img3)
        img4 = add_white_background(img4)
        img5 = add_white_background(img5)
        img6 = add_white_background(img6)
        img7 = add_white_background(img7)
        img8 = add_white_background(img8)
        pdf_filename = "/opt/airflow/dags/QPZ_ETL_Image/ГПЗ.pdf"  # Укажите нужный путь
        img1.save(pdf_filename, format="PDF", resolution=300.0, save_all=True, append_images=[img2,img3,img4,img5,img6,img7,img8])

        html_content = f"""
        <html>
            <body>
                <img src="data:image/png;base64,{vis_1}" alt="График1">
                <img src="data:image/png;base64,{vis_2}" alt="График2">
            </body>
        </html>
        """

        email_task = EmailOperator(
            task_id='send_email_task',
            to=['e.mezhiritskaya@titan2.ru','t.speranskaya@titan2.ru','i.elnikova@titan2.ru','s.mamedov@titan2.ru'],
            subject='ГПЗ',
            html_content=html_content,
            files=[pdf_filename]  # Передаем только путь к файлу
        )
        email_task.execute(context={})

    vis_1 = vis1(d_25)
    vis_2 = vis2(d_25)
    vis_3 = vis3(d_25)
    vis_4 = vis4(d_25)
    vis_5 = vis5(d_25)
    vis_6 = vis6(d_25)
    vis_7 = vis7(d_25)
    vis_8 = vis8(d_25)

    send_email(vis_1, vis_2,vis_3,vis_4,vis_5,vis_6, vis_7,vis_8)


