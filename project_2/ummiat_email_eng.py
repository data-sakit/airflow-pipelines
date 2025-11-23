from datetime import datetime
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.email_operator import EmailOperator

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 5
}

with DAG('email_ummiat', default_args=default_args, schedule='30 6 * * *') as dag:

    email = EmailOperator(
        task_id='send_email',
        to=['g.yalhovskih@titan2.ru','e.kokov@titan2.ru','k.kostenko@titan2.ru','i.loshkarev@titan2.ru','a.antonova@titan2.ru','e.sviridova@titan2.ru','s.belova@titan2.ru','v.tarasenko@titan2.ru','e.belanova@titan2.ru','i.kozyreva@titan2.ru','a.mishanin@titan2.ru','d.fathulov@titan2.ru','a.gulidova@titan2.ru','s.mamedov@titan2.ru'],
        subject='Ежедневный отчет',
        html_content="""
        <html>
        <body>
            <h1>Отчет УМиАТ</h1>
            <p>Нажмите <a href="https://pixbi.titan2.ru/shared/view/ae196488-c918-42df-bf57-606af6f71089?ticket=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJodHRwOi8vc2NoZW1hcy54bWxzb2FwLm9yZy93cy8yMDA1LzA1L2lkZW50aXR5L2NsYWltcy9uYW1lIjoiVGlja2V0VXNlciIsIm5iZiI6MTcyMzgyMTE1NiwiZXhwIjoyMDM5MzUzOTU2fQ.exrU1cOqEKZpFYJuUB7SwanVrUZwJ85VbBF9Ypo0jwk">здесь</a>, чтобы посмотреть отчет.</p>
        </body>
        </html>
        """,
        dag=dag
    )
