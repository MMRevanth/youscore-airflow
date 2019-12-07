from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
import requests
import re
from bs4 import BeautifulSoup

default_args = {
    'owner': 'Revanth',
    'depends_on_past': False,
    'start_date': datetime(2019,12,5),
    'email': ['mmrevanth00@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
dag = DAG('You-views', default_args=default_args, schedule_interval=timedelta(days=1))

def scrap():
    base_url="https://www.youtube.com/watch?v=GlJBrOzIzAM"
    page=requests.get(base_url)

    bsobj=BeautifulSoup(page.text,'html.parser')



    you_list=bsobj.find(class_='style-scope ytd-video-primary-info-renderer')
    you_list1=you_list.find(class_='view-count style-scope yt-view-count-renderer')
    you_list2=you_list1.find('span')

    for views in you_list1:
        result=views.contents[0]
        print(result)




t1 = SqliteOperator('you-viwe.sql',sqlite_conn_id='sqlite_default')


t2 = PythonOperator(
    task_id='Scrap',
    python_callable=scrap,
    dag=dag)

t3 = BashOperator(
    task_id='sleep',
    bash_command='sleep 5',
    dag=dag)

t2.set_downstream(t1)
t3.set_downstream(t2)
