from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import sys

sys.path.insert(0,os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

default_args = {
    'owner':'Abdelrahman',
    'start_date': datetime(2024,8,26),
}

file_postfix = datetime.now().strftime('%Y%m%d')

dag = DAG(
    dag_id='etl_reddit_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['reddit','etl','pipeline']
)

#TODO: extract from reddit
extract = PythonOperator(
    task_id='reddit_extraction',
    python_callable=reddit_pipeline,
    op_kwargs={
        'filename':f'reddit_{file_postfix}',
        'subbreddit':'dateengineering',
        'time_Filter':'day',
        'limit':100
    }
    
    )
#TODO: upload to S3