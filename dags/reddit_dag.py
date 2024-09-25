from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import sys
from utils.constants import OUTPUT_PATH

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipelines import reddit_pipeline, aws_pipeline 

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
    dag=dag,
    python_callable=reddit_pipeline.reddit_pipeline,
    op_kwargs={
        'file_name': f'reddit_{file_postfix}',
        'subreddit': 'dataengineering', 
        'time_filter': 'day',  
        'limit': 10
    })
#TODO: upload to S3

upload_s3 = PythonOperator(
    task_id='upload_to_s3',
    dag=dag,
    python_callable=aws_pipeline.upload_s3_pipeline,
    op_kwargs={
        'file_path': f'{OUTPUT_PATH}/{file_name}.csv',
        's3_bucket': 'reddit-s3-bucket',
        's3_key': f'reddit_posts/{file_name}.csv'
    })

