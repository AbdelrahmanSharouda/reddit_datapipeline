from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipelines import reddit_pipeline, aws_pipeline 
from utils.constants import OUTPUT_PATH



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

# Alternative approach using FileSensor
watch_for_deletion = FileSensor(
    task_id='watch_for_deletion',
    filepath=f"{OUTPUT_PATH}/reddit_{file_postfix}.csv",
    poke_interval=30,
    timeout=60 * 5,
    soft_fail=True,
    dag=dag
)

delete_csv = PythonOperator(
    task_id='delete_csv',
    dag=dag,
    python_callable=reddit_pipeline.remove_csv_file,
    trigger_rule='none_failed'
)

upload_s3 = PythonOperator(
    task_id='upload_to_s3',
    dag=dag,
    python_callable=aws_pipeline.upload_s3_pipeline,
    )

extract >> upload_s3 >> watch_for_deletion >> delete_csv