from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os


from ingestao_amazon import ingestao_amazon_csv

with DAG(
    dag_id='amazon_sales_pipeline',
    start_date=datetime(2025, 2, 21),
    schedule_interval='@daily',
    catchup=False
) as dag:

    task_ingest = PythonOperator(
        task_id='ingest_csv_to_postgres',
        python_callable=ingestao_amazon_csv
    )