import pandas as pd
from sqlalchemy import create_engine


def ingestao_amazon_csv():
    path = '/opt/airflow/data/amazon_sales_dataset.csv'
    df = pd.read_csv(path)

    engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres:5432/amazon_data')

    df.to_sql('raw_amazon_sales', engine, if_exists='replace', index=False)