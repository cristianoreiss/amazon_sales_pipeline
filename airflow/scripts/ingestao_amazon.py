import pandas as pd
from sqlalchemy import create_engine

def ingestao_amazon_csv():
    path = '/opt/airflow/data/ingestao_amazon.csv'
    df = pd.read_csv(path)
    engine = create_engine('postgresql+psycopg2://airflow:airflow@localhost:5432/amazon_data')
    df.to_sql('raw_amazon_sales', engine, if_exists='replace', index=False)
if __name__ == '__main__':
    ingestao_amazon_csv()
