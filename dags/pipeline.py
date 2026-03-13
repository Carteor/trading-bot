from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from sqlalchemy import create_engine
import os

from src.extract import extract
from src.load import load_raw_prices, load_mart
from src.transform import transform


def get_engine():
    return create_engine(
        f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@db/{os.getenv('POSTGRES_DB')}"
    )


def task_extract_load():
    engine = get_engine()
    df = extract(symbols=["AAPL", "MSFT"], start="2024-01-01")
    load_raw_prices(df, engine)


def task_transform_load():
    engine = get_engine()
    df_enriched = transform(engine)
    load_mart(df_enriched, engine)


with DAG(
    dag_id="market_data_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False
) as dag:
    extract_load = PythonOperator(
        task_id="extract_load_raw",
        python_callable=task_extract_load,
    )

    transform_load = PythonOperator(
        task_id="transform_load_mart",
        python_callable=task_transform_load,
    )

    extract_load >> transform_load
