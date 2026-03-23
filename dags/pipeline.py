from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable

from datetime import datetime
from sqlalchemy import create_engine
import os

from src.extract import extract
from src.quality import run_quality_checks
from src.load import load_raw_prices


def get_engine():
    return create_engine(
        f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@db/{os.getenv('POSTGRES_DB')}"
    )


def task_extract_load():
    engine = get_engine()
    symbols_str = Variable.get("symbols", default_var="AAPL,MSFT")
    symbols = [s.strip() for s in symbols_str.split(",")]
    df = extract(symbols=symbols, start="2024-01-01")
    load_raw_prices(df, engine)


def task_quality_check():
    engine = get_engine()
    run_quality_checks(engine)


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

    quality_check = PythonOperator(
        task_id="quality_check",
        python_callable=task_quality_check,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "cd /opt/airflow/market_data && "
            "dbt run --profiles-dir /opt/airflow/market_data --log-path /tmp/dbt_logs --no-partial-parse && "
            "dbt test --profiles-dir /opt/airflow/market_data --log-path /tmp/dbt_logs --no-partial-parse"
        ),
        env={
            "PATH": "/home/airflow/.local/bin:/usr/local/bin:/usr/bin:/bin",
            "POSTGRES_HOST": "db",
            "POSTGRES_USER": os.getenv("POSTGRES_USER"),
            "POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD"),
            "POSTGRES_DB": os.getenv("POSTGRES_DB"),
        },
    )

    extract_load >> dbt_run >> quality_check
