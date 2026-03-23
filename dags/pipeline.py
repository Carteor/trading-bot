from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable

from datetime import datetime
from sqlalchemy import create_engine
import os

from src.extract import extract
from src.quality import run_quality_checks
from src.extract_fred import extract_fred
from src.load import load_raw_prices, load_raw_indicators


def get_engine():
    return create_engine(
        f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@db/{os.getenv('POSTGRES_DB')}"
    )


def task_extract_load():
    engine = get_engine()
    symbols_str = Variable.get("symbols", default_var="AAPL,MSFT")
    start_date = Variable.get("start_date", default_var="2024-01-01")

    symbols = [s.strip() for s in symbols_str.split(",")]
    df = extract(symbols=symbols, start=start_date)
    load_raw_prices(df, engine)

def task_extract_load_fred():
    engine = get_engine()
    start_date = Variable.get("start_date", default_var="2024-01-01")
    df_fred = extract_fred(["FEDFUNDS", "CPIAUCSL", "DCOILWTICO"], start=start_date)
    load_raw_indicators(df_fred, engine)

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

    extract_fred_task = PythonOperator(
        task_id="extract_load_fred",
        python_callable=task_extract_load_fred,
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

    [ extract_load, extract_fred_task ]>> dbt_run >> quality_check
