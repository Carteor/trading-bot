# Market Data Pipeline

An ELT pipeline that ingests daily market data, computes technical indicators, and produces an analysis-ready mart table suitable for ML or BI consumption.

## Architecture

yfinance API -> raw.prices -> dbt (stg_prices view) -> mart.daily_market_summary -> quality checks

## Stack
 - Apache Airflow - orchestration
 - PostgreSQL - data warehouse
 - dbt - transformation layer
 - Python - extract and load scripts
 - Docker - containerized deployment

## Layers
 - `raw.prices` - raw data from yfinance
 - `mart.daily_market_summary` - enriched with MA7, MA21, RSI14, daily returns and vvolatility

## How to run
1. `git clone` the project
2. Copy the `.env.example` to `.env` and fill in the variables
3. Run `docker compose up -d --build`
4. Airflow UI: `http://localhost:8080` (admin/admin)
5. PgAdmin `http://localhost:5050`
6. Trigger the `market_data_pipeline` DAG manually

## DAG
extract_load_raw -> transform_load_mart -> quality_check
