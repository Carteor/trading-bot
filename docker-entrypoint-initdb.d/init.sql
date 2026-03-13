CREATE DATABASE airflow;
CREATE SCHEMA raw;

CREATE TABLE raw.prices (
    id serial primary key,
    symbol text, 
    date timestamptz,
    open numeric(12,6), 
    high numeric(12,6), 
    low numeric(12,6), 
    close numeric(12,6), 
    volume bigint
);

CREATE SCHEMA staging;

CREATE TABLE staging.prices_enriched (
    id SERIAL PRIMARY KEY,
    symbol TEXT,
    date TIMESTAMPTZ,
    open NUMERIC(12,6),
    high NUMERIC(12,6),
    low NUMERIC(12,6),
    close NUMERIC(12,6),
    volume BIGINT,
    ma_7 NUMERIC(12,6),
    ma_21 NUMERIC(12,6),
    rsi_14 NUMERIC(8,4),
    daily_return NUMERIC(8,6),
    volatility_21 NUMERIC(8,6)
);

CREATE SCHEMA mart;

create table mart.daily_market_summary (
    id SERIAL PRIMARY KEY,
    symbol TEXT,
    date TIMESTAMPTZ,
    close NUMERIC(12,6),
    ma_7 NUMERIC(12,6),
    ma_21 NUMERIC(12,6),
    rsi_14 NUMERIC(8,4),
    daily_return NUMERIC(8,6),
    volatility_21 NUMERIC(8,6),
    trend VARCHAR(10)
);
