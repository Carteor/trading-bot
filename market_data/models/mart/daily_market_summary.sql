{{
    config(
        materialized='incremental',
        unique_key=['symbol', 'date']
    )
}}

WITH prices AS (
    SELECT * FROM {{ ref('stg_prices') }}
),
indicators AS (
    SELECT * FROM {{ ref('stg_economic_indicators') }}
),

fed_funds AS (
    SELECT date, value AS federal_funds_rate
    FROM indicators
    WHERE series_id = 'FEDFUNDS'
),
cpi AS (
    SELECT date, value AS cpi_inflation
    FROM indicators
    WHERE series_id = 'CPIAUCSL'
),
oil AS (
    SELECT date, value AS crude_oil_price
    FROM indicators
    WHERE series_id = 'DCOILWTICO'
)
SELECT
    p.symbol,
    p.date,
    p.close,
    p.ma_7,
    p.ma_21,
    p.daily_return,
    p.volatility_21,
    f.federal_funds_rate,
    c.cpi_inflation,
    o.crude_oil_price
FROM prices p
LEFT JOIN fed_funds f ON DATE_TRUNC('month', p.date) = DATE_TRUNC('month', f.date)
LEFT JOIN cpi c ON DATE_TRUNC('month',  p.date) = DATE_TRUNC('month', c.date)
LEFT JOIN oil o ON p.date = o.date
{% if is_incremental() %}
WHERE p.date > (SELECT MAX(date) FROM {{ this }})
{% endif %}
