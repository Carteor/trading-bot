{{
    config(
        materialized='incremental',
        unique_key=['symbol', 'date']
    )
}}

SELECT
    symbol,
    date,
    close,
    ma_7,
    ma_21,
    daily_return,
    volatility_21
FROM {{ ref('stg_prices') }}
{% if is_incremental() %}
WHERE date > (SELECT MAX(date) FROM {{ this }})
{% endif %}
