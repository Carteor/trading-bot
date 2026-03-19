SELECT
    symbol,
    date,
    close,
    ma_7,
    ma_21,
    daily_return,
    volatility_21
FROM {{ ref('stg_prices') }}
