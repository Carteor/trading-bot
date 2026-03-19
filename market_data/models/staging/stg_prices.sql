WITH base AS (
    -- Select all columns from raw.prices
    SELECT
        symbol,
        date,
        open,
        high,
        low,
        close,
        volume,
        (close - LAG(close, 1) OVER (
            PARTITION BY symbol ORDER BY date
        )) / LAG(close, 1) OVER (
            PARTITION BY symbol ORDER BY date
        ) AS daily_return,
        AVG(close) OVER (
            PARTITION BY symbol ORDER BY date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS ma_7,
        AVG(close) OVER (
            PARTITION BY symbol ORDER BY date
            ROWS BETWEEN 20 PRECEDING AND CURRENT ROW
        ) AS ma_21
    FROM raw.prices
)
SELECT
    *,
    STDDEV(daily_return) OVER (
        PARTITION BY symbol ORDER BY date
        ROWS BETWEEN 20 PRECEDING AND CURRENT ROW
    ) AS volatility_21
FROM base
