WITH market_data AS (
    SELECT * FROM USER_DB_QUAIL.ANALYTICS.stg_market_prices
),

calculations AS (
    SELECT
        symbol,
        date,
        close,
        asset_type,
        
        -- Calculate 14-Day Return
        (close - LAG(close, 14) OVER (PARTITION BY symbol ORDER BY date)) 
        / NULLIF(LAG(close, 14) OVER (PARTITION BY symbol ORDER BY date), 0) as return_14d,

        -- Calculate 30-Day Volatility (Risk)
        STDDEV(close) OVER (
            PARTITION BY symbol 
            ORDER BY date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as volatility_30d
        
    FROM market_data
)

SELECT * FROM calculations
WHERE date >= DATEADD(year, -1, CURRENT_DATE())
ORDER BY date DESC, symbol