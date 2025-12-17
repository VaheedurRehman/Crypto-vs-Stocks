

WITH current_data AS (
    SELECT 
        symbol,
        close as current_price,
        date
    FROM USER_DB_QUAIL.ANALYTICS.stg_market_prices
    QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) = 1
),

forecast_data AS (
    SELECT 
        symbol,
        predicted_close as forecasted_price_14d,
        forecast_date
    FROM USER_DB_QUAIL.ANALYTICS.market_forecast_results
    QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY forecast_date DESC) = 1
)

SELECT 
    c.symbol,
    c.current_price,
    f.forecasted_price_14d,
    ((f.forecasted_price_14d - c.current_price) / c.current_price) * 100 as predicted_return_pct,
    f.forecast_date
FROM current_data c
JOIN forecast_data f ON c.symbol = f.symbol