SELECT
    symbol,
    date,
    open,
    high,
    low,
    close,
    volume,
    asset_type
FROM {{ source('raw_data', 'market_prices') }}
WHERE close IS NOT NULL