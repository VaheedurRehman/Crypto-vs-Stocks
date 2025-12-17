
  create or replace   view USER_DB_QUAIL.ANALYTICS.stg_market_prices
  
  
  
  
  as (
    SELECT
    symbol,
    date,
    open,
    high,
    low,
    close,
    volume,
    asset_type
FROM USER_DB_QUAIL.RAW.market_prices
WHERE close IS NOT NULL
  );

