{{ config(materialized='table') }}

WITH prices AS (
    SELECT
        cg_id,
        time,
        date,
        hour,
        market_caps AS market_cap,
        prices AS price,
        market_caps/prices AS circulating_supply,
        total_volumes AS volume
    FROM bigdorksonly.coingecko.historical_prices_hourly
),

scores AS (
    SELECT
        cg_id,
        date,
        hour,
        AVG(market_cap_rank) AS market_cap_rank,
        AVG(trending_score) AS trending_score
    FROM {{ref('historical_trending_score_raw')}}
    GROUP BY 1,2,3
)

SELECT
    prices.*,
    asset_meta.symbol,
    asset_meta.name,
    scores.market_cap_rank,
    scores.trending_score
FROM prices
LEFT JOIN scores 
    ON prices.cg_id = scores.cg_id
    AND prices.date = scores.date
    AND prices.hour = scores.hour
LEFT JOIN bigdorksonly.coingecko.trending_assets AS asset_meta
    ON prices.cg_id = asset_meta.cg_id
ORDER BY prices.cg_id, prices.date, prices.hour


