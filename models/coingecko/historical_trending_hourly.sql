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

log_counts_hourly AS (
    SELECT
        date,
        hour,
        COUNT(DISTINCT time) AS log_count_hourly
    FROM {{ref('historical_trending_score_raw')}}
    GROUP BY 1,2
),

scores AS (
    SELECT
        cg_id,
        date,
        hour,
        AVG(market_cap_rank) AS market_cap_rank,
        SUM(trending_score) AS trending_score
    FROM {{ref('historical_trending_score_raw')}}
    GROUP BY 1,2,3
),

final_scores AS (
    SELECT scores.*, log_counts_hourly.log_count_hourly
    FROM scores
    LEFT JOIN log_counts_hourly ON scores.date = log_counts_hourly.date AND scores.hour = log_counts_hourly.hour
)

SELECT
    prices.*,
    asset_meta.symbol,
    asset_meta.name,
    final_scores.log_count_hourly,
    final_scores.market_cap_rank,
    final_scores.trending_score / final_scores.log_count_hourly AS trending_score
FROM prices
LEFT JOIN final_scores 
    ON prices.cg_id = final_scores.cg_id
    AND prices.date = final_scores.date
    AND prices.hour = final_scores.hour
LEFT JOIN bigdorksonly.coingecko.trending_assets AS asset_meta
    ON prices.cg_id = asset_meta.cg_id
ORDER BY prices.cg_id, prices.date, prices.hour


