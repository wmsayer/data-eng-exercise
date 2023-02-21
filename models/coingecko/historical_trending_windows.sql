{{ config(materialized='table') }}

WITH max_trend_dt AS (
    SELECT MAX(date || '-' || to_varchar(hour, '00'))  AS dt_hr_str
    FROM {{ref('historical_trending_hourly')}}
    WHERE trending_score IS NOT NULL
),
asset_trnds_hr AS (
    SELECT *
    FROM {{ref('historical_trending_score_raw_enr')}}
    WHERE asset_hr_rn = 1
),
master_window AS (
SELECT
    hth.cg_id, hth.time, hth.date, hth.hour,
    hth.name, hth.symbol, hth.market_cap,
    LAST_VALUE(hth.market_cap_rank IGNORE NULLS) OVER (PARTITION BY hth.cg_id ORDER BY hth.time ASC ROWS UNBOUNDED PRECEDING) as market_cap_rank,
    -- hth.market_cap_rank,
    hth.volume AS volume_1hr,
    SUM(hth.volume) OVER (PARTITION BY hth.cg_id ORDER BY hth.time ASC ROWS BETWEEN 5 PRECEDING AND 0 FOLLOWING) as volume_6hr,
    SUM(hth.volume) OVER (PARTITION BY hth.cg_id ORDER BY hth.time ASC ROWS BETWEEN 23 PRECEDING AND 0 FOLLOWING) as volume_24hr,
    SUM(hth.volume) OVER (PARTITION BY hth.cg_id ORDER BY hth.time ASC ROWS BETWEEN 71 PRECEDING AND 0 FOLLOWING) as volume_3d,
    SUM(hth.volume) OVER (PARTITION BY hth.cg_id ORDER BY hth.time ASC ROWS BETWEEN 167 PRECEDING AND 0 FOLLOWING) as volume_7d,
    SUM(hth.volume) OVER (PARTITION BY hth.cg_id ORDER BY hth.time ASC ROWS BETWEEN 335 PRECEDING AND 0 FOLLOWING) as volume_14d,
    SUM(hth.volume) OVER (PARTITION BY hth.cg_id ORDER BY hth.time ASC ROWS BETWEEN 671 PRECEDING AND 0 FOLLOWING) as volume_28d,
    hth.price,
    100/hth.price*(hth.price - LAG(hth.price, 1) OVER (PARTITION BY hth.cg_id ORDER BY hth.time ASC)) AS price_pct_chg_1hr,
    100/hth.price*(hth.price - LAG(hth.price, 6) OVER (PARTITION BY hth.cg_id ORDER BY hth.time ASC)) AS price_pct_chg_6hr,
    100/hth.price*(hth.price - LAG(hth.price, 24) OVER (PARTITION BY hth.cg_id ORDER BY hth.time ASC)) AS price_pct_chg_24hr,
    100/hth.price*(hth.price - LAG(hth.price, 72) OVER (PARTITION BY hth.cg_id ORDER BY hth.time ASC)) AS price_pct_chg_3d,
    100/hth.price*(hth.price - LAG(hth.price, 168) OVER (PARTITION BY hth.cg_id ORDER BY hth.time ASC)) AS price_pct_chg_7d,
    100/hth.price*(hth.price - LAG(hth.price, 336) OVER (PARTITION BY hth.cg_id ORDER BY hth.time ASC)) AS price_pct_chg_14d,
    100/hth.price*(hth.price - LAG(hth.price, 672) OVER (PARTITION BY hth.cg_id ORDER BY hth.time ASC)) AS price_pct_chg_28d,
    hth.trending_score AS trending_score_1hr,
    SUM(hth.trending_score) OVER (PARTITION BY hth.cg_id ORDER BY hth.time ASC ROWS BETWEEN 5 PRECEDING AND 0 FOLLOWING) as trending_score_6hr,
    SUM(hth.trending_score) OVER (PARTITION BY hth.cg_id ORDER BY hth.time ASC ROWS BETWEEN 23 PRECEDING AND 0 FOLLOWING) as trending_score_24hr,
    SUM(hth.trending_score) OVER (PARTITION BY hth.cg_id ORDER BY hth.time ASC ROWS BETWEEN 71 PRECEDING AND 0 FOLLOWING) as trending_score_3d,
    SUM(hth.trending_score) OVER (PARTITION BY hth.cg_id ORDER BY hth.time ASC ROWS BETWEEN 167 PRECEDING AND 0 FOLLOWING) as trending_score_7d,
    SUM(hth.trending_score) OVER (PARTITION BY hth.cg_id ORDER BY hth.time ASC ROWS BETWEEN 335 PRECEDING AND 0 FOLLOWING) as trending_score_14d,
    SUM(hth.trending_score) OVER (PARTITION BY hth.cg_id ORDER BY hth.time ASC ROWS BETWEEN 671 PRECEDING AND 0 FOLLOWING) as trending_score_28d,
    LAST_VALUE(athr.glb_last_log_taken IGNORE NULLS) OVER (ORDER BY hth.time ASC ROWS UNBOUNDED PRECEDING) as glb_last_log_taken,
    LAST_VALUE(athr.glb_log_delay_sec IGNORE NULLS) OVER (ORDER BY hth.time ASC ROWS UNBOUNDED PRECEDING) as glb_log_delay_sec,
    LAST_VALUE(athr.asset_last_log_taken IGNORE NULLS) OVER (PARTITION BY hth.cg_id ORDER BY hth.time ASC ROWS UNBOUNDED PRECEDING) as asset_last_log_taken,
    LAST_VALUE(athr.asset_log_delay_sec IGNORE NULLS) OVER (PARTITION BY hth.cg_id ORDER BY hth.time ASC ROWS UNBOUNDED PRECEDING) as asset_log_delay_sec
FROM bigdorksonly.dbt_output_dev.historical_trending_hourly AS hth
LEFT JOIN asset_trnds_hr AS athr ON hth.cg_id = athr.cg_id AND hth.date = athr.date AND hth.hour = athr.hour
WHERE (hth.date || '-' || to_varchar(hth.hour, '00')) <= (SELECT dt_hr_str FROM max_trend_dt)
ORDER BY 1, 2 ASC
)
SELECT
    master_window.*,
    100/volume_24hr*(volume_24hr - LAG(volume_24hr, 1) OVER (PARTITION BY cg_id ORDER BY time ASC)) AS volume_pct_chg_1hr,
    100/volume_24hr*(volume_24hr - LAG(volume_24hr, 6) OVER (PARTITION BY cg_id ORDER BY time ASC)) AS volume_pct_chg_6hr,
    100/volume_24hr*(volume_24hr - LAG(volume_24hr, 24) OVER (PARTITION BY cg_id ORDER BY time ASC)) AS volume_pct_chg_24hr,
    100/volume_24hr*(volume_24hr - LAG(volume_24hr, 72) OVER (PARTITION BY cg_id ORDER BY time ASC)) AS volume_pct_chg_3d,
    100/volume_24hr*(volume_24hr - LAG(volume_24hr, 168) OVER (PARTITION BY cg_id ORDER BY time ASC)) AS volume_pct_chg_7d,
    100/volume_24hr*(volume_24hr - LAG(volume_24hr, 336) OVER (PARTITION BY cg_id ORDER BY time ASC)) AS volume_pct_chg_14d,
    100/volume_24hr*(volume_24hr - LAG(volume_24hr, 672) OVER (PARTITION BY cg_id ORDER BY time ASC)) AS volume_pct_chg_28d
FROM master_window
