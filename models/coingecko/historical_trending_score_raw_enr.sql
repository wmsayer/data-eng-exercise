{{ config(materialized='view') }}

WITH hist_tsr AS (
    SELECT
        htr.*,
        ROW_NUMBER() OVER (PARTITION BY htr.cg_id, htr.date, htr.hour ORDER BY htr.time DESC) AS asset_hr_rn,
        ROW_NUMBER() OVER (PARTITION BY htr.date, htr.hour ORDER BY htr.time DESC) AS glb_hr_rn,
        runs.last_log_taken AS glb_last_log_taken,
        runs.log_lag_seconds AS glb_log_delay_sec,
        LAG(htr.time, 1) OVER (PARTITION BY htr.cg_id ORDER BY htr.time ASC) AS asset_last_log_taken
    FROM {{ref('historical_trending_score_raw')}} AS htr
    LEFT JOIN bigdorksonly.coingecko.trending_log_runs AS runs ON htr.time = runs.log_time
    WHERE htr.trending_score IS NOT NULL
    ORDER BY htr.cg_id, htr.time
)

SELECT
    hist_tsr.*,
    DATE_PART(EPOCH, time) - DATE_PART(EPOCH, asset_last_log_taken) AS asset_log_delay_sec
FROM hist_tsr