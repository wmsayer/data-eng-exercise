-- All scores should have been normalized to 100.
-- Therefore return records where this isn't true to make the test fail
SELECT
    cg_id, date, hour
FROM {{ ref('historical_trending_hourly' )}}
WHERE trending_score > 100