{{ config(materialized='table') }}

SELECT
    *
FROM {{ref('historical_trending_windows')}}
WHERE (date || '-' || to_varchar(hour, '00')) = (SELECT MAX((date || '-' || to_varchar(hour, '00'))) 
                                                 FROM {{ref('historical_trending_windows')}})
