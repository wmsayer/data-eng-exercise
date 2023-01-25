{{ config(materialized='view') }}

SELECT DISTINCT ID, NAME, SYMBOL
FROM bigdorksonly.coingecko.trending
