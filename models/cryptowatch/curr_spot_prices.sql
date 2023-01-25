
{{ config(materialized='view') }}

SELECT _ETL_TIMESTAMP AS TIME, ASSET, BASE, PRICE
FROM bigdorksonly.cryptowatch.spot_prices
WHERE _ETL_TIMESTAMP = (SELECT MAX(_ETL_TIMESTAMP) FROM bigdorksonly.cryptowatch.spot_prices)

