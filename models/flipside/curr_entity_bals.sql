
{{ config(materialized='table') }}

WITH addr_labels AS (
        -- This refers to the table created from seeds/network_address_labels.csv
        SELECT *
        FROM {{ ref('network_address_labels') }}
),

entities AS (
            -- This refers to the table created from seeds/entities.csv
        SELECT *
        FROM {{ ref('entities') }}
),

entity_bals AS (
    SELECT
        addr_labels.entity,
        addr_labels.network,
        curr_bals.symbol,
        SUM(CURRENT_BAL) AS BALANCE,
        SUM(USD_VALUE_NOW) AS USD_VALUE,
        MAX(LAST_ACTIVITY_BLOCK) AS LAST_ACTIVITY_BLOCK,
        MAX(LAST_ACTIVITY_BLOCK_TIMESTAMP) AS LAST_ACTIVITY_BLOCK_TIMESTAMP,
        MIN(LAST_RECORDED_PRICE) AS LAST_RECORDED_PRICE_MIN,
        MAX(LAST_RECORDED_PRICE) AS LAST_RECORDED_PRICE_MAX,
        MAX(_ETL_TIMESTAMP) AS BALANCE_TIMESTAMP
    FROM bigdorksonly.flipside.curr_account_bals AS curr_bals
    LEFT JOIN addr_labels ON curr_bals.USER_ADDRESS = addr_labels.address
                            AND curr_bals.network = addr_labels.network
    WHERE curr_bals.symbol IS NOT NULL
    GROUP BY 1, 2, 3
)

SELECT 
    entities.entity_type,
    entity_bals.*
FROM entity_bals
LEFT JOIN entities ON entity_bals.entity = entities.entity

