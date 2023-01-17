
{{ config(materialized='table') }}

WITH addr_labels AS (
        -- This refers to the table created from seeds/network_address_labels.csv
        SELECT *
        FROM {{ ref('network_address_labels') }}
)

select *
from source_data

