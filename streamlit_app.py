import streamlit as st
import scripts.SnowflakeAPI as snwflk

# -r https://raw.githubusercontent.com/snowflakedb/snowflake-connector-python/v2.7.9/tested_requirements/requirements_39.reqs

SNWFLK_DB = "flipside"
SNWFLK_SCHEMA = "dbt_wsayer2"
SNWFLK_API = snwflk.SnowflakeAPI(db=SNWFLK_DB, schema=SNWFLK_SCHEMA, profile="streamlit")

test_query = """
SELECT entity, SUM(usd_value) AS usd_value
FROM curr_entity_bals
WHERE usd_value > 1000
    AND entity IN ('Coinbase', 'Kraken', 'Binance')
GROUP BY 1
"""
test_df = SNWFLK_API.run_get_query(test_query)

st.bar_chart(test_df, x="entity", y="usd_value")
