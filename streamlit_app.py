import streamlit as st
import scripts.SnowflakeAPI as snwflk
import scripts.Admin as Admin

# -r https://raw.githubusercontent.com/snowflakedb/snowflake-connector-python/v2.7.9/tested_requirements/requirements_39.reqs

SNWFLK_DB = "flipside"
SNWFLK_SCHEMA = "dbt_wsayer2"
SNWFLK_API = snwflk.SnowflakeAPI(db=SNWFLK_DB, schema=SNWFLK_SCHEMA, profile="streamlit")


def build_entity_bar_chart():
    query = """
    SELECT ENTITY, SUM(usd_value) AS USD_VALUE
    FROM curr_entity_bals
    WHERE usd_value > 1000
        AND entity IN ('Coinbase', 'Kraken', 'Binance')
    GROUP BY 1
    """
    df = SNWFLK_API.run_get_query(query)

    return st.bar_chart(df, x="ENTITY", y="USD_VALUE")


def build_crypto_prices():
    query = """
        SELECT 
            ASSET AS "Coin", 
            USD AS "Price",
            USD_MARKET_CAP AS "Market Cap",
            USD_24H_VOL AS "24hr Vol"
        FROM flipside.coingecko.prices
        """
    df = SNWFLK_API.run_get_query(query).set_index(keys="Coin")
    df = df.applymap(Admin.format_num_to_sig_figs)

    return st.dataframe(data=df)


def build_trending():
    query = """
        SELECT 
            NAME AS "Name", 
            SCORE + 1 AS "Rank",
            RANK_TIMESTAMP
        FROM flipside.coingecko.trending
        WHERE RANK_TIMESTAMP = (SELECT MAX(RANK_TIMESTAMP) FROM flipside.coingecko.trending)
    """
    df = SNWFLK_API.run_get_query(query)
    return st.dataframe(data=df)


st.title("Chip's Crypto Data Dashboard")
build_entity_bar_chart()
build_crypto_prices()
build_trending()
