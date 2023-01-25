import pandas as pd
import scripts.SnowflakeAPI as snwflk


# class AppDataIO():
#     def __init__(self, env):
#         self.env = env

def get_btc_price_data(snwflk_api):
    query = """
    SELECT *
    FROM BIGDORKSONLY.coingecko.historical_prices
    ORDER BY TIME
    """
    result_df = snwflk_api.run_get_query(query)
    result_df['TIME'] = pd.to_datetime(result_df['TIME'])
    return result_df


def get_spot_prices(snwflk_api):
    table_str = "BIGDORKSONLY.cryptowatch.spot_prices"
    query = f"""
        SELECT _ETL_TIMESTAMP AS TIME, ASSET, BASE, PRICE
        FROM {table_str}
        WHERE _ETL_TIMESTAMP = (SELECT MAX(_ETL_TIMESTAMP) FROM {table_str})
        """
    result_df = snwflk_api.run_get_query(query)
    result_df['TIME'] = pd.to_datetime(result_df['TIME'])
    return result_df


def get_trending_data(snwflk_api):
    query = """
        SELECT 
            NAME,
            SYMBOL,
            MAX(TIME) AS TIME, 
            AVG(MARKET_CAP_RANK) AS MARKET_CAP_RANK,
            AVG(TRENDING_SCORE) AS TRENDING_SCORE
        FROM BIGDORKSONLY.dbt_output_prod.historical_trending
        GROUP BY NAME, SYMBOL, DATE, HOUR
        ORDER BY NAME, DATE, HOUR
    """
    result_df = snwflk_api.run_get_query(query)
    result_df['TIME'] = pd.to_datetime(result_df['TIME'])
    result_df.columns = [c.replace("_", " ").title() for c in list(result_df.columns)]
    return result_df


if __name__ == '__main__':
    test_api = snwflk.SnowflakeAPI(schema="dbt_output_prod", wh="APPLICATION")
    test_df = get_spot_prices(test_api)
    print(test_df)

