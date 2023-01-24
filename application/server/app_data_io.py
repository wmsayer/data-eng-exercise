import pandas as pd
import scripts.SnowflakeAPI as snwflk


def get_btc_price_data(snwflk_api):
    query = """
    SELECT *
    FROM flipside.coingecko.historical_prices
    """
    result_df = snwflk_api.run_get_query(query)
    result_df['TIME'] = pd.to_datetime(result_df['TIME'])
    return result_df


def get_spot_prices(snwflk_api):
    table_str = "flipside.cryptowatch.spot_prices"
    query = f"""
        SELECT _ETL_TIMESTAMP AS TIME, ASSET, BASE, PRICE
        FROM {table_str}
        WHERE _ETL_TIMESTAMP = (SELECT MAX(_ETL_TIMESTAMP) FROM {table_str})
        """
    result_df = snwflk_api.run_get_query(query)
    result_df['TIME'] = pd.to_datetime(result_df['TIME'])
    return result_df


if __name__ == '__main__':
    test_api = snwflk.SnowflakeAPI(schema="dbt_wsayer2", wh="APPLICATION")
    test_df = get_spot_prices(test_api)
    print(test_df)

