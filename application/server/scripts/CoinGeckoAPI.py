from application.server.scripts.Admin import run_rest_get
import application.server.scripts.SnowflakeAPI as snwflk
import pandas as pd
import time
import numpy as np


# CG Documentation
# https://www.coingecko.com/en/api/documentation


class CoinGeckoAPI:
    def __init__(self, assets=[]):
        self.api_root = 'https://api.coingecko.com/api/v3'
        self.data_key = "coins"
        self.print_summ = False
        self.snwflk_db = "FLIPSIDE"

        self.assets = assets

        self.log_book = {
            "trending": {"fn": self.get_trending, "freq": 60*5},
            "prices": {"fn": self.get_simple_prices, "freq": 30}
        }

    def get_trending(self, write_snwflk=True):
        url = "/".join([self.api_root, "search/trending"])
        result_dict, status_code = run_rest_get(url, params={}, print_summ=self.print_summ)
        result_df = pd.json_normalize(result_dict[self.data_key], record_prefix="")
        result_df.columns = [c.split(".")[1] for c in result_df.columns]
        keep_cols = ["id", "name", "symbol", "market_cap_rank", "score"]
        result_df = result_df[keep_cols]

        if write_snwflk:
            result_df.columns = [c.upper() for c in result_df.columns]
            snwflk_schema = 'COINGECKO'
            snwflk_table = 'TRENDING'
            snwflk_api = snwflk.SnowflakeAPI(db=self.snwflk_db, schema=snwflk_schema)
            snwflk_api.write_df(result_df, snwflk_table, replace=False)

        return result_df

    def get_simple_prices(self, write_snwflk=True):

        url = "/".join([self.api_root, "simple/price"])
        params = {
            "ids": ",".join(self.assets),
            "vs_currencies": "usd",
            "include_market_cap": "true",
            "include_24hr_vol": "true"
        }

        if self.assets:
            result_dict, status_code = run_rest_get(url, params=params, print_summ=self.print_summ)
            result_df = pd.DataFrame(result_dict).transpose()
            result_df.index.rename("asset", inplace=True)
            result_df = result_df.reset_index(drop=False)

            if write_snwflk:
                result_df.columns = [c.upper() for c in result_df.columns]
                snwflk_schema = 'COINGECKO'
                snwflk_table = 'PRICES'
                snwflk_api = snwflk.SnowflakeAPI(db=self.snwflk_db, schema=snwflk_schema)
                snwflk_api.write_df(result_df, snwflk_table, replace=True)
        else:
            result_df = pd.DataFrame()

        return result_df

    def get_asset_mkt_chart(self, asset_ids, days, base="usd", print_summ=False, write_snwflk=True):
        df_list = []
        # asset_ids = [a for a in l1_asset_tups if a[1] in asset_tkrs]

        # If you're pulling data from  this API and the script fails due to a 429 Error it is you are making too many
        # requests to that API. Therefore, I implement a time.sleep(X) timer to pause for X seconds every Y calls.
        # If you get a 429 Error, adjust these parameters as needed.
        count = 1
        pause_X_sec = 10
        every_Y_calls = 10

        for a in asset_ids:

            if (count % every_Y_calls) == 0:
                print("From CoinGeckoAPI.py >>> get_asset_mkt_chart(): Pausing %d sec for API..." % pause_X_sec)
                time.sleep(pause_X_sec)

            url = "/".join([self.api_root, "coins", a[0], "market_chart"])
            params = {'vs_currency': base, 'days': days, "interval": "daily"}

            result_dict, status_code = run_rest_get(url, params=params, print_summ=print_summ)
            result_df = pd.DataFrame()

            for k, v in result_dict.items():
                temp_df = pd.DataFrame(v, columns=["time", k])

                if "time" in result_df.columns:
                    result_df[k] = temp_df[k]
                else:
                    result_df = temp_df

            result_df["asset"] = a[1]
            result_df.sort_values("time", inplace=True)
            df_list.append(result_df)

            count += 1

        df = pd.concat(df_list)
        df['time'] = pd.to_datetime(df['time'], unit='ms')
        df['time'] = df['time'] - np.timedelta64(1, 's')  # this is to make sure prices represent close not open
        df['time'] = df['time'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        # df['date'] = df['time'].dt.strftime('%Y-%m-%d')
        # df.drop_duplicates(subset=["date", "asset"], inplace=True)
        #
        # df.drop(columns=["date"], inplace=True)

        if write_snwflk:
            df.columns = [c.upper() for c in df.columns]
            snwflk_schema = 'COINGECKO'
            snwflk_table = 'HISTORICAL_PRICES'
            snwflk_api = snwflk.SnowflakeAPI(db=self.snwflk_db, schema=snwflk_schema)
            snwflk_api.write_df(df, snwflk_table, replace=True)

        return df


if __name__ == "__main__":
    test_api = CoinGeckoAPI()
    # test_api.log_cg_to_snwflk()
    test_assets = [("bitcoin", "BTC")]
    test_df = test_api.get_asset_mkt_chart(test_assets, 30, base="usd", print_summ=True)
    print(test_df.columns)
    print(test_df)