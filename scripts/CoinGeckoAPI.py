from scripts.Admin import run_rest_get
import scripts.SnowflakeAPI as snwflk
import pandas as pd

import time

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


if __name__ == "__main__":
    test_api = CoinGeckoAPI()
    test_api.log_cg_to_snwflk()
