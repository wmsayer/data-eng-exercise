from application.server.scripts.Admin import run_rest_get
import application.server.scripts.SnowflakeAPI as snwflk
import pandas as pd
import time
import numpy as np
from sys import platform
from dotenv import load_dotenv
import os
import pathlib
import math

PROJECT_ROOT = "%s" % pathlib.Path(__file__).parent.parent.parent.parent.absolute()

if platform == "linux":
    load_dotenv('/home/ubuntu/.bashrc')

API_KEY = os.environ.get('CRYPTOWATCH_API')


class CryptowatchAPI:
    def __init__(self, assets):
        self.api_root = 'https://api.cryptowat.ch'
        self.snwflk_db = "FLIPSIDE"
        self.assets = assets
        self.rate_limit_24hr = 10
        self.rate_limit_tol = 0.95
        self.log_book = {
            "spot_prices": {"fn": self.get_current_prices, "api_credits": 0.005, "min_freq": 5},
            # "next_prices": {"fn": self.get_current_prices, "api_credits": 0.005, "min_freq": 600}
        }

        self.calc_log_freq()

    def calc_log_freq(self, debug=False):

        target_tol = self.rate_limit_24hr * self.rate_limit_tol

        for k, v in self.log_book.items():
            total_calls = math.floor(target_tol / v["api_credits"])
            tgt_freq = 24*3600/total_calls  # call API every X sec
            self.log_book[k]["freq"] = max(tgt_freq, v["min_freq"])

        check_sum = 0
        for k, v in self.log_book.items():
            check_sum += v["api_credits"]*(24*3600/v["freq"])

        if debug:
            print(f"API Credits Target: {target_tol} --- APCredits Expected: {check_sum}")
            print(pd.DataFrame(self.log_book).transpose().drop(columns="fn"))

        assert(check_sum <= target_tol)

    def get_current_prices(self, base_asst="usdt", exchange="binance", write_snwflk=True, pull_new=True):
        header = {"X-CW-API-Key": API_KEY}
        url = "/".join([self.api_root, f"markets/prices"])

        cache_path = "C:/Users/wsaye/PycharmProjects/data-eng-exercise/data/cw_market_prices.csv"

        if pull_new:
            result_dict, status_code = run_rest_get(url, headers=header, print_summ=False)
            result_df = pd.DataFrame(result_dict).reset_index(drop=False)
            result_df.to_csv(cache_path, index=False)
        else:
            result_df = pd.read_csv(cache_path)

        # expand metadata into columns
        meta_data = result_df["index"].str.split(":", expand=True, n=3)
        result_df["exchange"] = meta_data[1]
        result_df["pair"] = meta_data[2]

        # filter by exchange and asset pairs
        asset_pairs = [a + base_asst for a in self.assets]
        result_df = result_df.loc[result_df["exchange"] == exchange, :]
        result_df = result_df.loc[result_df["pair"].isin(asset_pairs), :]

        # split pair into columns
        result_df["asset"] = result_df["pair"].str.split(base_asst, expand=True)[0]
        result_df["base"] = base_asst

        # clean up columns
        result_df = result_df.rename(columns={"result": "price"})
        result_df = result_df[["exchange", "asset", "base", "price"]]

        if write_snwflk:
            snwflk_api = snwflk.SnowflakeAPI(schema='CRYPTOWATCH', db=self.snwflk_db)
            snwflk_api.write_df(result_df, table='SPOT_PRICES', replace=False)

        return result_df


if __name__ == "__main__":
    test_assets = ["btc", "eth", "ada", "matic", "sol"]
    test_api = CryptowatchAPI(assets=test_assets)
    test_df = test_api.get_current_prices(write_snwflk=True, pull_new=True)
    print(test_df.columns)
    print(test_df)
